import os from 'node:os';
import { getRepo } from './repo.js';
import { getTransport } from './transport/index.js';
import { getHandler } from './handlers/index.js';
import { decideAfterFailure, withTimeout } from './core/index.js';
import { getConfig } from './config.js';

/**
 * Process a single transport message with full idempotency, crash simulation handling,
 * and retry/dead-lettering logic.
 *
 * Designed with dependency injection for pure unit testing without external services.
 *
 * @param {object} message
 * @param {string} message.messageId
 * @param {string} message.eventId
 * @param {object} deps
 * @param {import('./repo.js').EventRepository} deps.repo
 * @param {import('./transport/base.js').EventTransport} deps.transport
 * @param {Record<string, Function>} [deps.handlers]
 * @param {object} [deps.clock=Date]
 * @param {number} [deps.handlerTimeoutMs=8000]
 * @param {number} [deps.visibilityMs=30000]
 * @param {object} [deps.backoffOpts]
 * @param {Function} [deps.log]
 * @returns {Promise<object>} Outcome object
 */
export async function processMessage(
  { messageId, eventId },
  {
    repo,
    transport,
    handlers = null,
    clock = Date,
    handlerTimeoutMs = 8000,
    visibilityMs = 30000,
    backoffOpts = { baseMs: 2000, maxMs: 60000 },
    log = () => {},
  }
) {
  // 1. Fetch event from Postgres (source of truth)
  const event = await repo.getEventById(eventId);
  if (!event) {
    // If Postgres has no record of this event, ack to clear stream
    await transport.ack(messageId);
    return { outcome: 'missing_event', eventId };
  }

  // 2. Atomic claim against Postgres (verifies state is claimable)
  const claimed = await repo.claimEvent(eventId, visibilityMs);
  if (!claimed) {
    // Duplicate delivery or already claimed by another active worker; ACK and skip
    await transport.ack(messageId);
    return { outcome: 'duplicate_skip', eventId };
  }

  const attemptNumber = claimed.attempts;
  const startedAt = new Date(clock.now ? clock.now() : Date.now());

  // 3. Resolve handler
  let handlerFn;
  if (handlers && handlers[claimed.type]) {
    handlerFn = handlers[claimed.type];
  } else {
    handlerFn = getHandler(claimed.type);
  }

  // 4. Execute handler with timeout protection
  try {
    const handlerPromise = handlerFn({
      event: claimed,
      attempt: attemptNumber,
      log,
    });

    const result = await withTimeout(handlerPromise, handlerTimeoutMs);

    const finishedAt = new Date(clock.now ? clock.now() : Date.now());
    const durationMs = Math.max(0, finishedAt.getTime() - startedAt.getTime());

    // Record success attempt
    await repo.recordAttempt({
      eventId,
      attempt: attemptNumber,
      startedAt,
      finishedAt,
      outcome: 'success',
      durationMs,
    });

    // Mark event succeeded
    await repo.markSucceeded(eventId, result, durationMs);

    // Acknowledge stream message
    await transport.ack(messageId);

    return { outcome: 'success', eventId, result };
  } catch (err) {
    const finishedAt = new Date(clock.now ? clock.now() : Date.now());
    const durationMs = Math.max(0, finishedAt.getTime() - startedAt.getTime());

    // Simulated crash: worker dies abruptly mid-processing
    if (err.name === 'CrashSimulated') {
      await repo.recordAttempt({
        eventId,
        attempt: attemptNumber,
        startedAt,
        finishedAt,
        outcome: 'crashed',
        error: err.message,
        durationMs,
      });

      // DO NOT ack and DO NOT update status (message stays pending, row stays 'processing')
      // The recovery sweeper will reclaim it after the visibility timeout
      return { outcome: 'crashed', eventId, error: err.message };
    }

    const isTimeout = err.name === 'TimeoutError';
    const isPermanent = err.name === 'PermanentError';
    const attemptOutcome = isTimeout ? 'timeout' : isPermanent ? 'permanent' : 'error';

    // Record failure attempt
    await repo.recordAttempt({
      eventId,
      attempt: attemptNumber,
      startedAt,
      finishedAt,
      outcome: attemptOutcome,
      error: err.message,
      durationMs,
    });

    // Determine whether to retry or dead-letter
    const decision = decideAfterFailure({
      attempts: attemptNumber,
      maxAttempts: claimed.max_attempts,
      permanent: isPermanent,
      now: finishedAt,
      backoffOpts,
    });

    if (decision.status === 'retrying') {
      await repo.markRetrying(eventId, decision.nextAttemptAt, err.message);
      // ACK the current message so it does not stay pending; sweeper / fast timer republishes
      await transport.ack(messageId);
      return {
        outcome: 'retrying',
        eventId,
        nextAttemptAt: decision.nextAttemptAt,
        delayMs: decision.delayMs,
        error: err.message,
      };
    } else {
      // Status 'dead'
      await repo.markDead(eventId, err.message);
      // Mirror to DLQ stream
      await transport.deadLetterPublish({
        eventId,
        error: err.message,
        attempts: attemptNumber,
      });
      await transport.ack(messageId);
      return {
        outcome: 'dead',
        eventId,
        error: err.message,
      };
    }
  }
}

/**
 * Worker engine managing adaptive polling, concurrent batch processing,
 * and graceful shutdown.
 */
export class Worker {
  /**
   * @param {object} [opts]
   * @param {import('./repo.js').EventRepository} [opts.repo]
   * @param {import('./transport/base.js').EventTransport} [opts.transport]
   */
  constructor(opts = {}) {
    const config = getConfig();

    this.repo = opts.repo || getRepo();
    this.transport = opts.transport || getTransport();
    this.concurrency = opts.concurrency || config.WORKER_CONCURRENCY || 5;
    this.visibilityMs = opts.visibilityMs || config.VISIBILITY_TIMEOUT_MS || 30000;
    this.handlerTimeoutMs = opts.handlerTimeoutMs || config.HANDLER_TIMEOUT_MS || 8000;
    this.idlePollMaxMs = opts.idlePollMaxMs || config.IDLE_POLL_MAX_MS || 60000;
    this.backoffOpts = {
      baseMs: config.RETRY_BASE_MS || 2000,
      maxMs: config.RETRY_MAX_MS || 60000,
    };

    this.instanceId = `${os.hostname()}-${process.pid}`;
    this.running = false;
    this.lastPollAt = null;
    this.currentIdleMs = 500;
    this.startedAt = null;
    this.inFlightCount = 0;
    this.wakeResolve = null;
    this.retryTimers = new Set();
  }

  /**
   * Wake up worker from idle sleep immediately (e.g. after fresh event ingestion)
   */
  wake() {
    this.currentIdleMs = 500;
    if (this.wakeResolve) {
      const resolve = this.wakeResolve;
      this.wakeResolve = null;
      resolve();
    }
  }

  /**
   * Helper sleep that can be interrupted by wake()
   * @param {number} ms
   */
  async sleepWithWake(ms) {
    if (!this.running) return;
    return new Promise((resolve) => {
      let timer = null;
      const onWake = () => {
        if (timer) clearTimeout(timer);
        resolve();
      };

      this.wakeResolve = onWake;

      timer = setTimeout(() => {
        this.wakeResolve = null;
        resolve();
      }, ms);
    });
  }

  /**
   * Start worker polling loop
   */
  async start() {
    if (this.running) return;
    this.running = true;
    this.startedAt = new Date();
    process.stdout.write(`[Worker] Started worker instance ${this.instanceId} (concurrency=${this.concurrency})\n`);

    // Ensure transport group initialized
    await this.transport.init();

    this.loop();
  }

  /**
   * Main adaptive polling loop
   */
  async loop() {
    while (this.running) {
      this.lastPollAt = new Date();

      try {
        const messages = await this.transport.readBatch({ count: this.concurrency });

        if (messages && messages.length > 0) {
          // Reset adaptive polling to fast rate
          this.currentIdleMs = 500;

          // Track in-flight executions for graceful shutdown
          this.inFlightCount += messages.length;

          // Process batch concurrently with Promise.allSettled
          await Promise.allSettled(
            messages.map(async (msg) => {
              try {
                const res = await processMessage(msg, {
                  repo: this.repo,
                  transport: this.transport,
                  visibilityMs: this.visibilityMs,
                  handlerTimeoutMs: this.handlerTimeoutMs,
                  backoffOpts: this.backoffOpts,
                  log: (m) => process.stdout.write(`[Worker Log] ${m}\n`),
                });

                // Schedule fast-path retry if needed
                if (res.outcome === 'retrying' && res.delayMs) {
                  this.scheduleFastRetry(res.eventId, res.delayMs);
                }
              } catch (err) {
                process.stderr.write(`[Worker Item Error] Event ${msg.eventId}: ${err.message}\n`);
              } finally {
                this.inFlightCount--;
              }
            })
          );
        } else {
          // No messages: sleep with exponential backoff up to idlePollMaxMs
          const sleepDuration = this.currentIdleMs;
          this.currentIdleMs = Math.min(this.idlePollMaxMs, this.currentIdleMs * 2);
          await this.sleepWithWake(sleepDuration);
        }
      } catch (err) {
        process.stderr.write(`[Worker Loop Error] ${err.message}. Backing off 3s...\n`);
        await this.sleepWithWake(3000);
      }
    }
  }

  /**
   * Fast-path in-process retry scheduler
   * @param {string} eventId
   * @param {number} delayMs
   */
  scheduleFastRetry(eventId, delayMs) {
    if (!this.running) return;

    const timer = setTimeout(async () => {
      this.retryTimers.delete(timer);
      try {
        const streamId = await this.transport.publish(eventId);
        await this.repo.markQueued(eventId, streamId);
        this.wake();
      } catch (err) {
        // Sweeper acts as durable safety net
        process.stderr.write(`[Fast Retry Error] Event ${eventId}: ${err.message}\n`);
      }
    }, delayMs);

    if (timer.unref) timer.unref();
    this.retryTimers.add(timer);
  }

  /**
   * Return status for monitoring
   */
  getInfo() {
    return {
      instanceId: this.instanceId,
      concurrency: this.concurrency,
      running: this.running,
      lastPollAt: this.lastPollAt ? this.lastPollAt.toISOString() : null,
      currentIdleMs: this.currentIdleMs,
      startedAt: this.startedAt ? this.startedAt.toISOString() : null,
      uptimeSeconds: this.startedAt ? Math.floor((Date.now() - this.startedAt.getTime()) / 1000) : 0,
      inFlightCount: this.inFlightCount,
    };
  }

  /**
   * Graceful stop: wait up to 10s for in-flight tasks
   */
  async stop() {
    this.running = false;
    this.wake();

    // Clear all pending in-memory retry timers (sweeper will recover them durably)
    for (const timer of this.retryTimers) {
      clearTimeout(timer);
    }
    this.retryTimers.clear();

    const deadline = Date.now() + 10000;
    while (this.inFlightCount > 0 && Date.now() < deadline) {
      await new Promise((res) => setTimeout(res, 200));
    }

    process.stdout.write(`[Worker] Worker ${this.instanceId} stopped cleanly.\n`);
  }
}

let workerInstance = null;
export function getWorker() {
  if (!workerInstance) {
    workerInstance = new Worker();
  }
  return workerInstance;
}
