import { getRepo } from './repo.js';
import { getTransport } from './transport/index.js';
import { getConfig } from './config.js';

/**
 * Recovery sweeper that recovers:
 * 1. Events due for retry ('retrying' with next_attempt_at <= now())
 * 2. Events stuck in 'received' (e.g. initial XADD crashed/failed)
 * 3. Events stuck in 'processing' beyond VISIBILITY_TIMEOUT_MS (crashed workers)
 */
export class Sweeper {
  /**
   * @param {object} [opts]
   * @param {import('./repo.js').EventRepository} [opts.repo]
   * @param {import('./transport/base.js').EventTransport} [opts.transport]
   * @param {Function} [opts.onWakeWorker]
   */
  constructor(opts = {}) {
    const config = getConfig();

    this.repo = opts.repo || getRepo();
    this.transport = opts.transport || getTransport();
    this.onWakeWorker = opts.onWakeWorker || (() => {});
    this.intervalMs = opts.intervalMs || config.SWEEP_INTERVAL_MS || 30000;
    this.visibilityMs = opts.visibilityMs || config.VISIBILITY_TIMEOUT_MS || 30000;

    this.timer = null;
    this.running = false;
    this.lastAutoClaimAt = 0;
    this.lastSweepAt = null;
  }

  /**
   * Run a single sweep pass
   */
  async sweep() {
    this.lastSweepAt = new Date();
    let wokeWorker = false;

    try {
      // 1. Retry due: Find retrying events ready for another attempt
      const dueRetryIds = await this.repo.getDueRetries(50);
      for (const eventId of dueRetryIds) {
        try {
          const streamId = await this.transport.publish(eventId);
          await this.repo.markQueued(eventId, streamId);
          wokeWorker = true;
        } catch (err) {
          process.stderr.write(`[Sweeper] Failed to republish retry event ${eventId}: ${err.message}\n`);
        }
      }

      // 2. Stuck 'received': Events inserted but never queued in Redis stream
      const stuckIds = await this.repo.getStuckReceived(30000, 50);
      for (const eventId of stuckIds) {
        try {
          const streamId = await this.transport.publish(eventId);
          await this.repo.markQueued(eventId, streamId);
          wokeWorker = true;
        } catch (err) {
          process.stderr.write(`[Sweeper] Failed to republish stuck received event ${eventId}: ${err.message}\n`);
        }
      }

      // 3. Stale processing: First mark dead any stale rows that exhausted max_attempts
      const exhausted = await this.repo.getStaleProcessingExhausted(this.visibilityMs, 50);
      for (const item of exhausted) {
        try {
          const errMessage = item.last_error || 'Visibility timeout exceeded max attempts';
          await this.repo.markDead(item.id, errMessage);
          await this.transport.deadLetterPublish({
            eventId: item.id,
            error: errMessage,
            attempts: 99,
          });
        } catch (err) {
          process.stderr.write(`[Sweeper] Failed to mark exhausted stale event ${item.id} dead: ${err.message}\n`);
        }
      }

      // Check if DB shows any stale processing rows or 5 minutes elapsed
      const staleCount = await this.repo.getStaleProcessingCount(this.visibilityMs);
      const fiveMinutesPassed = Date.now() - this.lastAutoClaimAt >= 5 * 60 * 1000;

      if (staleCount > 0 || fiveMinutesPassed) {
        this.lastAutoClaimAt = Date.now();
        const reclaimResult = await this.transport.reclaimStale({
          minIdleMs: this.visibilityMs,
          count: 50,
        });

        if (reclaimResult.messages.length > 0) {
          wokeWorker = true;
        }
      }

      if (wokeWorker) {
        this.onWakeWorker();
      }
    } catch (err) {
      process.stderr.write(`[Sweeper Pass Error] ${err.message}\n`);
    }
  }

  /**
   * Start recurring background sweeper
   */
  start() {
    if (this.running) return;
    this.running = true;

    // Run once immediately at startup
    this.sweep();

    // Schedule recurring interval
    this.timer = setInterval(() => {
      this.sweep();
    }, this.intervalMs);

    if (this.timer.unref) {
      this.timer.unref();
    }

    process.stdout.write(`[Sweeper] Started recovery sweeper (interval=${this.intervalMs}ms)\n`);
  }

  /**
   * Stop sweeper cleanly
   */
  stop() {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    process.stdout.write('[Sweeper] Stopped recovery sweeper.\n');
  }

  getInfo() {
    return {
      running: this.running,
      intervalMs: this.intervalMs,
      lastSweepAt: this.lastSweepAt ? this.lastSweepAt.toISOString() : null,
    };
  }
}

let sweeperInstance = null;
export function getSweeper(opts) {
  if (!sweeperInstance) {
    sweeperInstance = new Sweeper(opts);
  }
  return sweeperInstance;
}
