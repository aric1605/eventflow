import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import { z } from 'zod';

import { getConfig, maskUrl } from './config.js';
import { getPool, closePool } from './db.js';
import { runMigrations } from '../scripts/migrate.js';
import { getRepo } from './repo.js';
import { getTransport } from './transport/index.js';
import { getWorker } from './worker.js';
import { getSweeper } from './sweeper.js';
import { hashPayload, withTimeout } from './core/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.resolve(__dirname, '../public');

/**
 * Constant-time string comparison to prevent timing attacks
 */
function safeCompare(a, b) {
  if (!a || !b || typeof a !== 'string' || typeof b !== 'string') return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * In-memory rate limiting structures
 */
const ipRateLimits = new Map();
function checkIpRateLimit(ip, limit = 120) {
  const now = Date.now();
  const entry = ipRateLimits.get(ip) || { count: 0, resetAt: now + 60000 };
  if (now > entry.resetAt) {
    entry.count = 0;
    entry.resetAt = now + 60000;
  }
  entry.count++;
  ipRateLimits.set(ip, entry);
  return entry.count <= limit;
}

// Global demo emit rate limit: max 30 events / minute
let demoWindowResetAt = Date.now() + 60000;
let demoEmittedInWindow = 0;
function checkDemoGlobalLimit(count = 1) {
  const now = Date.now();
  if (now > demoWindowResetAt) {
    demoEmittedInWindow = 0;
    demoWindowResetAt = now + 60000;
  }
  if (demoEmittedInWindow + count > 30) {
    return false;
  }
  demoEmittedInWindow += count;
  return true;
}

// Clean up stale IP records every 5 minutes
const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of ipRateLimits.entries()) {
    if (now > entry.resetAt) {
      ipRateLimits.delete(ip);
    }
  }
}, 300000);
if (cleanupTimer.unref) cleanupTimer.unref();

/**
 * Ingestion request payload schema
 */
const ingestSchema = z.object({
  type: z.string().regex(/^[a-z0-9_.-]{1,64}$/, 'Type must match ^[a-z0-9_.-]{1,64}$'),
  payload: z.record(z.any()).refine((p) => {
    try {
      return JSON.stringify(p).length <= 16384;
    } catch {
      return false;
    }
  }, 'Payload serialized size must not exceed 16 KB'),
  idempotencyKey: z.string().min(1).max(128).optional(),
  maxAttempts: z.coerce.number().int().min(1).max(10).optional(),
});

/**
 * Demo emit request schema
 */
const demoEmitSchema = z.object({
  scenario: z.enum(['success', 'flaky', 'poison', 'permanent', 'slow', 'crash', 'burst']),
});

/**
 * Cached metrics singleton with 3s TTL
 */
let cachedMetrics = null;
let metricsExpiresAt = 0;

async function getCachedMetrics(repo, transport, worker) {
  const now = Date.now();
  if (cachedMetrics && now < metricsExpiresAt) {
    return cachedMetrics;
  }

  const [dbData, streamDepth, pending] = await Promise.all([
    repo.getMetricsData(),
    transport.depth(),
    transport.pendingSummary(),
  ]);

  const workerInfo = worker ? worker.getInfo() : null;

  cachedMetrics = {
    ...dbData,
    streamDepth,
    pendingCount: pending.count,
    oldestPendingIdleMs: pending.oldestIdleMs,
    dlqCount: dbData.statusCounts.dead,
    worker: workerInfo,
    cachedAt: new Date().toISOString(),
  };

  metricsExpiresAt = now + 3000;
  return cachedMetrics;
}

/**
 * Build Fastify application
 */
export async function buildApp(opts = {}) {
  const config = getConfig();

  const fastify = Fastify({
    logger: {
      level: config.LOG_LEVEL,
      serializers: {
        // Redact request/response bodies from logs to protect payloads
        req(req) {
          return {
            id: req.id,
            method: req.method,
            url: req.url,
            remoteAddress: req.ip,
          };
        },
      },
    },
    bodyLimit: 32768, // 32 KB body limit
    requestIdHeader: 'x-request-id',
  });

  const repo = opts.repo || getRepo();
  const transport = opts.transport || getTransport();
  const worker = opts.worker || (config.ROLE !== 'api' ? getWorker() : null);

  // Serve static dashboard files from public/
  await fastify.register(fastifyStatic, {
    root: PUBLIC_DIR,
    prefix: '/',
  });

  // Global error handler with standard JSON shape { error, message }
  fastify.setErrorHandler((error, request, reply) => {
    if (error.validation) {
      reply.code(400).send({
        error: 'validation_error',
        message: error.message,
      });
      return;
    }
    const statusCode = error.statusCode || 500;
    reply.code(statusCode).send({
      error: error.name || 'internal_server_error',
      message: error.message || 'An unexpected error occurred',
    });
  });

  // Auth helper
  function checkAuth(req, reply, isDemoExempt = false) {
    if (config.DEMO_MODE && isDemoExempt) {
      return true;
    }
    const apiKeyHeader = req.headers['x-api-key'];
    if (apiKeyHeader && safeCompare(apiKeyHeader, config.API_KEY)) {
      return true;
    }
    reply.code(401).send({
      error: 'unauthorized',
      message: 'Valid x-api-key header required',
    });
    return false;
  }

  // IP rate limit hook
  fastify.addHook('preHandler', async (req, reply) => {
    if (req.url.startsWith('/events') || req.url.startsWith('/demo/emit')) {
      if (!checkIpRateLimit(req.ip, 120)) {
        reply.code(429).send({
          error: 'rate_limited',
          message: 'IP rate limit exceeded (max 120 requests/minute)',
        });
      }
    }
  });

  // 1. Health check (fast, no heavy lifting, 2s timeout)
  fastify.get('/health', async (req, reply) => {
    const pgPromise = withTimeout(repo.checkHealth(), 2000)
      .then(() => true)
      .catch(() => false);
    const redisPromise = withTimeout(transport.ping(), 2000)
      .then(() => true)
      .catch(() => false);

    const [pgOk, redisOk] = await Promise.all([pgPromise, redisPromise]);

    if (pgOk && redisOk) {
      return reply.code(200).send({
        status: 'ok',
        postgres: 'up',
        redis: 'up',
      });
    }

    return reply.code(503).send({
      status: 'degraded',
      postgres: pgOk ? 'up' : 'down',
      redis: redisOk ? 'up' : 'down',
    });
  });

  // 2. Metrics JSON endpoint (cached 3s)
  fastify.get('/metrics', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;
    const metrics = await getCachedMetrics(repo, transport, worker);
    return reply.code(200).send(metrics);
  });

  // 3. Prometheus exposition endpoint (under 40 lines)
  fastify.get('/metrics/prometheus', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;
    const m = await getCachedMetrics(repo, transport, worker);
    const lines = [
      '# HELP eventflow_stream_depth Current Redis stream length',
      '# TYPE eventflow_stream_depth gauge',
      `eventflow_stream_depth ${m.streamDepth}`,
      '# HELP eventflow_events_status Status count of events in Postgres',
      '# TYPE eventflow_events_status gauge',
      `eventflow_events_status{status="received"} ${m.statusCounts.received || 0}`,
      `eventflow_events_status{status="queued"} ${m.statusCounts.queued || 0}`,
      `eventflow_events_status{status="processing"} ${m.statusCounts.processing || 0}`,
      `eventflow_events_status{status="succeeded"} ${m.statusCounts.succeeded || 0}`,
      `eventflow_events_status{status="retrying"} ${m.statusCounts.retrying || 0}`,
      `eventflow_events_status{status="dead"} ${m.statusCounts.dead || 0}`,
      '# HELP eventflow_pending_in_flight In-flight pending stream messages',
      '# TYPE eventflow_pending_in_flight gauge',
      `eventflow_pending_in_flight ${m.pendingCount}`,
      '# HELP eventflow_processing_latency_ms Latency percentiles',
      '# TYPE eventflow_processing_latency_ms gauge',
      `eventflow_processing_latency_ms{quantile="0.5"} ${m.latency.p50 || 0}`,
      `eventflow_processing_latency_ms{quantile="0.95"} ${m.latency.p95 || 0}`,
    ];
    reply.header('content-type', 'text/plain; version=0.0.4; charset=utf-8');
    return reply.code(200).send(lines.join('\n') + '\n');
  });

  // 4. Ingest Event: POST /events (requires API Key)
  fastify.post('/events', async (req, reply) => {
    if (!checkAuth(req, reply, false)) return;

    // Body parsing & validation
    const parsed = ingestSchema.safeParse(req.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'validation_error',
        message: parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join(', '),
      });
    }

    const { type, payload } = parsed.data;
    const idempotencyKey = parsed.data.idempotencyKey || req.headers['idempotency-key'] || null;
    const maxAttempts = parsed.data.maxAttempts || config.MAX_ATTEMPTS || 4;
    const payloadHash = hashPayload(payload);

    // Insert with idempotency guard
    const { event, isDuplicate } = await repo.insertEvent({
      type,
      payload,
      payloadHash,
      idempotencyKey,
      maxAttempts,
    });

    if (isDuplicate) {
      if (event.payload_hash === payloadHash) {
        return reply.code(200).send({ duplicate: true, event });
      } else {
        return reply.code(422).send({
          error: 'idempotency_conflict',
          message: 'Supplied payload does not match original request for this idempotency key',
        });
      }
    }

    // Publish to transport stream
    let streamId = null;
    try {
      streamId = await transport.publish(event.id);
      await repo.markQueued(event.id, streamId);
      if (worker) {
        worker.wake();
      }
    } catch (err) {
      req.log.warn(`Transport publish failed for event ${event.id}: ${err.message}. Sweeper will recover.`);
    }

    return reply.code(202).send({
      id: event.id,
      status: streamId ? 'queued' : 'received',
    });
  });

  // 5. List events: GET /events (read exempt in demo mode)
  fastify.get('/events', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;

    const query = req.query || {};
    const events = await repo.listEvents({
      status: query.status || null,
      type: query.type || null,
      limit: query.limit || 50,
      before: query.before || null,
    });

    return reply.code(200).send({ events });
  });

  // 6. Get event detail: GET /events/:id (read exempt in demo mode)
  fastify.get('/events/:id', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;

    const { id } = req.params;
    const event = await repo.getEventById(id);
    if (!event) {
      return reply.code(404).send({ error: 'not_found', message: `Event ${id} not found` });
    }

    const attempts = await repo.getEventAttempts(id);
    return reply.code(200).send({ event, attempts });
  });

  // 7. Dead-letter queue: GET /dlq
  fastify.get('/dlq', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;

    const query = req.query || {};
    const deadEvents = await repo.listDeadEvents({
      limit: query.limit || 50,
      before: query.before || null,
    });
    return reply.code(200).send({ deadEvents });
  });

  // 8. Replay single dead event: POST /dlq/:id/replay (demo exempt)
  fastify.post('/dlq/:id/replay', async (req, reply) => {
    if (!checkAuth(req, reply, true)) return;

    const { id } = req.params;
    const event = await repo.replayDeadEvent(id);
    if (!event) {
      return reply.code(404).send({ error: 'not_found', message: `Dead event ${id} not found or not in dead state` });
    }

    const streamId = await transport.publish(id);
    await repo.markQueued(id, streamId);
    if (worker) worker.wake();

    return reply.code(200).send({ replayed: true, id, status: 'queued' });
  });

  // 9. Discard single dead event: POST /dlq/:id/discard (requires API key)
  fastify.post('/dlq/:id/discard', async (req, reply) => {
    if (!checkAuth(req, reply, false)) return;

    const { id } = req.params;
    const event = await repo.discardDeadEvent(id);
    if (!event) {
      return reply.code(404).send({ error: 'not_found', message: `Dead event ${id} not found` });
    }

    return reply.code(200).send({ discarded: true, id });
  });

  // 10. Replay all dead events: POST /dlq/replay-all (requires API key, max 100)
  fastify.post('/dlq/replay-all', async (req, reply) => {
    if (!checkAuth(req, reply, false)) return;

    const replayedIds = await repo.replayAllDeadEvents(100);
    for (const id of replayedIds) {
      try {
        const streamId = await transport.publish(id);
        await repo.markQueued(id, streamId);
      } catch (err) {
        req.log.warn(`Failed to republish dead event ${id}: ${err.message}`);
      }
    }

    if (replayedIds.length > 0 && worker) {
      worker.wake();
    }

    return reply.code(200).send({ replayedCount: replayedIds.length, ids: replayedIds });
  });

  // 11. Demo emission endpoint: POST /demo/emit
  fastify.post('/demo/emit', async (req, reply) => {
    if (!config.DEMO_MODE) {
      return reply.code(403).send({ error: 'forbidden', message: 'Demo mode is disabled' });
    }

    const parsed = demoEmitSchema.safeParse(req.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'validation_error',
        message: parsed.error.issues.map((i) => i.message).join(', '),
      });
    }

    const { scenario } = parsed.data;
    const isBurst = scenario === 'burst';
    const countToEmit = isBurst ? 20 : 1;

    // Guard 1: Stream depth check (> 200 items -> 429)
    const currentDepth = await transport.depth();
    if (currentDepth > 200) {
      return reply.code(429).send({
        error: 'rate_limited',
        message: `Stream depth (${currentDepth}) exceeds 200. Allow workers to catch up before emitting more.`,
      });
    }

    // Guard 2: Global demo emission limit (30 events / minute)
    if (!checkDemoGlobalLimit(countToEmit)) {
      return reply.code(429).send({
        error: 'rate_limited',
        message: 'Demo rate limit exceeded (maximum 30 events per minute).',
      });
    }

    const emitted = [];

    // Helper to emit one demo event (ignores any client payload)
    async function emitSingle(type, payload, maxAttempts = 4) {
      const payloadHash = hashPayload(payload);
      const { event } = await repo.insertEvent({
        type,
        payload,
        payloadHash,
        maxAttempts,
      });

      const streamId = await transport.publish(event.id);
      await repo.markQueued(event.id, streamId);
      emitted.push({ id: event.id, type, status: 'queued' });
    }

    if (scenario === 'success') {
      await emitSingle('demo.echo', { message: 'Demo hello from EventFlow', timestamp: new Date().toISOString() });
    } else if (scenario === 'flaky') {
      // Fails twice, then succeeds on attempt 3
      await emitSingle('demo.flaky', { failTimes: 2 }, 4);
    } else if (scenario === 'poison') {
      // Always fails until max attempts -> DLQ
      await emitSingle('demo.poison', { poison: true, sample: 123 }, 3);
    } else if (scenario === 'permanent') {
      // Throws PermanentError -> straight to DLQ
      await emitSingle('demo.permanent', { accountId: 'deleted_user_449' }, 3);
    } else if (scenario === 'slow') {
      // Exceeds handler timeout -> TimeoutError
      await emitSingle('demo.slow', { durationMs: 10000 }, 3);
    } else if (scenario === 'crash') {
      // Throws CrashSimulated on first attempt, recovered by sweeper
      await emitSingle('demo.crash', { simulate: 'hard_crash' }, 3);
    } else if (scenario === 'burst') {
      // Emit 20 demo.echo events
      for (let i = 1; i <= 20; i++) {
        await emitSingle('demo.echo', { burstIndex: i, time: new Date().toISOString() });
      }
    }

    if (worker) {
      worker.wake();
    }

    return reply.code(200).send({
      scenario,
      count: emitted.length,
      events: emitted,
    });
  });

  return fastify;
}

/**
 * Server startup bootstrap
 */
export async function startServer() {
  const config = getConfig();

  process.stdout.write(
    `[EventFlow] Bootstrapping (role=${config.ROLE}, port=${config.PORT}, demo=${config.DEMO_MODE})\n`
  );
  process.stdout.write(`[EventFlow] Database: ${maskUrl(config.DATABASE_URL)}\n`);
  process.stdout.write(`[EventFlow] Redis: ${maskUrl(config.REDIS_URL)}\n`);

  // Run database migrations idempotently at startup
  try {
    const applied = await runMigrations();
    if (applied.length > 0) {
      process.stdout.write(`[EventFlow] Applied migrations: ${applied.join(', ')}\n`);
    } else {
      process.stdout.write('[EventFlow] Database schema up to date.\n');
    }
  } catch (err) {
    process.stderr.write(`[EventFlow] Migration startup error: ${err.message}\n`);
    throw err;
  }

  const repo = getRepo();
  const transport = getTransport();
  await transport.init();

  let worker = null;
  let sweeper = null;

  // Initialize Worker & Sweeper if role allows
  if (config.ROLE === 'all' || config.ROLE === 'worker') {
    worker = getWorker();
    await worker.start();

    sweeper = getSweeper({
      onWakeWorker: () => worker && worker.wake(),
    });
    sweeper.start();
  }

  // Initialize HTTP API app
  const app = await buildApp({ repo, transport, worker });

  // Listen on 0.0.0.0 and PORT
  await app.listen({ port: config.PORT, host: '0.0.0.0' });
  process.stdout.write(`[EventFlow] Server listening at http://0.0.0.0:${config.PORT}\n`);

  // Graceful shutdown handling
  async function shutdown(signal) {
    process.stdout.write(`\n[EventFlow] Received ${signal}. Initiating graceful shutdown...\n`);
    try {
      if (sweeper) sweeper.stop();
      if (worker) await worker.stop();
      await app.close();
      await transport.close();
      await closePool();
      process.stdout.write('[EventFlow] Shutdown complete. Exiting.\n');
      process.exit(0);
    } catch (err) {
      process.stderr.write(`[EventFlow] Shutdown error: ${err.message}\n`);
      process.exit(1);
    }
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  return { app, worker, sweeper, repo, transport };
}

// Direct CLI startup
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  startServer().catch((err) => {
    process.stderr.write(`[EventFlow Fatal] ${err.stack || err.message}\n`);
    process.exit(1);
  });
}
