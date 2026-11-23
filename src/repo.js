import { getPool } from './db.js';

/**
 * Event repository layer encapsulating all PostgreSQL queries.
 * Uses parameterized queries and row-level locking (FOR UPDATE SKIP LOCKED) where appropriate.
 */
export class EventRepository {
  /**
   * @param {import('pg').Pool} [pool]
   */
  constructor(pool = getPool()) {
    this.pool = pool;
  }

  /**
   * Insert a new event with status 'received'.
   * If idempotency_key is provided and conflicts:
   * Returns { event, isDuplicate: true }
   * Otherwise returns { event, isDuplicate: false }
   */
  async insertEvent({ type, payload, payloadHash, idempotencyKey = null, maxAttempts = 4 }) {
    if (idempotencyKey) {
      const insertQuery = `
        INSERT INTO events (type, payload, payload_hash, idempotency_key, max_attempts, status)
        VALUES ($1, $2, $3, $4, $5, 'received')
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *;
      `;
      const res = await this.pool.query(insertQuery, [type, payload, payloadHash, idempotencyKey, maxAttempts]);
      if (res.rows.length > 0) {
        return { event: res.rows[0], isDuplicate: false };
      }

      // Conflict: fetch existing row
      const selectQuery = 'SELECT * FROM events WHERE idempotency_key = $1;';
      const existing = await this.pool.query(selectQuery, [idempotencyKey]);
      return { event: existing.rows[0], isDuplicate: true };
    }

    const query = `
      INSERT INTO events (type, payload, payload_hash, max_attempts, status)
      VALUES ($1, $2, $3, $4, 'received')
      RETURNING *;
    `;
    const res = await this.pool.query(query, [type, payload, payloadHash, maxAttempts]);
    return { event: res.rows[0], isDuplicate: false };
  }

  /**
   * Mark an event as queued after successfully publishing to the transport stream.
   */
  async markQueued(id, streamId) {
    const query = `
      UPDATE events
      SET status = 'queued',
          stream_id = $2,
          queued_at = now()
      WHERE id = $1
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id, streamId]);
    return res.rows[0] || null;
  }

  /**
   * Atomically claim an event for processing.
   * Succeeds only if event is 'queued'/'retrying' OR stale 'processing' (older than visibilityMs).
   * Returns updated event with incremented attempts, or null if claim failed (duplicate delivery).
   */
  async claimEvent(id, visibilityMs = 30000) {
    const query = `
      UPDATE events
      SET status = 'processing',
          attempts = attempts + 1,
          started_at = now()
      WHERE id = $1
        AND (
          status IN ('queued', 'retrying')
          OR (status = 'processing' AND started_at <= now() - ($2 || ' milliseconds')::interval)
        )
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id, visibilityMs]);
    return res.rows[0] || null;
  }

  /**
   * Get full event record by ID (including payload).
   */
  async getEventById(id) {
    const query = 'SELECT * FROM events WHERE id = $1;';
    const res = await this.pool.query(query, [id]);
    return res.rows[0] || null;
  }

  /**
   * List attempts for an event ordered chronologically.
   */
  async getEventAttempts(eventId) {
    const query = 'SELECT * FROM event_attempts WHERE event_id = $1 ORDER BY attempt ASC;';
    const res = await this.pool.query(query, [eventId]);
    return res.rows;
  }

  /**
   * Record an execution attempt in the event_attempts audit log.
   */
  async recordAttempt({ eventId, attempt, startedAt, finishedAt, outcome, error = null, durationMs = 0 }) {
    const query = `
      INSERT INTO event_attempts (event_id, attempt, started_at, finished_at, outcome, error, duration_ms)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *;
    `;
    const res = await this.pool.query(query, [
      eventId,
      attempt,
      startedAt,
      finishedAt,
      outcome,
      error,
      Math.max(0, Math.round(durationMs)),
    ]);
    return res.rows[0];
  }

  /**
   * Mark event succeeded with result and processing time.
   */
  async markSucceeded(id, result = null, processingMs = 0) {
    const query = `
      UPDATE events
      SET status = 'succeeded',
          result = $2,
          finished_at = now(),
          processing_ms = $3
      WHERE id = $1
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id, result, processingMs]);
    return res.rows[0] || null;
  }

  /**
   * Mark event retrying with next_attempt_at and last_error.
   */
  async markRetrying(id, nextAttemptAt, lastError) {
    const query = `
      UPDATE events
      SET status = 'retrying',
          next_attempt_at = $2,
          last_error = $3
      WHERE id = $1
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id, nextAttemptAt, lastError]);
    return res.rows[0] || null;
  }

  /**
   * Mark event dead.
   */
  async markDead(id, lastError) {
    const query = `
      UPDATE events
      SET status = 'dead',
          last_error = $2,
          finished_at = now()
      WHERE id = $1
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id, lastError]);
    return res.rows[0] || null;
  }

  /**
   * Keyset pagination query for events.
   * Note: payloads are omitted from list queries for performance and payload safety.
   */
  async listEvents({ status = null, type = null, limit = 50, before = null }) {
    const safeLimit = Math.min(Math.max(1, Number(limit) || 50), 100);

    let beforeCreatedAt = null;
    let beforeId = null;
    if (before) {
      // Keyset cursor format: createdAt_id
      const parts = before.split('_');
      if (parts.length >= 2) {
        beforeCreatedAt = parts[0];
        beforeId = parts.slice(1).join('_');
      }
    }

    const query = `
      SELECT id, type, payload_hash, idempotency_key, status, attempts, max_attempts,
             next_attempt_at, last_error, result, stream_id, created_at, queued_at,
             started_at, finished_at, processing_ms
      FROM events
      WHERE ($1::text IS NULL OR status = $1)
        AND ($2::text IS NULL OR type = $2)
        AND (
          $3::timestamptz IS NULL OR (created_at, id) < ($3::timestamptz, $4::uuid)
        )
      ORDER BY created_at DESC, id DESC
      LIMIT $5;
    `;

    const res = await this.pool.query(query, [status, type, beforeCreatedAt, beforeId, safeLimit]);
    return res.rows;
  }

  /**
   * List dead events for DLQ management.
   */
  async listDeadEvents({ limit = 50, before = null }) {
    return this.listEvents({ status: 'dead', limit, before });
  }

  /**
   * Replay a dead event: resets attempts to 0 and status to 'queued'.
   */
  async replayDeadEvent(id) {
    const query = `
      UPDATE events
      SET status = 'queued',
          attempts = 0,
          next_attempt_at = NULL,
          last_error = NULL,
          result = NULL,
          queued_at = now(),
          finished_at = NULL,
          started_at = NULL
      WHERE id = $1 AND status = 'dead'
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id]);
    return res.rows[0] || null;
  }

  /**
   * Discard a dead event.
   */
  async discardDeadEvent(id) {
    const query = `
      UPDATE events
      SET last_error = COALESCE(last_error, '') || ' [discarded by operator]'
      WHERE id = $1 AND status = 'dead'
      RETURNING *;
    `;
    const res = await this.pool.query(query, [id]);
    return res.rows[0] || null;
  }

  /**
   * Replay all dead events up to a maximum limit.
   */
  async replayAllDeadEvents(limit = 100) {
    const query = `
      UPDATE events
      SET status = 'queued',
          attempts = 0,
          next_attempt_at = NULL,
          last_error = NULL,
          result = NULL,
          queued_at = now(),
          finished_at = NULL,
          started_at = NULL
      WHERE id IN (
        SELECT id FROM events
        WHERE status = 'dead'
        ORDER BY created_at ASC
        LIMIT $1
      )
      RETURNING id;
    `;
    const res = await this.pool.query(query, [limit]);
    return res.rows.map((r) => r.id);
  }

  /**
   * Sweeper: Find events due for retry.
   */
  async getDueRetries(limit = 50) {
    const query = `
      SELECT id FROM events
      WHERE status = 'retrying' AND next_attempt_at <= now()
      FOR UPDATE SKIP LOCKED
      LIMIT $1;
    `;
    const res = await this.pool.query(query, [limit]);
    return res.rows.map((r) => r.id);
  }

  /**
   * Sweeper: Find events stuck in 'received' (e.g. initial XADD failed).
   */
  async getStuckReceived(stuckOlderThanMs = 30000, limit = 50) {
    const query = `
      SELECT id FROM events
      WHERE status = 'received' AND created_at <= now() - ($1 || ' milliseconds')::interval
      FOR UPDATE SKIP LOCKED
      LIMIT $2;
    `;
    const res = await this.pool.query(query, [stuckOlderThanMs, limit]);
    return res.rows.map((r) => r.id);
  }

  /**
   * Sweeper: Check how many events are currently stale in 'processing'.
   */
  async getStaleProcessingCount(visibilityMs = 30000) {
    const query = `
      SELECT COUNT(*)::int AS count
      FROM events
      WHERE status = 'processing' AND started_at <= now() - ($1 || ' milliseconds')::interval;
    `;
    const res = await this.pool.query(query, [visibilityMs]);
    return res.rows[0]?.count || 0;
  }

  /**
   * Sweeper: Stale processing events that have exhausted max_attempts.
   */
  async getStaleProcessingExhausted(visibilityMs = 30000, limit = 50) {
    const query = `
      SELECT id, last_error FROM events
      WHERE status = 'processing'
        AND started_at <= now() - ($1 || ' milliseconds')::interval
        AND attempts >= max_attempts
      FOR UPDATE SKIP LOCKED
      LIMIT $2;
    `;
    const res = await this.pool.query(query, [visibilityMs, limit]);
    return res.rows;
  }

  /**
   * Aggregate metrics for dashboard and /metrics endpoint.
   */
  async getMetricsData() {
    const client = await this.pool.connect();
    try {
      // 1. Status counts
      const statusRes = await client.query(`
        SELECT status, COUNT(*)::int AS count
        FROM events
        GROUP BY status;
      `);
      const statusCounts = {
        received: 0,
        queued: 0,
        processing: 0,
        succeeded: 0,
        retrying: 0,
        dead: 0,
      };
      for (const row of statusRes.rows) {
        if (statusCounts[row.status] !== undefined) {
          statusCounts[row.status] = row.count;
        }
      }

      // 2. Processed counts (1m, 5m, 60m)
      const processedRes = await client.query(`
        SELECT
          COUNT(*) FILTER (WHERE finished_at >= now() - interval '1 minute')::int AS last_1m,
          COUNT(*) FILTER (WHERE finished_at >= now() - interval '5 minutes')::int AS last_5m,
          COUNT(*) FILTER (WHERE finished_at >= now() - interval '60 minutes')::int AS last_60m
        FROM events
        WHERE status IN ('succeeded', 'dead');
      `);
      const processed = processedRes.rows[0] || { last_1m: 0, last_5m: 0, last_60m: 0 };

      // 3. Per-minute throughput series for last 30 minutes
      const seriesRes = await client.query(`
        SELECT
          to_char(date_trunc('minute', finished_at), 'YYYY-MM-DD"T"HH24:MI:00"Z"') AS minute,
          COUNT(*)::int AS count
        FROM events
        WHERE finished_at >= now() - interval '30 minutes'
          AND status IN ('succeeded', 'dead')
        GROUP BY minute
        ORDER BY minute ASC;
      `);

      // 4. Latency percentiles (p50, p95) over last 1 hour
      const latencyRes = await client.query(`
        SELECT
          COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY processing_ms), 0)::int AS p50,
          COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY processing_ms), 0)::int AS p95
        FROM events
        WHERE status = 'succeeded'
          AND finished_at >= now() - interval '1 hour'
          AND processing_ms IS NOT NULL;
      `);
      const latency = latencyRes.rows[0] || { p50: 0, p95: 0 };

      // 5. Oldest queued event age
      const oldestRes = await client.query(`
        SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))) * 1000, 0)::bigint AS oldest_queued_ms
        FROM events
        WHERE status IN ('queued', 'retrying');
      `);
      const oldestQueuedMs = oldestRes.rows[0]?.oldest_queued_ms || 0;

      return {
        statusCounts,
        processed: {
          last1m: processed.last_1m,
          last5m: processed.last_5m,
          last60m: processed.last_60m,
        },
        throughputSeries: seriesRes.rows,
        latency: {
          p50: latency.p50,
          p95: latency.p95,
        },
        oldestQueuedMs: Number(oldestQueuedMs),
      };
    } finally {
      client.release();
    }
  }

  /**
   * Health check query
   */
  async checkHealth() {
    const res = await this.pool.query('SELECT 1 AS ok;');
    return res.rows.length > 0;
  }
}

let repoInstance = null;
export function getRepo() {
  if (!repoInstance) {
    repoInstance = new EventRepository();
  }
  return repoInstance;
}
