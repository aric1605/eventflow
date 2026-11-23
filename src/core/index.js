import crypto from 'node:crypto';
import { TimeoutError, PermanentError, CrashSimulated } from './errors.js';

export { TimeoutError, PermanentError, CrashSimulated };

/**
 * Compute exponential backoff with full jitter in [50%, 100%] of the exponential value.
 * Formula: min(maxMs, baseMs * 2^(attempt-1)) * jitter[0.5, 1.0]
 * @param {number} attempt - Current attempt count (1-based)
 * @param {object} [opts]
 * @param {number} [opts.baseMs=2000] - Base delay in milliseconds
 * @param {number} [opts.maxMs=60000] - Maximum delay ceiling in milliseconds
 * @returns {number} Backoff duration in milliseconds
 */
export function computeBackoff(attempt, { baseMs = 2000, maxMs = 60000 } = {}) {
  const safeAttempt = Math.max(1, attempt || 1);
  const rawExponential = Math.min(maxMs, baseMs * Math.pow(2, safeAttempt - 1));
  // Full jitter uniformly distributed between 50% and 100% of rawExponential
  const jitterFactor = 0.5 + Math.random() * 0.5;
  const backoff = Math.round(rawExponential * jitterFactor);
  return Math.min(maxMs, Math.max(1, backoff));
}

/**
 * Decide next status and retry timestamp following a failed execution attempt.
 * @param {object} params
 * @param {number} params.attempts - Total attempts completed so far (including this failure)
 * @param {number} params.maxAttempts - Maximum configured attempts
 * @param {boolean} [params.permanent=false] - If true or error is PermanentError, dead-letter immediately
 * @param {Date} [params.now=new Date()]
 * @param {object} [params.backoffOpts]
 * @returns {{ status: 'dead' } | { status: 'retrying', nextAttemptAt: Date, delayMs: number }}
 */
export function decideAfterFailure({
  attempts,
  maxAttempts,
  permanent = false,
  now = new Date(),
  backoffOpts = {},
}) {
  if (permanent || attempts >= maxAttempts) {
    return { status: 'dead' };
  }

  const delayMs = computeBackoff(attempts, backoffOpts);
  const nextAttemptAt = new Date(now.getTime() + delayMs);

  return {
    status: 'retrying',
    nextAttemptAt,
    delayMs,
  };
}

/**
 * Pure predicate: determines if an event can be claimed for execution.
 * @param {object} event
 * @param {string} event.status
 * @param {Date | string | null} [event.started_at]
 * @param {Date | number} [now=new Date()]
 * @param {number} [visibilityMs=30000]
 * @returns {boolean}
 */
export function canClaim(event, now = new Date(), visibilityMs = 30000) {
  if (!event || !event.status) return false;

  if (event.status === 'queued' || event.status === 'retrying') {
    return true;
  }

  if (event.status === 'processing') {
    if (!event.started_at) return false;
    const nowMs = typeof now === 'number' ? now : new Date(now).getTime();
    const startedMs = new Date(event.started_at).getTime();
    return nowMs - startedMs >= visibilityMs;
  }

  return false;
}

/**
 * Recursively sort keys of an object to produce a canonical representation.
 * @param {any} value
 * @returns {any}
 */
function canonicalize(value) {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  const sorted = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = canonicalize(value[key]);
  }
  return sorted;
}

/**
 * Compute SHA-256 hash of a JSON payload where key ordering is normalized.
 * @param {any} payload
 * @returns {string} Hex encoded SHA-256 hash
 */
export function hashPayload(payload) {
  const canonical = canonicalize(payload);
  const serialized = JSON.stringify(canonical);
  return crypto.createHash('sha256').update(serialized).digest('hex');
}

/**
 * Wrap a promise with a timeout. Rejects with TimeoutError if not resolved within ms.
 * @template T
 * @param {Promise<T>} promise
 * @param {number} ms
 * @returns {Promise<T>}
 */
export function withTimeout(promise, ms) {
  let timer = null;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new TimeoutError(`Operation timed out after ${ms}ms`));
    }, ms);
    // unref timer if available so Node doesn't hold event loop open
    if (timer.unref) {
      timer.unref();
    }
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) {
      clearTimeout(timer);
    }
  });
}
