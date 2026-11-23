import { PermanentError, CrashSimulated } from '../core/errors.js';
import { getConfig } from '../config.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomSleep = (min, max) => sleep(Math.floor(min + Math.random() * (max - min)));

/**
 * Handlers Registry
 * Every handler signature: async ({ event, attempt, log }) => resultObject
 * Handlers must be idempotent and must NOT touch Redis.
 */
export const handlers = {
  /**
   * demo.echo: Clean, fast success
   */
  'demo.echo': async ({ event, attempt, log }) => {
    await randomSleep(50, 300);
    return {
      echoed: true,
      data: event.payload,
      processedAttempt: attempt,
      timestamp: new Date().toISOString(),
    };
  },

  /**
   * demo.email: Simulated email delivery
   */
  'demo.email': async ({ event, attempt, log }) => {
    await randomSleep(100, 400);
    const to = event.payload?.to || 'dev@example.com';
    return {
      delivered: true,
      to,
      messageId: `msg_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
    };
  },

  /**
   * demo.flaky: Fails n times with a normal Error, then succeeds
   */
  'demo.flaky': async ({ event, attempt, log }) => {
    const failTimes = Number(event.payload?.failTimes ?? 2);
    if (attempt <= failTimes) {
      throw new Error(`Flaky external API error (attempt ${attempt} <= threshold ${failTimes})`);
    }
    return {
      resolved: true,
      attemptsRequired: attempt,
      recoveredAfterFailures: failTimes,
    };
  },

  /**
   * demo.poison: Always throws a normal Error -> retries until DLQ
   */
  'demo.poison': async ({ event, attempt, log }) => {
    await sleep(50);
    throw new Error('Poison message: corrupt data or external API 500 error');
  },

  /**
   * demo.permanent: Throws PermanentError -> straight to DLQ with 0 retries
   */
  'demo.permanent': async ({ event, attempt, log }) => {
    await sleep(50);
    throw new PermanentError('Invalid target account: resource permanently deleted');
  },

  /**
   * demo.slow: Waits longer than HANDLER_TIMEOUT_MS -> worker cancels with TimeoutError
   */
  'demo.slow': async ({ event, attempt, log }) => {
    const config = getConfig();
    const timeoutMs = config.HANDLER_TIMEOUT_MS || 8000;
    // Sleep well past the timeout
    await sleep(timeoutMs + 2000);
    return { slowSuccess: true };
  },

  /**
   * demo.crash: On attempt 1, throws CrashSimulated (worker dies mid-processing, no ACK).
   * On later attempts, succeeds.
   */
  'demo.crash': async ({ event, attempt, log }) => {
    if (attempt <= 1) {
      throw new CrashSimulated('Worker terminated abruptly during computation');
    }
    return {
      recovered: true,
      attempt,
      note: 'Successfully recovered and processed by visibility timeout sweeper',
    };
  },
};

/**
 * Resolve handler by event type.
 * Returns a fallback handler throwing PermanentError if unknown.
 * @param {string} type
 * @returns {Function}
 */
export function getHandler(type) {
  if (handlers[type]) {
    return handlers[type];
  }
  return async () => {
    throw new PermanentError(`no_handler: Unknown event type '${type}'`);
  };
}
