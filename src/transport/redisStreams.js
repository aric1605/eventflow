import os from 'node:os';
import Redis from 'ioredis';
import { EventTransport } from './base.js';
import { getConfig, parseRedisConfig } from '../config.js';

/**
 * Helper to convert Redis array format [k1, v1, k2, v2] to key-value object
 */
function parseFieldArray(arr) {
  const obj = {};
  if (!Array.isArray(arr)) return obj;
  for (let i = 0; i < arr.length; i += 2) {
    obj[arr[i]] = arr[i + 1];
  }
  return obj;
}

/**
 * Redis Streams transport implementation using ioredis.
 * Adheres strictly to the EventTransport contract.
 */
export class RedisStreamsTransport extends EventTransport {
  /**
   * @param {object} [opts]
   * @param {string} [opts.redisUrl]
   * @param {string} [opts.mainStream='events:main']
   * @param {string} [opts.dlqStream='events:dlq']
   * @param {string} [opts.groupName='eventflow']
   * @param {string} [opts.consumerName]
   * @param {number} [opts.streamMaxlen]
   * @param {number} [opts.dlqMaxlen]
   * @param {Redis} [opts.client]
   */
  constructor(opts = {}) {
    super();
    const config = getConfig();

    this.mainStream = opts.mainStream || 'events:main';
    this.dlqStream = opts.dlqStream || 'events:dlq';
    this.groupName = opts.groupName || 'eventflow';
    this.consumerName =
      opts.consumerName || `${os.hostname()}-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
    this.streamMaxlen = opts.streamMaxlen || config.STREAM_MAXLEN || 10000;
    this.dlqMaxlen = opts.dlqMaxlen || config.DLQ_MAXLEN || 1000;

    if (opts.client) {
      this.client = opts.client;
    } else {
      const redisOptions = parseRedisConfig(opts.redisUrl || config.REDIS_URL);
      this.client = new Redis({
        ...redisOptions,
        maxRetriesPerRequest: 3,
        enableReadyCheck: true,
        lazyConnect: false,
        retryStrategy(times) {
          return Math.min(times * 500, 3000);
        },
      });

      this.client.on('error', (err) => {
        process.stderr.write(`[Redis Error] ${err.message}\n`);
      });
    }
  }

  /**
   * Initialize consumer group with MKSTREAM (ignore BUSYGROUP if it already exists)
   */
  async init() {
    try {
      await this.client.xgroup('CREATE', this.mainStream, this.groupName, '$', 'MKSTREAM');
    } catch (err) {
      if (!err.message || !err.message.includes('BUSYGROUP')) {
        throw err;
      }
      // Group already exists; ignore BUSYGROUP
    }
  }

  /**
   * Publish an eventId to the main event stream.
   * Capped with MAXLEN ~ to maintain bounded memory.
   */
  async publish(eventId) {
    const messageId = await this.client.xadd(
      this.mainStream,
      'MAXLEN',
      '~',
      this.streamMaxlen,
      '*',
      'eventId',
      eventId
    );
    return messageId;
  }

  /**
   * Read a batch of events using XREADGROUP without BLOCK (adaptive polling).
   */
  async readBatch({ count = 5 } = {}) {
    // XREADGROUP GROUP groupName consumerName COUNT count STREAMS streamName >
    const raw = await this.client.xreadgroup(
      'GROUP',
      this.groupName,
      this.consumerName,
      'COUNT',
      count,
      'STREAMS',
      this.mainStream,
      '>'
    );

    if (!raw || !Array.isArray(raw) || raw.length === 0) {
      return [];
    }

    const messages = [];
    for (const [streamName, entries] of raw) {
      if (streamName === this.mainStream && Array.isArray(entries)) {
        for (const [messageId, fieldArr] of entries) {
          const fields = parseFieldArray(fieldArr);
          if (fields.eventId) {
            messages.push({
              messageId,
              eventId: fields.eventId,
            });
          }
        }
      }
    }

    return messages;
  }

  /**
   * Acknowledge message processing in the consumer group
   */
  async ack(messageId) {
    await this.client.xack(this.mainStream, this.groupName, messageId);
  }

  /**
   * Reclaim abandoned/stale messages using XAUTOCLAIM
   */
  async reclaimStale({ minIdleMs = 30000, count = 50 } = {}) {
    try {
      // XAUTOCLAIM <stream> <group> <consumer> <min-idle-time> <start> [COUNT <count>]
      const result = await this.client.xautoclaim(
        this.mainStream,
        this.groupName,
        this.consumerName,
        minIdleMs,
        '0-0',
        'COUNT',
        count
      );

      if (!result || !Array.isArray(result)) {
        return { messages: [], nextCursor: '0-0' };
      }

      const nextCursor = result[0];
      const entries = result[1] || [];
      const messages = [];

      for (const [messageId, fieldArr] of entries) {
        if (!messageId || !fieldArr) continue;
        const fields = parseFieldArray(fieldArr);
        if (fields.eventId) {
          messages.push({
            messageId,
            eventId: fields.eventId,
          });
        }
      }

      return { messages, nextCursor };
    } catch (err) {
      process.stderr.write(`[Transport XAUTOCLAIM Error] ${err.message}\n`);
      return { messages: [], nextCursor: '0-0' };
    }
  }

  /**
   * Return main stream depth (XLEN)
   */
  async depth() {
    const len = await this.client.xlen(this.mainStream);
    return Number(len) || 0;
  }

  /**
   * Return pending summary from XPENDING
   */
  async pendingSummary() {
    try {
      const summary = await this.client.xpending(this.mainStream, this.groupName);
      if (!summary || !Array.isArray(summary)) {
        return { count: 0, oldestIdleMs: 0 };
      }

      const count = Number(summary[0]) || 0;
      let oldestIdleMs = 0;

      if (count > 0 && summary[1]) {
        // summary[1] is min message ID (e.g. 1700000000000-0)
        const minId = String(summary[1]);
        const timestampMs = parseInt(minId.split('-')[0], 10);
        if (!isNaN(timestampMs) && timestampMs > 0) {
          oldestIdleMs = Math.max(0, Date.now() - timestampMs);
        }
      }

      return { count, oldestIdleMs };
    } catch (err) {
      return { count: 0, oldestIdleMs: 0 };
    }
  }

  /**
   * Publish an informational mirror record to events:dlq
   */
  async deadLetterPublish({ eventId, error = '', attempts = 0 }) {
    const dlqId = await this.client.xadd(
      this.dlqStream,
      'MAXLEN',
      '~',
      this.dlqMaxlen,
      '*',
      'eventId',
      eventId,
      'error',
      String(error || '').slice(0, 500),
      'attempts',
      String(attempts || 0),
      'deadAt',
      new Date().toISOString()
    );
    return dlqId;
  }

  /**
   * Ping Redis to verify connectivity
   */
  async ping() {
    return this.client.ping();
  }

  /**
   * Close Redis connection
   */
  async close() {
    try {
      await this.client.quit();
    } catch {
      this.client.disconnect();
    }
  }
}

let transportInstance = null;
export function getTransport() {
  if (!transportInstance) {
    transportInstance = new RedisStreamsTransport();
  }
  return transportInstance;
}
