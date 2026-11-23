/**
 * EventFlow Transport Interface Contract
 *
 * This base class defines the transport abstraction for EventFlow.
 * Redis Streams implements this interface (src/transport/redisStreams.js).
 *
 * How an Apache Kafka implementation would slot in:
 * ------------------------------------------------
 * If migrating or adding Kafka support:
 * 1. publish(eventId):
 *    producer.send({ topic: 'events.main', messages: [{ key: eventId, value: JSON.stringify({ eventId }) }] })
 * 2. readBatch({ count }):
 *    Kafka consumer group ('eventflow') polling up to `count` records from topic 'events.main'
 * 3. ack(messageId):
 *    Kafka consumer commits partition offsets for processed messages
 * 4. reclaimStale({ minIdleMs, count }):
 *    Kafka rebalances partitions automatically when heartbeats fail (session.timeout.ms / max.poll.interval.ms);
 *    stale processing retry loops typically route unacked messages to intermediate retry topics
 * 5. depth():
 *    Query partition high-water marks minus committed consumer offsets via Kafka AdminClient
 * 6. pendingSummary():
 *    Kafka consumer group lag summary across partition assignments
 * 7. deadLetterPublish({ eventId, error, attempts }):
 *    producer.send({ topic: 'events.dlq', messages: [{ key: eventId, value: JSON.stringify({ eventId, error, attempts }) }] })
 */

export class EventTransport {
  /**
   * Initialize transport resources (e.g. consumer groups, stream creation)
   * @returns {Promise<void>}
   */
  async init() {
    throw new Error('Not implemented');
  }

  /**
   * Publish an event ID to the main event stream
   * @param {string} eventId - UUID of the event
   * @returns {Promise<string>} Transport message ID
   */
  async publish(eventId) {
    throw new Error('Not implemented');
  }

  /**
   * Read a batch of messages for this consumer group instance without blocking
   * @param {object} opts
   * @param {number} [opts.count=5]
   * @returns {Promise<Array<{ messageId: string, eventId: string }>>}
   */
  async readBatch({ count = 5 } = {}) {
    throw new Error('Not implemented');
  }

  /**
   * Acknowledge that a message has been processed
   * @param {string} messageId
   * @returns {Promise<void>}
   */
  async ack(messageId) {
    throw new Error('Not implemented');
  }

  /**
   * Reclaim abandoned/stale messages from dead consumers using XAUTOCLAIM
   * @param {object} opts
   * @param {number} [opts.minIdleMs=30000]
   * @param {number} [opts.count=50]
   * @returns {Promise<{ messages: Array<{ messageId: string, eventId: string }>, nextCursor: string }>}
   */
  async reclaimStale({ minIdleMs = 30000, count = 50 } = {}) {
    throw new Error('Not implemented');
  }

  /**
   * Current queue depth (messages in main stream)
   * @returns {Promise<number>}
   */
  async depth() {
    throw new Error('Not implemented');
  }

  /**
   * Pending summary of in-flight unacknowledged messages
   * @returns {Promise<{ count: number, oldestIdleMs: number }>}
   */
  async pendingSummary() {
    throw new Error('Not implemented');
  }

  /**
   * Publish a dead-letter mirror entry to events:dlq
   * @param {object} params
   * @param {string} params.eventId
   * @param {string} [params.error]
   * @param {number} [params.attempts]
   * @returns {Promise<string>}
   */
  async deadLetterPublish({ eventId, error = '', attempts = 0 }) {
    throw new Error('Not implemented');
  }

  /**
   * Close connections cleanly
   * @returns {Promise<void>}
   */
  async close() {
    throw new Error('Not implemented');
  }
}
