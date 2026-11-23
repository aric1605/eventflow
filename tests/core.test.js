import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  computeBackoff,
  decideAfterFailure,
  canClaim,
  hashPayload,
  withTimeout,
  TimeoutError,
  PermanentError,
  CrashSimulated,
} from '../src/core/index.js';
import { processMessage } from '../src/worker.js';

// Test 1: computeBackoff stays within jitter bounds and respects maxMs
test('1. computeBackoff stays within jitter bounds and respects maxMs', () => {
  const baseMs = 2000;
  const maxMs = 60000;

  // Check attempt 1: raw is 2000, bounds [1000, 2000]
  for (let i = 0; i < 30; i++) {
    const b1 = computeBackoff(1, { baseMs, maxMs });
    assert.ok(b1 >= 1000, `Backoff ${b1} should be >= 1000ms`);
    assert.ok(b1 <= 2000, `Backoff ${b1} should be <= 2000ms`);
  }

  // Check attempt 2: raw is 4000, bounds [2000, 4000]
  for (let i = 0; i < 30; i++) {
    const b2 = computeBackoff(2, { baseMs, maxMs });
    assert.ok(b2 >= 2000, `Backoff ${b2} should be >= 2000ms`);
    assert.ok(b2 <= 4000, `Backoff ${b2} should be <= 4000ms`);
  }

  // Check large attempt (10): raw is capped at maxMs (60000), bounds [30000, 60000]
  for (let i = 0; i < 30; i++) {
    const b10 = computeBackoff(10, { baseMs, maxMs });
    assert.ok(b10 >= 30000, `Backoff ${b10} should be >= 30000ms`);
    assert.ok(b10 <= maxMs, `Backoff ${b10} should be <= ${maxMs}ms`);
  }
});

// Test 2: decideAfterFailure retrying vs dead at max attempts, permanent goes straight to dead
test('2. decideAfterFailure: retrying vs dead at max attempts, and permanent errors go straight to dead', () => {
  const now = new Date('2026-01-01T12:00:00.000Z');

  // Case A: normal error with attempts < maxAttempts -> retrying
  const d1 = decideAfterFailure({ attempts: 2, maxAttempts: 4, permanent: false, now });
  assert.equal(d1.status, 'retrying');
  assert.ok(d1.nextAttemptAt instanceof Date);
  assert.ok(d1.nextAttemptAt.getTime() > now.getTime());

  // Case B: normal error reaching maxAttempts -> dead
  const d2 = decideAfterFailure({ attempts: 4, maxAttempts: 4, permanent: false, now });
  assert.equal(d2.status, 'dead');

  // Case C: permanent error on attempt 1 -> straight to dead
  const d3 = decideAfterFailure({ attempts: 1, maxAttempts: 4, permanent: true, now });
  assert.equal(d3.status, 'dead');
});

// Test 3: canClaim predicate states
test('3. canClaim: queued/retrying true, succeeded/dead false, fresh processing false, stale processing true', () => {
  const now = new Date(1700000000000);
  const visibilityMs = 30000;

  assert.equal(canClaim({ status: 'queued' }, now, visibilityMs), true);
  assert.equal(canClaim({ status: 'retrying' }, now, visibilityMs), true);
  assert.equal(canClaim({ status: 'succeeded' }, now, visibilityMs), false);
  assert.equal(canClaim({ status: 'dead' }, now, visibilityMs), false);

  // Fresh processing (started 10s ago < 30s) -> false
  assert.equal(
    canClaim(
      { status: 'processing', started_at: new Date(now.getTime() - 10000) },
      now,
      visibilityMs
    ),
    false
  );

  // Stale processing (started 35s ago >= 30s) -> true
  assert.equal(
    canClaim(
      { status: 'processing', started_at: new Date(now.getTime() - 35000) },
      now,
      visibilityMs
    ),
    true
  );
});

// Test 4: hashPayload ignores key order
test('4. hashPayload ignores key order', () => {
  const p1 = { b: 2, a: 1, nested: { y: 'test', x: 10 } };
  const p2 = { nested: { x: 10, y: 'test' }, a: 1, b: 2 };
  const p3 = { a: 1, b: 3 };

  assert.equal(hashPayload(p1), hashPayload(p2));
  assert.notEqual(hashPayload(p1), hashPayload(p3));
});

// Test 5: withTimeout rejects on slow promise
test('5. withTimeout rejects on slow promise', async () => {
  const slowPromise = new Promise((resolve) => setTimeout(() => resolve('done'), 150));
  await assert.rejects(
    async () => {
      await withTimeout(slowPromise, 30);
    },
    (err) => err.name === 'TimeoutError'
  );

  const fastPromise = new Promise((resolve) => setTimeout(() => resolve('ok'), 20));
  const res = await withTimeout(fastPromise, 100);
  assert.equal(res, 'ok');
});

// Test 6: processMessage (worker unit, with fakes): success path acks and marks succeeded
test('6. processMessage (worker unit, with fakes): success path acks and marks succeeded', async () => {
  const acks = [];
  const succeeded = [];
  const attempts = [];

  const fakeRepo = {
    async getEventById(id) {
      return { id, type: 'test.success', status: 'queued', max_attempts: 4, attempts: 0 };
    },
    async claimEvent(id) {
      return { id, type: 'test.success', status: 'processing', max_attempts: 4, attempts: 1 };
    },
    async recordAttempt(data) {
      attempts.push(data);
    },
    async markSucceeded(id, result, duration) {
      succeeded.push({ id, result, duration });
    },
  };

  const fakeTransport = {
    async ack(messageId) {
      acks.push(messageId);
    },
  };

  const fakeHandlers = {
    'test.success': async ({ event, attempt }) => ({ done: true, attempt }),
  };

  const outcome = await processMessage(
    { messageId: 'msg-1', eventId: 'ev-1' },
    {
      repo: fakeRepo,
      transport: fakeTransport,
      handlers: fakeHandlers,
    }
  );

  assert.equal(outcome.outcome, 'success');
  assert.deepEqual(acks, ['msg-1']);
  assert.equal(succeeded.length, 1);
  assert.equal(succeeded[0].id, 'ev-1');
  assert.equal(succeeded[0].result.done, true);
  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].outcome, 'success');
});

// Test 7: processMessage: failing handler schedules retry and acks; at max attempts it dead-letters
test('7. processMessage: failing handler schedules retry and acks; at max attempts it dead-letters', async () => {
  const acks = [];
  const retrying = [];
  const dead = [];
  const dlqPublishes = [];

  const fakeHandlers = {
    'test.fail': async () => {
      throw new Error('Downstream API outage');
    },
  };

  // Case A: attempts < maxAttempts -> retry scheduled
  const fakeRepoRetry = {
    async getEventById(id) {
      return { id, type: 'test.fail', status: 'queued', max_attempts: 3, attempts: 1 };
    },
    async claimEvent(id) {
      return { id, type: 'test.fail', status: 'processing', max_attempts: 3, attempts: 2 };
    },
    async recordAttempt() {},
    async markRetrying(id, nextAttemptAt, lastError) {
      retrying.push({ id, nextAttemptAt, lastError });
    },
    async markDead() {},
  };

  const fakeTransportA = {
    async ack(msgId) {
      acks.push(msgId);
    },
    async deadLetterPublish() {},
  };

  const outcomeA = await processMessage(
    { messageId: 'msg-retry', eventId: 'ev-retry' },
    {
      repo: fakeRepoRetry,
      transport: fakeTransportA,
      handlers: fakeHandlers,
    }
  );

  assert.equal(outcomeA.outcome, 'retrying');
  assert.ok(acks.includes('msg-retry'));
  assert.equal(retrying.length, 1);
  assert.equal(retrying[0].id, 'ev-retry');

  // Case B: attempts >= maxAttempts -> dead-letters
  const fakeRepoDead = {
    async getEventById(id) {
      return { id, type: 'test.fail', status: 'retrying', max_attempts: 3, attempts: 2 };
    },
    async claimEvent(id) {
      return { id, type: 'test.fail', status: 'processing', max_attempts: 3, attempts: 3 };
    },
    async recordAttempt() {},
    async markRetrying() {},
    async markDead(id, lastError) {
      dead.push({ id, lastError });
    },
  };

  const fakeTransportB = {
    async ack(msgId) {
      acks.push(msgId);
    },
    async deadLetterPublish(payload) {
      dlqPublishes.push(payload);
    },
  };

  const outcomeB = await processMessage(
    { messageId: 'msg-dead', eventId: 'ev-dead' },
    {
      repo: fakeRepoDead,
      transport: fakeTransportB,
      handlers: fakeHandlers,
    }
  );

  assert.equal(outcomeB.outcome, 'dead');
  assert.ok(acks.includes('msg-dead'));
  assert.equal(dead.length, 1);
  assert.equal(dead[0].id, 'ev-dead');
  assert.equal(dlqPublishes.length, 1);
  assert.equal(dlqPublishes[0].eventId, 'ev-dead');
});

// Test 8: processMessage: duplicate delivery (claim fails) acks without running the handler; simulated crash does NOT ack
test('8. processMessage: duplicate delivery acks without running handler; simulated crash does NOT ack', async () => {
  let handlerRan = false;
  const acks = [];
  const recordedAttempts = [];

  // Case A: claim returns null (duplicate delivery)
  const fakeRepoDup = {
    async getEventById(id) {
      return { id, type: 'test.dup', status: 'processing', max_attempts: 3, attempts: 1 };
    },
    async claimEvent() {
      return null; // claim failed
    },
  };

  const fakeTransportDup = {
    async ack(id) {
      acks.push(id);
    },
  };

  const fakeHandlersDup = {
    'test.dup': async () => {
      handlerRan = true;
    },
  };

  const dupOutcome = await processMessage(
    { messageId: 'msg-dup', eventId: 'ev-dup' },
    {
      repo: fakeRepoDup,
      transport: fakeTransportDup,
      handlers: fakeHandlersDup,
    }
  );

  assert.equal(dupOutcome.outcome, 'duplicate_skip');
  assert.equal(handlerRan, false, 'Handler must NOT run on duplicate delivery');
  assert.ok(acks.includes('msg-dup'), 'Duplicate delivery must be acked to clear stream');

  // Case B: simulated crash (CrashSimulated)
  const acksCrash = [];
  const fakeRepoCrash = {
    async getEventById(id) {
      return { id, type: 'test.crash', status: 'queued', max_attempts: 3, attempts: 0 };
    },
    async claimEvent(id) {
      return { id, type: 'test.crash', status: 'processing', max_attempts: 3, attempts: 1 };
    },
    async recordAttempt(data) {
      recordedAttempts.push(data);
    },
    async markSucceeded() {
      assert.fail('Should not mark succeeded on crash');
    },
    async markRetrying() {
      assert.fail('Should not mark retrying on crash');
    },
    async markDead() {
      assert.fail('Should not mark dead on crash');
    },
  };

  const fakeTransportCrash = {
    async ack(id) {
      acksCrash.push(id);
    },
    async deadLetterPublish() {},
  };

  const fakeHandlersCrash = {
    'test.crash': async () => {
      throw new CrashSimulated('Process SIGKILL');
    },
  };

  const crashOutcome = await processMessage(
    { messageId: 'msg-crash', eventId: 'ev-crash' },
    {
      repo: fakeRepoCrash,
      transport: fakeTransportCrash,
      handlers: fakeHandlersCrash,
    }
  );

  assert.equal(crashOutcome.outcome, 'crashed');
  assert.equal(acksCrash.length, 0, 'Crashed message must NOT be acked');
  assert.equal(recordedAttempts.length, 1);
  assert.equal(recordedAttempts[0].outcome, 'crashed');
});
