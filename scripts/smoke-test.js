/**
 * EventFlow Smoke Test (Phase 4)
 * Exactly ONE smoke test:
 * 1. Bootstraps server (ROLE=all, DEMO_MODE=true)
 * 2. POST /demo/emit { scenario: 'flaky' }
 * 3. Polls GET /events until status reaches 'succeeded' with attempts > 1 (max 8 polls / 30s)
 * 4. Calls GET /health and GET /metrics once
 * 5. Shuts down cleanly and outputs compact summary
 */
import { startServer } from '../src/server.js';
import { closePool } from '../src/db.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runSmokeTest() {
  process.stdout.write('[Smoke Test] Starting EventFlow server...\n');
  const server = await startServer();
  const baseUrl = 'http://127.0.0.1:10000';

  try {
    // 1. POST /demo/emit { scenario: 'flaky' }
    process.stdout.write('[Smoke Test] Emitting flaky scenario...\n');
    const emitRes = await fetch(`${baseUrl}/demo/emit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scenario: 'flaky' }),
    });

    if (!emitRes.ok) {
      const errText = await emitRes.text();
      throw new Error(`POST /demo/emit failed: ${emitRes.status} ${errText}`);
    }

    const emitData = await emitRes.json();
    const targetEventId = emitData.events[0]?.id;
    if (!targetEventId) {
      throw new Error('No event ID returned from /demo/emit');
    }
    process.stdout.write(`[Smoke Test] Target Event ID: ${targetEventId}\n`);

    // 2. Poll GET /events/:id or GET /events (max 8 polls, up to 30s)
    let finalEvent = null;
    let pollCount = 0;
    const maxPolls = 8;

    while (pollCount < maxPolls) {
      pollCount++;
      await sleep(2500);

      const eventRes = await fetch(`${baseUrl}/events/${targetEventId}`);
      if (eventRes.ok) {
        const data = await eventRes.json();
        const ev = data.event;
        process.stdout.write(
          `[Smoke Test] Poll #${pollCount}: status='${ev.status}', attempts=${ev.attempts}, last_error=${ev.last_error ? `'${ev.last_error.slice(0, 40)}...'` : 'none'}\n`
        );

        if (ev.status === 'succeeded' && ev.attempts > 1) {
          finalEvent = ev;
          break;
        }
      }
    }

    if (!finalEvent) {
      throw new Error(`Smoke test timed out waiting for event ${targetEventId} to reach 'succeeded' with attempts > 1`);
    }

    // 3. GET /health
    const healthRes = await fetch(`${baseUrl}/health`);
    const healthData = await healthRes.json();

    // 4. GET /metrics
    const metricsRes = await fetch(`${baseUrl}/metrics`);
    const metricsData = await metricsRes.json();

    // Compact output display
    process.stdout.write('\n=== SMOKE TEST COMPACT SUMMARY ===\n');
    process.stdout.write(`Target Event ID: ${finalEvent.id}\n`);
    process.stdout.write(`Event Status:    ${finalEvent.status}\n`);
    process.stdout.write(`Total Attempts:  ${finalEvent.attempts}\n`);
    process.stdout.write(`Processing Time: ${finalEvent.processing_ms}ms\n`);
    process.stdout.write(`Health Check:    ${JSON.stringify(healthData)}\n`);
    process.stdout.write(
      `Metrics Counts:  ${JSON.stringify({
        statusCounts: metricsData.statusCounts,
        streamDepth: metricsData.streamDepth,
        pendingCount: metricsData.pendingCount,
        processed1m: metricsData.processed.last1m,
        p50LatencyMs: metricsData.latency.p50,
      })}\n`
    );
    process.stdout.write('==================================\n\n');
  } finally {
    process.stdout.write('[Smoke Test] Cleaning up and shutting down...\n');
    if (server.sweeper) server.sweeper.stop();
    if (server.worker) await server.worker.stop();
    await server.app.close();
    await server.transport.close();
    await closePool();
    process.stdout.write('[Smoke Test] Teardown complete.\n');
  }
}

runSmokeTest()
  .then(() => {
    process.exit(0);
  })
  .catch((err) => {
    process.stderr.write(`[Smoke Test Failed] ${err.message}\n`);
    process.exit(1);
  });
