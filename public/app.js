/**
 * EventFlow Dashboard Application
 * Vanilla JS, no bundlers, zero dependencies.
 * Safe DOM manipulation: no innerHTML with server/event data.
 */

(function () {
  'use strict';

  // State
  let currentTab = 'tab-events';
  let activeStatusFilter = '';
  let activeTypeFilter = '';
  let refreshTimer = null;
  let isFirstLoad = true;

  // DOM Elements
  const themeToggleBtn = document.getElementById('theme-toggle-btn');
  const themeIcon = document.getElementById('theme-icon');
  const refreshBtn = document.getElementById('refresh-btn');
  const apiKeyInput = document.getElementById('api-key-input');
  const healthPill = document.getElementById('health-pill');
  const healthText = document.getElementById('health-text');
  const warmupBanner = document.getElementById('warmup-banner');
  const notificationBar = document.getElementById('notification-bar');

  // KPI elements
  const kpiDepth = document.getElementById('kpi-depth');
  const kpiInflight = document.getElementById('kpi-inflight');
  const kpiSucceeded = document.getElementById('kpi-succeeded');
  const kpiRetrying = document.getElementById('kpi-retrying');
  const kpiDead = document.getElementById('kpi-dead');
  const kpiThroughput = document.getElementById('kpi-throughput');
  const kpiP95 = document.getElementById('kpi-p95');

  // Chart elements
  const throughputSvg = document.getElementById('throughput-svg');
  const chartSummaryText = document.getElementById('chart-summary-text');
  const chartSrSummary = document.getElementById('chart-sr-summary');

  // Tables
  const eventsTbody = document.getElementById('events-tbody');
  const dlqTbody = document.getElementById('dlq-tbody');
  const statusChips = document.getElementById('status-chips');
  const typeFilterInput = document.getElementById('type-filter');
  const replayAllBtn = document.getElementById('replay-all-btn');

  // Modal
  const eventModal = document.getElementById('event-modal');
  const modalCloseBtn = document.getElementById('modal-close-btn');
  const modalBody = document.getElementById('modal-body');

  /**
   * Safe text escape helper
   */
  function escapeText(str) {
    if (str === null || str === undefined) return '';
    return String(str);
  }

  /**
   * Show notification toast
   */
  function showToast(message, type = 'info') {
    notificationBar.textContent = '';
    const toast = document.createElement('div');
    toast.className = `banner`;
    toast.style.borderColor = type === 'error' ? 'var(--danger)' : 'var(--accent)';
    toast.textContent = message;
    notificationBar.appendChild(toast);
    setTimeout(() => {
      if (toast.parentNode) {
        toast.parentNode.removeChild(toast);
      }
    }, 4000);
  }

  /**
   * Theme Management
   */
  function initTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
    themeIcon.textContent = currentTheme === 'dark' ? '☀️' : '🌙';

    themeToggleBtn.addEventListener('click', () => {
      const active = document.documentElement.getAttribute('data-theme') || 'dark';
      const next = active === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('eventflow_theme', next);
      themeIcon.textContent = next === 'dark' ? '☀️' : '🌙';
    });
  }

  /**
   * API Key Management (sessionStorage only)
   */
  function initApiKey() {
    const savedKey = sessionStorage.getItem('eventflow_api_key');
    if (savedKey) {
      apiKeyInput.value = savedKey;
    }

    apiKeyInput.addEventListener('input', (e) => {
      const val = e.target.value.trim();
      if (val) {
        sessionStorage.setItem('eventflow_api_key', val);
      } else {
        sessionStorage.removeItem('eventflow_api_key');
      }
    });
  }

  function getHeaders() {
    const headers = { 'Content-Type': 'application/json' };
    const key = sessionStorage.getItem('eventflow_api_key') || apiKeyInput.value.trim();
    if (key) {
      headers['x-api-key'] = key;
    }
    return headers;
  }

  /**
   * Safe Fetch with Warm-up detection for free tier cold starts
   */
  async function safeFetch(url, options = {}) {
    let timer = null;
    if (isFirstLoad) {
      timer = setTimeout(() => {
        warmupBanner.classList.remove('hidden');
      }, 3000);
    }

    try {
      const res = await fetch(url, options);
      if (timer) clearTimeout(timer);
      warmupBanner.classList.add('hidden');
      isFirstLoad = false;
      return res;
    } catch (err) {
      if (timer) clearTimeout(timer);
      warmupBanner.classList.add('hidden');
      throw err;
    }
  }

  /**
   * Fetch and update Health indicator
   */
  async function updateHealth() {
    try {
      const res = await safeFetch('/health');
      const data = await res.json();
      healthPill.className = 'pill';

      if (res.ok && data.status === 'ok') {
        healthPill.classList.add('healthy');
        healthText.textContent = 'Postgres & Redis Online';
      } else {
        healthPill.classList.add('degraded');
        healthText.textContent = `Degraded (PG: ${data.postgres}, Redis: ${data.redis})`;
      }
    } catch (err) {
      healthPill.className = 'pill down';
      healthText.textContent = 'Backend Offline';
    }
  }

  /**
   * Render inline SVG Throughput chart
   */
  function renderThroughputChart(series) {
    while (throughputSvg.firstChild) {
      throughputSvg.removeChild(throughputSvg.firstChild);
    }

    if (!series || series.length === 0) {
      chartSummaryText.textContent = 'No processed events in last 30 minutes';
      chartSrSummary.textContent = 'No events processed in the last 30 minutes.';
      return;
    }

    const totalEvents = series.reduce((acc, pt) => acc + (pt.count || 0), 0);
    chartSummaryText.textContent = `${totalEvents} events processed in last 30m`;
    chartSrSummary.textContent = `Total of ${totalEvents} events processed in the last 30 minutes across ${series.length} active time intervals.`;

    const maxCount = Math.max(...series.map((s) => s.count), 1);
    const svgWidth = 800;
    const svgHeight = 120;
    const barWidth = Math.max(8, Math.floor((svgWidth - series.length * 4) / series.length));

    series.forEach((pt, idx) => {
      const height = Math.max(4, Math.round((pt.count / maxCount) * (svgHeight - 20)));
      const x = idx * (barWidth + 4) + 10;
      const y = svgHeight - height;

      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('x', x);
      rect.setAttribute('y', y);
      rect.setAttribute('width', barWidth);
      rect.setAttribute('height', height);
      rect.setAttribute('rx', '3');
      rect.setAttribute('class', 'chart-bar');

      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      title.textContent = `${pt.minute}: ${pt.count} events`;
      rect.appendChild(title);

      throughputSvg.appendChild(rect);
    });
  }

  /**
   * Fetch and update KPIs & Metrics
   */
  async function updateMetrics() {
    try {
      const res = await safeFetch('/metrics', { headers: getHeaders() });
      if (!res.ok) return;
      const data = await res.json();

      kpiDepth.textContent = escapeText(data.streamDepth ?? 0);
      kpiInflight.textContent = escapeText(data.pendingCount ?? 0);
      kpiSucceeded.textContent = escapeText(data.statusCounts?.succeeded ?? 0);
      kpiRetrying.textContent = escapeText(data.statusCounts?.retrying ?? 0);
      kpiDead.textContent = escapeText(data.statusCounts?.dead ?? 0);
      kpiThroughput.textContent = escapeText(data.processed?.last1m ?? 0);
      kpiP95.textContent = `${escapeText(data.latency?.p95 ?? 0)}ms`;

      renderThroughputChart(data.throughputSeries || []);
    } catch (err) {
      console.warn('[Dashboard Metrics Error]', err.message);
    }
  }

  /**
   * Build a status badge element
   */
  function createStatusBadge(status) {
    const badge = document.createElement('span');
    badge.className = `badge badge-${escapeText(status)}`;
    badge.textContent = escapeText(status);
    return badge;
  }

  /**
   * Fetch and render Events List
   */
  async function updateEvents() {
    try {
      let url = '/events?limit=50';
      if (activeStatusFilter) url += `&status=${encodeURIComponent(activeStatusFilter)}`;
      if (activeTypeFilter) url += `&type=${encodeURIComponent(activeTypeFilter)}`;

      const res = await safeFetch(url, { headers: getHeaders() });
      if (!res.ok) return;
      const data = await res.json();

      eventsTbody.textContent = '';

      if (!data.events || data.events.length === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 6;
        td.style.textAlign = 'center';
        td.style.color = 'var(--text-muted)';
        td.style.padding = '24px';
        td.textContent = 'No events match the current filter.';
        tr.appendChild(td);
        eventsTbody.appendChild(tr);
        return;
      }

      data.events.forEach((ev) => {
        const tr = document.createElement('tr');
        tr.addEventListener('click', () => openEventDetails(ev.id));

        // ID
        const tdId = document.createElement('td');
        tdId.className = 'mono';
        tdId.setAttribute('data-label', 'Event ID');
        tdId.textContent = ev.id ? ev.id.slice(0, 8) + '...' : '-';
        tr.appendChild(tdId);

        // Type
        const tdType = document.createElement('td');
        tdType.setAttribute('data-label', 'Type');
        tdType.textContent = escapeText(ev.type);
        tr.appendChild(tdType);

        // Status
        const tdStatus = document.createElement('td');
        tdStatus.setAttribute('data-label', 'Status');
        tdStatus.appendChild(createStatusBadge(ev.status));
        tr.appendChild(tdStatus);

        // Attempts
        const tdAttempts = document.createElement('td');
        tdAttempts.className = 'mono';
        tdAttempts.setAttribute('data-label', 'Attempts');
        tdAttempts.textContent = `${ev.attempts} / ${ev.max_attempts}`;
        tr.appendChild(tdAttempts);

        // Duration
        const tdDuration = document.createElement('td');
        tdDuration.className = 'mono';
        tdDuration.setAttribute('data-label', 'Duration');
        tdDuration.textContent = ev.processing_ms !== null ? `${ev.processing_ms}ms` : '-';
        tr.appendChild(tdDuration);

        // Created At
        const tdCreated = document.createElement('td');
        tdCreated.setAttribute('data-label', 'Created');
        tdCreated.textContent = ev.created_at ? new Date(ev.created_at).toLocaleTimeString() : '-';
        tr.appendChild(tdCreated);

        eventsTbody.appendChild(tr);
      });
    } catch (err) {
      console.warn('[Dashboard Events Error]', err.message);
    }
  }

  /**
   * Fetch and render DLQ List
   */
  async function updateDlq() {
    try {
      const res = await safeFetch('/dlq?limit=50', { headers: getHeaders() });
      if (!res.ok) return;
      const data = await res.json();

      dlqTbody.textContent = '';

      if (!data.deadEvents || data.deadEvents.length === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 6;
        td.style.textAlign = 'center';
        td.style.color = 'var(--text-muted)';
        td.style.padding = '24px';
        td.textContent = 'Dead-letter queue is empty.';
        tr.appendChild(td);
        dlqTbody.appendChild(tr);
        return;
      }

      data.deadEvents.forEach((ev) => {
        const tr = document.createElement('tr');

        const tdId = document.createElement('td');
        tdId.className = 'mono';
        tdId.setAttribute('data-label', 'Event ID');
        tdId.textContent = ev.id ? ev.id.slice(0, 8) + '...' : '-';
        tr.appendChild(tdId);

        const tdType = document.createElement('td');
        tdType.setAttribute('data-label', 'Type');
        tdType.textContent = escapeText(ev.type);
        tr.appendChild(tdType);

        const tdAttempts = document.createElement('td');
        tdAttempts.className = 'mono';
        tdAttempts.setAttribute('data-label', 'Attempts');
        tdAttempts.textContent = `${ev.attempts} / ${ev.max_attempts}`;
        tr.appendChild(tdAttempts);

        const tdError = document.createElement('td');
        tdError.setAttribute('data-label', 'Last Error');
        tdError.style.color = 'var(--danger)';
        tdError.style.fontSize = '12px';
        tdError.textContent = escapeText(ev.last_error || 'Unknown error');
        tr.appendChild(tdError);

        const tdTime = document.createElement('td');
        tdTime.setAttribute('data-label', 'Finished At');
        tdTime.textContent = ev.finished_at ? new Date(ev.finished_at).toLocaleTimeString() : '-';
        tr.appendChild(tdTime);

        // Actions: Replay & Discard
        const tdActions = document.createElement('td');
        tdActions.setAttribute('data-label', 'Actions');
        tdActions.style.display = 'flex';
        tdActions.style.gap = '6px';

        const replayBtn = document.createElement('button');
        replayBtn.className = 'sm primary';
        replayBtn.textContent = 'Replay';
        replayBtn.addEventListener('click', async (e) => {
          e.stopPropagation();
          await replayEvent(ev.id);
        });
        tdActions.appendChild(replayBtn);

        const discardBtn = document.createElement('button');
        discardBtn.className = 'sm';
        discardBtn.textContent = 'Discard';
        discardBtn.title = 'Requires API Key';
        discardBtn.addEventListener('click', async (e) => {
          e.stopPropagation();
          await discardEvent(ev.id);
        });
        tdActions.appendChild(discardBtn);

        tr.appendChild(tdActions);
        dlqTbody.appendChild(tr);
      });
    } catch (err) {
      console.warn('[Dashboard DLQ Error]', err.message);
    }
  }

  /**
   * Replay single dead event
   */
  async function replayEvent(id) {
    try {
      const res = await safeFetch(`/dlq/${id}/replay`, {
        method: 'POST',
        headers: getHeaders(),
      });
      if (res.ok) {
        showToast(`Event ${id.slice(0, 8)} rescheduled for execution.`, 'info');
        refreshAll();
      } else {
        const err = await res.json();
        showToast(`Replay failed: ${err.message}`, 'error');
      }
    } catch (err) {
      showToast(`Replay error: ${err.message}`, 'error');
    }
  }

  /**
   * Discard single dead event
   */
  async function discardEvent(id) {
    try {
      const res = await safeFetch(`/dlq/${id}/discard`, {
        method: 'POST',
        headers: getHeaders(),
      });
      if (res.ok) {
        showToast(`Event ${id.slice(0, 8)} discarded.`, 'info');
        refreshAll();
      } else {
        const err = await res.json();
        showToast(`Discard failed: ${err.message} (Enter API Key in header)`, 'error');
      }
    } catch (err) {
      showToast(`Discard error: ${err.message}`, 'error');
    }
  }

  /**
   * Replay all dead events
   */
  async function replayAll() {
    try {
      const res = await safeFetch('/dlq/replay-all', {
        method: 'POST',
        headers: getHeaders(),
      });
      if (res.ok) {
        const data = await res.json();
        showToast(`Replayed ${data.replayedCount} dead events.`, 'info');
        refreshAll();
      } else {
        const err = await res.json();
        showToast(`Replay All failed: ${err.message} (Requires API Key)`, 'error');
      }
    } catch (err) {
      showToast(`Replay All error: ${err.message}`, 'error');
    }
  }

  /**
   * Open Event Details Modal
   */
  async function openEventDetails(id) {
    modalBody.textContent = 'Loading execution timeline...';
    eventModal.classList.add('open');

    try {
      const res = await safeFetch(`/events/${id}`, { headers: getHeaders() });
      if (!res.ok) {
        modalBody.textContent = `Failed to load details for event ${id}.`;
        return;
      }
      const data = await res.json();
      const { event, attempts } = data;

      modalBody.textContent = '';

      // Meta info section
      const metaDiv = document.createElement('div');
      metaDiv.style.display = 'grid';
      metaDiv.style.gridTemplateColumns = 'repeat(auto-fit, minmax(200px, 1fr))';
      metaDiv.style.gap = '10px';
      metaDiv.style.padding = '12px';
      metaDiv.style.backgroundColor = 'var(--surface-alt)';
      metaDiv.style.borderRadius = 'var(--radius-sm)';

      const makeField = (lbl, val) => {
        const d = document.createElement('div');
        const l = document.createElement('div');
        l.style.fontSize = '11px';
        l.style.color = 'var(--text-muted)';
        l.textContent = lbl;
        const v = document.createElement('div');
        v.className = 'mono';
        v.style.fontSize = '12px';
        v.textContent = escapeText(val);
        d.appendChild(l);
        d.appendChild(v);
        return d;
      };

      metaDiv.appendChild(makeField('Event ID', event.id));
      metaDiv.appendChild(makeField('Type', event.type));
      metaDiv.appendChild(makeField('Status', event.status));
      metaDiv.appendChild(makeField('Attempts', `${event.attempts} / ${event.max_attempts}`));
      metaDiv.appendChild(makeField('Created', event.created_at ? new Date(event.created_at).toLocaleString() : '-'));
      metaDiv.appendChild(makeField('Finished', event.finished_at ? new Date(event.finished_at).toLocaleString() : '-'));

      modalBody.appendChild(metaDiv);

      // Attempts Timeline Table
      const timelineHeader = document.createElement('h4');
      timelineHeader.textContent = 'Execution Attempts Timeline';
      timelineHeader.style.marginTop = '12px';
      modalBody.appendChild(timelineHeader);

      if (!attempts || attempts.length === 0) {
        const noAttempts = document.createElement('p');
        noAttempts.style.color = 'var(--text-muted)';
        noAttempts.style.fontSize = '12px';
        noAttempts.textContent = 'No attempt records found.';
        modalBody.appendChild(noAttempts);
      } else {
        const table = document.createElement('table');
        const thead = document.createElement('thead');
        const headRow = document.createElement('tr');
        ['#', 'Outcome', 'Duration', 'Error', 'Finished At'].forEach((h) => {
          const th = document.createElement('th');
          th.textContent = h;
          headRow.appendChild(th);
        });
        thead.appendChild(headRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        attempts.forEach((att) => {
          const r = document.createElement('tr');
          const tdNum = document.createElement('td');
          tdNum.className = 'mono';
          tdNum.textContent = String(att.attempt);
          r.appendChild(tdNum);

          const tdOut = document.createElement('td');
          const badge = document.createElement('span');
          badge.className = `badge badge-${att.outcome === 'success' ? 'succeeded' : 'dead'}`;
          badge.textContent = att.outcome;
          tdOut.appendChild(badge);
          r.appendChild(tdOut);

          const tdDur = document.createElement('td');
          tdDur.className = 'mono';
          tdDur.textContent = `${att.duration_ms}ms`;
          r.appendChild(tdDur);

          const tdErr = document.createElement('td');
          tdErr.style.color = att.error ? 'var(--danger)' : 'var(--text-muted)';
          tdErr.style.fontSize = '11px';
          tdErr.textContent = att.error || 'None';
          r.appendChild(tdErr);

          const tdFin = document.createElement('td');
          tdFin.textContent = att.finished_at ? new Date(att.finished_at).toLocaleTimeString() : '-';
          r.appendChild(tdFin);

          tbody.appendChild(r);
        });
        table.appendChild(tbody);
        modalBody.appendChild(table);
      }

      // Result / Payload JSON Views
      if (event.result) {
        const resHeader = document.createElement('h4');
        resHeader.textContent = 'Execution Result JSON';
        resHeader.style.marginTop = '12px';
        modalBody.appendChild(resHeader);

        const pre = document.createElement('pre');
        pre.className = 'json-view';
        pre.textContent = JSON.stringify(event.result, null, 2);
        modalBody.appendChild(pre);
      }

      if (event.payload) {
        const payloadHeader = document.createElement('h4');
        payloadHeader.textContent = 'Event Payload JSON';
        payloadHeader.style.marginTop = '12px';
        modalBody.appendChild(payloadHeader);

        const pre = document.createElement('pre');
        pre.className = 'json-view';
        pre.textContent = JSON.stringify(event.payload, null, 2);
        modalBody.appendChild(pre);
      }
    } catch (err) {
      modalBody.textContent = `Error loading event details: ${err.message}`;
    }
  }

  function closeModal() {
    eventModal.classList.remove('open');
  }

  /**
   * Emit Demo Scenario handler
   */
  async function emitScenario(scenario) {
    try {
      const res = await safeFetch('/demo/emit', {
        method: 'POST',
        headers: getHeaders(),
        body: JSON.stringify({ scenario }),
      });

      if (res.ok) {
        const data = await res.json();
        showToast(`Dispatched demo scenario: ${scenario} (${data.count} event(s))`, 'info');
        refreshAll();
      } else {
        const err = await res.json();
        showToast(`Emission rejected: ${err.message}`, 'error');
      }
    } catch (err) {
      showToast(`Emission error: ${err.message}`, 'error');
    }
  }

  /**
   * Global refresh
   */
  async function refreshAll() {
    await Promise.allSettled([updateHealth(), updateMetrics(), updateEvents(), updateDlq()]);
  }

  /**
   * Periodic refresh tied to Page Visibility API
   */
  function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(() => {
      if (document.visibilityState === 'visible') {
        refreshAll();
      }
    }, 10000);
  }

  /**
   * Initialize Event Listeners
   */
  function initListeners() {
    initTheme();
    initApiKey();

    refreshBtn.addEventListener('click', () => refreshAll());

    // Status filter chips
    statusChips.addEventListener('click', (e) => {
      if (e.target.classList.contains('chip')) {
        document.querySelectorAll('#status-chips .chip').forEach((c) => c.classList.remove('active'));
        e.target.classList.add('active');
        activeStatusFilter = e.target.getAttribute('data-status') || '';
        updateEvents();
      }
    });

    // Type filter
    let typeDebounce = null;
    typeFilterInput.addEventListener('input', (e) => {
      clearTimeout(typeDebounce);
      typeDebounce = setTimeout(() => {
        activeTypeFilter = e.target.value.trim();
        updateEvents();
      }, 300);
    });

    // Tab buttons
    document.querySelectorAll('.tab-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const targetTab = btn.getAttribute('data-tab');
        document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach((tc) => tc.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(targetTab).classList.add('active');
        currentTab = targetTab;
      });
    });

    // Demo buttons
    document.querySelectorAll('[data-scenario]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const scenario = btn.getAttribute('data-scenario');
        emitScenario(scenario);
      });
    });

    // Replay all
    replayAllBtn.addEventListener('click', () => replayAll());

    // Modal close
    modalCloseBtn.addEventListener('click', closeModal);
    eventModal.addEventListener('click', (e) => {
      if (e.target === eventModal) closeModal();
    });
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeModal();
    });

    // Page visibility listener
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        refreshAll();
      }
    });
  }

  // Bootstrap
  initListeners();
  refreshAll();
  startAutoRefresh();
})();
