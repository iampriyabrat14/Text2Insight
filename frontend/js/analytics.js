/**
 * analytics.js — Query Analytics Dashboard
 * Fetches all /eval/* endpoints in parallel and renders KPI cards + charts + tables.
 */
import { get } from './api.js';

// ── DOM refs ──────────────────────────────────────────────────────────────
const periodEl   = document.getElementById('period-select');
const refreshBtn = document.getElementById('refresh-btn');

// Chart.js instances — destroyed before re-render
const _charts = {};

// ── Helpers ───────────────────────────────────────────────────────────────

const _days = () => parseInt(periodEl.value, 10);

function _esc(s) {
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function _fetch(path) {
  const res = await get(path);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}

// ── Chart defaults ────────────────────────────────────────────────────────

const _PALETTE = [
  'rgba(37,99,235,.8)',    // blue
  'rgba(16,185,129,.8)',   // green
  'rgba(245,158,11,.8)',   // amber
  'rgba(99,102,241,.8)',   // indigo
  'rgba(236,72,153,.8)',   // pink
  'rgba(6,182,212,.8)',    // cyan
  'rgba(168,85,247,.8)',   // purple
];

const _GRID  = 'rgba(148,163,184,.1)';
const _TICKS = '#8b949e';

const _SCALE_OPTS = {
  x: { ticks: { color: _TICKS, font: { size: 11 } }, grid: { color: _GRID } },
  y: { ticks: { color: _TICKS, font: { size: 11 } }, grid: { color: _GRID }, beginAtZero: true },
};

function _chart(id, type, data, opts = {}) {
  if (_charts[id]) _charts[id].destroy();
  const ctx = document.getElementById(id).getContext('2d');
  _charts[id] = new Chart(ctx, {
    type,
    data,
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          bodyFont:  { family: 'Inter, sans-serif', size: 12 },
          titleFont: { family: 'Inter, sans-serif', size: 12 },
        },
      },
      scales: _SCALE_OPTS,
      ...opts,
    },
  });
}

// ── KPI cards ──────────────────────────────────────────────────────────────

function _kpi(id, label, value, sub, color) {
  const el = document.getElementById(id);
  el.classList.remove('skeleton');
  el.innerHTML = `
    <div class="kpi-label">${label}</div>
    <div class="kpi-value" style="color:${color}">${value}</div>
    ${sub ? `<div class="kpi-sub">${sub}</div>` : ''}
  `;
}

async function loadSummary(days) {
  const d = await _fetch(`/eval/summary?days=${days}`);
  _kpi('kpi-total',   'Total Queries',   d.total_queries.toLocaleString(),
       `last ${days} days`,              '#2563eb');
  _kpi('kpi-latency', 'Avg Latency',     `${d.avg_latency_ms} ms`,
       'end-to-end pipeline',            '#7c3aed');
  _kpi('kpi-cache',   'Cache Hit Rate',  `${d.cache_hit_rate}%`,
       `${Math.round(d.total_queries * d.cache_hit_rate / 100)} hits`, '#10b981');
  _kpi('kpi-valid',   'SQL Validity',    `${d.sql_valid_rate}%`,
       'of generated queries',           '#f59e0b');
  _kpi('kpi-tokens',  'Tokens Used',     d.total_tokens.toLocaleString(),
       'all users combined',             '#ec4899');
  _kpi('kpi-llm',     'LLM Latency',     `${d.avg_llm_latency_ms} ms`,
       'avg generation time',            '#06b6d4');
}

// ── Daily volume ──────────────────────────────────────────────────────────

async function loadDailyVolume(days) {
  const data = await _fetch(`/eval/daily-volume?days=${days}`);
  const sub = document.getElementById('vol-sub');
  if (sub) sub.textContent = `${data.length} active day${data.length !== 1 ? 's' : ''}`;

  _chart('chart-volume', 'line', {
    labels: data.map(d => d.day),
    datasets: [{
      label: 'Queries',
      data:  data.map(d => d.count),
      borderColor:     'rgba(37,99,235,.9)',
      backgroundColor: 'rgba(37,99,235,.08)',
      borderWidth: 2,
      fill: true,
      tension: 0.35,
      pointRadius:      data.length > 20 ? 0 : 3,
      pointHoverRadius: 5,
    }],
  }, {
    scales: {
      x: {
        ticks: { color: _TICKS, font: { size: 10 }, maxTicksLimit: 10 },
        grid:  { color: _GRID },
      },
      y: {
        ticks:       { color: _TICKS, font: { size: 11 } },
        grid:        { color: _GRID },
        beginAtZero: true,
      },
    },
  });
}

// ── Provider donut ────────────────────────────────────────────────────────

async function loadProviderBreakdown(days) {
  const data = await _fetch(`/eval/provider-breakdown?days=${days}`);
  _chart('chart-provider', 'doughnut', {
    labels: data.map(d => d.provider),
    datasets: [{
      data:            data.map(d => d.count),
      backgroundColor: _PALETTE.slice(0, data.length),
      borderWidth:     0,
      hoverOffset:     6,
    }],
  }, {
    cutout: '62%',
    plugins: {
      legend: {
        display: true,
        position: 'bottom',
        labels: {
          color: _TICKS, font: { size: 12 }, padding: 16, boxWidth: 12,
        },
      },
    },
    scales: {},   // no axes for doughnut
  });
}

// ── Latency percentiles ───────────────────────────────────────────────────

async function loadLatencyPercentiles(days) {
  const d = await _fetch(`/eval/latency-percentiles?days=${days}`);
  _chart('chart-latency', 'bar', {
    labels: ['p50', 'p75', 'p95', 'p99'],
    datasets: [{
      label: 'ms',
      data:  [d.p50, d.p75, d.p95, d.p99],
      backgroundColor: [
        'rgba(16,185,129,.8)',
        'rgba(6,182,212,.8)',
        'rgba(245,158,11,.8)',
        'rgba(239,68,68,.8)',
      ],
      borderRadius: 6,
      borderWidth:  0,
    }],
  }, {
    scales: {
      x: {
        ticks: { color: _TICKS, font: { size: 12, weight: '600' } },
        grid:  { display: false },
      },
      y: {
        ticks:       { color: _TICKS, font: { size: 11 } },
        grid:        { color: _GRID },
        beginAtZero: true,
      },
    },
  });
}

// ── Hourly distribution ───────────────────────────────────────────────────

async function loadHourly(days) {
  const data = await _fetch(`/eval/hourly-distribution?days=${days}`);
  _chart('chart-hourly', 'bar', {
    labels: data.map(d => `${String(d.hour).padStart(2, '0')}h`),
    datasets: [{
      label: 'Queries',
      data:  data.map(d => d.count),
      backgroundColor: 'rgba(99,102,241,.75)',
      borderRadius:    4,
      borderWidth:     0,
    }],
  }, {
    scales: {
      x: {
        ticks: { color: _TICKS, font: { size: 9 }, maxRotation: 0 },
        grid:  { display: false },
      },
      y: {
        ticks:       { color: _TICKS, font: { size: 11 } },
        grid:        { color: _GRID },
        beginAtZero: true,
      },
    },
  });
}

// ── Top queries table ─────────────────────────────────────────────────────

async function loadTopQueries(days) {
  const data  = await _fetch(`/eval/top-queries?days=${days}&limit=10`);
  const tbody = document.querySelector('#tbl-queries tbody');
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="3" class="tbl-empty">No queries yet</td></tr>';
    return;
  }
  tbody.innerHTML = data.map((r, i) => `
    <tr>
      <td class="tbl-rank">${i + 1}</td>
      <td class="tbl-query" title="${_esc(r.query)}">${_esc(r.query)}</td>
      <td class="tbl-count"><span class="count-pill">${r.count}</span></td>
    </tr>
  `).join('');
}

// ── User activity table ───────────────────────────────────────────────────

async function loadUserStats(days) {
  const data  = await _fetch(`/eval/user-stats?days=${days}&limit=10`);
  const tbody = document.querySelector('#tbl-users tbody');
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="tbl-empty">No data yet</td></tr>';
    return;
  }
  tbody.innerHTML = data.map(r => `
    <tr>
      <td>
        <div class="tbl-user-cell">
          <span class="user-dot"></span>${_esc(r.username)}
        </div>
      </td>
      <td><span class="tier-badge tier-${_esc(r.tier)}">${_esc(r.tier)}</span></td>
      <td>${r.query_count}</td>
      <td>${r.tokens_used.toLocaleString()}</td>
      <td>${r.avg_latency_ms} ms</td>
    </tr>
  `).join('');
}

// ── Auth guard — redirect if not admin ───────────────────────────────────

async function loadMe() {
  const res = await get('/auth/me');
  if (!res.ok) { window.location.href = '/'; return false; }
  const me = await res.json();
  if (me.tier !== 'admin') { window.location.href = '/chat'; return false; }
  document.getElementById('dash-username').textContent = me.username;
  document.getElementById('dash-tier').textContent     = me.tier;
  document.getElementById('dash-avatar').textContent   = me.username[0].toUpperCase();
  return true;
}

// ── Main load ─────────────────────────────────────────────────────────────

async function loadAll() {
  refreshBtn.disabled = true;
  refreshBtn.classList.add('loading');
  const days = _days();

  // Re-add skeleton to KPI cards while refreshing
  ['kpi-total','kpi-latency','kpi-cache','kpi-valid','kpi-tokens','kpi-llm'].forEach(id => {
    document.getElementById(id).className = 'kpi-card skeleton';
    document.getElementById(id).innerHTML = '';
  });

  try {
    await Promise.all([
      loadSummary(days),
      loadDailyVolume(days),
      loadProviderBreakdown(days),
      loadLatencyPercentiles(days),
      loadHourly(days),
      loadTopQueries(days),
      loadUserStats(days),
    ]);
  } catch (err) {
    console.error('Analytics load error:', err);
  } finally {
    refreshBtn.disabled = false;
    refreshBtn.classList.remove('loading');
  }
}

// ── Event listeners ───────────────────────────────────────────────────────

periodEl.addEventListener('change', loadAll);
refreshBtn.addEventListener('click', loadAll);

// ── Init ──────────────────────────────────────────────────────────────────

loadMe().then(ok => { if (ok) loadAll(); });
