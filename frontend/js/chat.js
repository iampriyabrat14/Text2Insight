/**
 * chat.js — Main chat logic: session management, message rendering, query dispatch.
 */
import { post, get, del, postStream } from './api.js';
import { buildExportPanel } from './export.js';
import { showToast } from './toast.js';
import { fetchAndUpdateQuota } from './quota.js';

// ── State ─────────────────────────────────────────────────────────────────
let currentSessionId = null;
let isLoading = false;
// Track last response that had real query data (rows + SQL)
let _lastDataMsg = null;  // { message_id, session_id, rows, columns }

// CSV upload state
let _csvTable   = null;   // DuckDB table name of uploaded CSV
let _csvFilename = null;  // original filename

// ── DOM refs (set in init) ─────────────────────────────────────────────────
let messagesEl, inputEl, sendBtn, sessionListEl,
    headerTitleEl, welcomeEl;

// ── Render helpers ─────────────────────────────────────────────────────────

function _escapeHtml(str) {
  return String(str)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function _renderUserBubble(text) {
  const row = document.createElement('div');
  row.className = 'message-row user';

  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';
  bubble.textContent = text;

  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar msg-avatar--user';
  avatar.id = 'user-msg-avatar';

  // Try to get user initial from sidebar avatar
  const sidebarAvatar = document.getElementById('user-avatar');
  avatar.textContent = sidebarAvatar ? sidebarAvatar.textContent : 'U';

  row.appendChild(bubble);
  row.appendChild(avatar);
  return row;
}

function _buildResultTable(columns, rows) {
  if (!columns.length || !rows.length) return null;

  const wrap = document.createElement('div');
  wrap.className = 'result-table-wrap';

  const hdr = document.createElement('div');
  hdr.className = 'result-table-header';
  hdr.innerHTML = `<span>📊 ${rows.length} row${rows.length !== 1 ? 's' : ''}</span>`;
  wrap.appendChild(hdr);

  const container = document.createElement('div');
  container.className = 'result-table-container';

  const tbl = document.createElement('table');
  tbl.className = 'result-table';

  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  for (const col of columns) {
    const th = document.createElement('th');
    th.textContent = col;
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);
  tbl.appendChild(thead);

  const tbody = document.createElement('tbody');
  const displayRows = rows.slice(0, 50);
  for (const row of displayRows) {
    const tr = document.createElement('tr');
    for (const col of columns) {
      const td = document.createElement('td');
      const val = row[col];
      td.textContent = val === null || val === undefined ? '—' : val;
      td.title = String(val ?? '');
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  tbl.appendChild(tbody);
  container.appendChild(tbl);
  wrap.appendChild(container);

  if (rows.length > 50) {
    const note = document.createElement('div');
    note.className = 'result-table-more';
    note.textContent = `Showing 50 of ${rows.length} rows`;
    wrap.appendChild(note);
  }
  return wrap;
}

function _buildConfidenceBadge(confidence) {
  const pct = Math.round((confidence ?? 0) * 100);
  const cls = pct >= 85 ? 'conf-high' : pct >= 65 ? 'conf-med' : 'conf-low';
  const badge = document.createElement('span');
  badge.className = `confidence-badge ${cls}`;
  badge.title = `SQL confidence: ${pct}%`;
  badge.innerHTML = `<span class="conf-dot"></span>${pct}%`;
  return badge;
}

function _buildSqlDisclosure(sql, confidence, reasoning) {
  const wrap = document.createElement('div');
  wrap.className = 'sql-disclosure-wrap';

  // ── Row 1: Generated SQL toggle + confidence badge ─────────────────────
  const details = document.createElement('details');
  details.className = 'sql-disclosure';

  const summary = document.createElement('summary');
  summary.innerHTML = `
    <svg class="sql-chevron" width="12" height="12" viewBox="0 0 12 12" fill="none">
      <path d="M4 2l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
    </svg>
    <span>Generated SQL</span>
    <button class="sql-copy-btn" type="button">Copy</button>
  `;
  summary.querySelector('.sql-copy-btn').addEventListener('click', async (e) => {
    e.preventDefault(); e.stopPropagation();
    await navigator.clipboard.writeText(sql).catch(() => {});
    const btn = e.currentTarget;
    const orig = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = orig; }, 1500);
  });
  if (confidence != null) {
    summary.appendChild(_buildConfidenceBadge(confidence));
  }
  details.appendChild(summary);

  const pre = document.createElement('pre');
  pre.className = 'sql-code-block';
  pre.textContent = sql;
  details.appendChild(pre);
  wrap.appendChild(details);

  // ── Row 2: "Why this SQL?" reasoning panel ─────────────────────────────
  if (reasoning) {
    const reasonDetails = document.createElement('details');
    reasonDetails.className = 'sql-reasoning';

    const reasonSummary = document.createElement('summary');
    reasonSummary.innerHTML = `
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
        <circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>
      </svg>
      Why this SQL?
    `;
    reasonDetails.appendChild(reasonSummary);

    const reasonBody = document.createElement('p');
    reasonBody.className = 'sql-reasoning-text';
    reasonBody.textContent = reasoning;
    reasonDetails.appendChild(reasonBody);
    wrap.appendChild(reasonDetails);
  }

  return wrap;
}

const _EXPORT_KEYWORDS = /\b(pdf|word|docx|ppt|pptx|powerpoint|export|download|report)\b/i;
const _CHART_KEYWORDS  = /\b(chart|graph|plot|visuali[sz]e|bar|pie|line|trend|visual)\b/i;

function _detectFormat(query) {
  if (!query) return null;
  const q = query.toLowerCase();
  if (/\bpdf\b/.test(q)) return 'pdf';
  if (/\b(word|docx)\b/.test(q)) return 'word';
  if (/\b(ppt|pptx|powerpoint|presentation)\b/.test(q)) return 'ppt';
  return null;
}

function _detectChartType(query) {
  const q = (query || '').toLowerCase();
  if (/\b(pie|donut|doughnut|proportion|share|breakdown)\b/.test(q)) return 'doughnut';
  if (/\b(line|trend|over time|monthly|yearly|daily|weekly|growth)\b/.test(q)) return 'line';
  return 'bar';
}

const _CHART_PALETTE = [
  'rgba(37,99,235,.75)','rgba(79,70,229,.75)','rgba(16,185,129,.75)',
  'rgba(245,158,11,.75)','rgba(239,68,68,.75)','rgba(14,165,233,.75)',
  'rgba(168,85,247,.75)','rgba(236,72,153,.75)','rgba(20,184,166,.75)',
  'rgba(251,146,60,.75)',
];

function _buildChart(columns, rows, query) {
  if (!columns?.length || !rows?.length) return null;

  const sample = rows[0];
  const numCols = columns.filter(c => !isNaN(parseFloat(sample[c])) && sample[c] !== null);
  const strCols = columns.filter(c => isNaN(parseFloat(sample[c])) || typeof sample[c] === 'string');

  if (!numCols.length) return null;

  const labelCol  = strCols[0] || columns[0];
  const valueCols = numCols.slice(0, 4); // max 4 series
  const chartType = _detectChartType(query);
  const displayRows = rows.slice(0, 20);
  const labels = displayRows.map(r => String(r[labelCol] ?? ''));

  const datasets = valueCols.map((col, i) => ({
    label: col,
    data: displayRows.map(r => parseFloat(r[col]) || 0),
    backgroundColor: chartType === 'doughnut'
      ? _CHART_PALETTE.slice(0, displayRows.length)
      : _CHART_PALETTE[i],
    borderColor: chartType === 'line' ? _CHART_PALETTE[i] : 'transparent',
    borderWidth: chartType === 'line' ? 2 : 0,
    borderRadius: chartType === 'bar' ? 5 : 0,
    fill: false,
    tension: 0.35,
    pointRadius: chartType === 'line' ? 3 : 0,
  }));

  const wrap = document.createElement('div');
  wrap.className = 'chart-wrap';

  const canvas = document.createElement('canvas');
  wrap.appendChild(canvas);

  setTimeout(() => {
    if (typeof Chart === 'undefined') return;
    new Chart(canvas, {
      type: chartType,
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: datasets.length > 1 || chartType === 'doughnut', position: 'bottom',
            labels: { font: { size: 11, family: 'Inter' }, boxWidth: 12, padding: 12 } },
          tooltip: { bodyFont: { family: 'Inter' }, titleFont: { family: 'Inter' } },
        },
        scales: chartType === 'doughnut' ? {} : {
          x: { ticks: { font: { size: 11, family: 'Inter' }, maxRotation: 45 },
               grid: { display: false } },
          y: { beginAtZero: true, ticks: { font: { size: 11, family: 'Inter' } },
               grid: { color: 'rgba(0,0,0,.05)' } },
        },
      },
    });
  }, 50);

  return wrap;
}

// ── Streaming bubble scaffold ──────────────────────────────────────────────
// Sections are hidden and revealed as SSE events arrive.
function _createStreamingBubble() {
  const row = document.createElement('div');
  row.className = 'message-row assistant';

  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar msg-avatar--bot';
  avatar.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.93 4.93l2.12 2.12M16.95 16.95l2.12 2.12M4.93 19.07l2.12-2.12M16.95 7.05l2.12-2.12"/></svg>`;

  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';

  // Status bar — shown while waiting, hidden once tokens arrive
  const statusBar = document.createElement('div');
  statusBar.className = 'stream-status bubble-section';
  statusBar.innerHTML = `<span class="stream-status-dot"></span><span class="stream-status-text">Thinking…</span>`;
  bubble.appendChild(statusBar);

  // SQL section — revealed on 'sql' event
  const sqlWrap = document.createElement('div');
  sqlWrap.style.display = 'none';
  bubble.appendChild(sqlWrap);

  // Result section — revealed on 'result' event
  const resultWrap = document.createElement('div');
  resultWrap.style.display = 'none';
  bubble.appendChild(resultWrap);

  // Summary section — revealed on first 'token' event, text types in here
  const summaryDivider = _buildDivider();
  summaryDivider.style.display = 'none';
  bubble.appendChild(summaryDivider);

  const summarySection = document.createElement('div');
  summarySection.className = 'bubble-section';
  summarySection.style.display = 'none';
  const summaryP = document.createElement('p');
  summaryP.className = 'summary-text';
  const cursor = document.createElement('span');
  cursor.className = 'stream-cursor';
  summarySection.appendChild(summaryP);
  summarySection.appendChild(cursor);
  bubble.appendChild(summarySection);

  // Footer — revealed on 'done' event (follow-ups + export)
  const footerWrap = document.createElement('div');
  footerWrap.style.display = 'none';
  bubble.appendChild(footerWrap);

  row.appendChild(avatar);
  row.appendChild(bubble);

  return { row, bubble, statusBar, sqlWrap, resultWrap, summaryDivider, summarySection, summaryP, cursor, footerWrap };
}


function _renderAssistantBubble(data) {
  const row = document.createElement('div');
  row.className = 'message-row assistant';
  row.dataset.sessionId = data.session_id;

  // Bot avatar
  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar msg-avatar--bot';
  avatar.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.93 4.93l2.12 2.12M16.95 16.95l2.12 2.12M4.93 19.07l2.12-2.12M16.95 7.05l2.12-2.12"/></svg>`;

  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';

  // ── Summary section ────────────────────────────────────────
  const summarySection = document.createElement('div');
  summarySection.className = 'bubble-section';

  const summaryP = document.createElement('p');
  summaryP.className = 'summary-text';
  summaryP.textContent = data.summary;
  summarySection.appendChild(summaryP);
  bubble.appendChild(summarySection);

  // ── Key insights ───────────────────────────────────────────
  if (data.key_insights?.length) {
    bubble.appendChild(_buildDivider());
    const insightsSection = document.createElement('div');
    insightsSection.className = 'bubble-section';

    const insightsHeader = document.createElement('div');
    insightsHeader.className = 'section-label';
    insightsHeader.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg> Key Insights`;
    insightsSection.appendChild(insightsHeader);

    const insightsList = document.createElement('ul');
    insightsList.className = 'key-insights';
    for (const insight of data.key_insights) {
      const li = document.createElement('li');
      li.textContent = insight;
      insightsList.appendChild(li);
    }
    insightsSection.appendChild(insightsList);
    bubble.appendChild(insightsSection);
  }

  // ── SQL disclosure ─────────────────────────────────────────
  if (data.generated_sql) {
    bubble.appendChild(_buildDivider());
    const sqlSection = document.createElement('div');
    sqlSection.className = 'bubble-section';
    sqlSection.appendChild(_buildSqlDisclosure(data.generated_sql, data.sql_confidence, data.sql_reasoning));
    bubble.appendChild(sqlSection);
  }

  // ── Chart (when user asks for graph/chart) ─────────────────
  const wantsChart = data.query && _CHART_KEYWORDS.test(data.query);
  const chartRows = data.rows?.length ? data.rows : (_lastDataMsg?.rows || []);
  const chartCols = data.columns?.length ? data.columns : (_lastDataMsg?.columns || []);
  if (wantsChart && chartRows.length && chartCols.length) {
    const chart = _buildChart(chartCols, chartRows, data.query);
    if (chart) {
      bubble.appendChild(_buildDivider());
      const chartSection = document.createElement('div');
      chartSection.className = 'bubble-section';
      const chartLabel = document.createElement('div');
      chartLabel.className = 'section-label';
      chartLabel.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M9 3v18"/><path d="M7 14l3-3 3 3 4-4"/></svg> Chart`;
      chartSection.appendChild(chartLabel);
      chartSection.appendChild(chart);
      bubble.appendChild(chartSection);
    }
  }

  // ── Result table ───────────────────────────────────────────
  // Use current rows, or fall back to last data rows when user asks for table
  const _TABLE_KEYWORDS = /\b(table|data|result|row|show\s+me|display)\b/i;
  const showLastTable = !data.rows?.length && data.query && _TABLE_KEYWORDS.test(data.query) && _lastDataMsg?.rows?.length;
  const tableRows    = data.rows?.length    ? data.rows    : (showLastTable ? _lastDataMsg.rows    : []);
  const tableCols    = data.columns?.length ? data.columns : (showLastTable ? _lastDataMsg.columns : []);

  if (tableRows.length && tableCols.length) {
    bubble.appendChild(_buildDivider());
    const tableSection = document.createElement('div');
    tableSection.className = 'bubble-section';

    const tableHeader = document.createElement('div');
    tableHeader.className = 'section-label';
    tableHeader.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18"/></svg> Data Results <span class="row-badge">${tableRows.length} rows</span>`;
    tableSection.appendChild(tableHeader);

    const tbl = _buildResultTable(tableCols, tableRows);
    if (tbl) tableSection.appendChild(tbl);
    bubble.appendChild(tableSection);
  }

  // ── Export panel — only when user explicitly asked for it ──
  if (data.query && _EXPORT_KEYWORDS.test(data.query)) {
    bubble.appendChild(_buildDivider());
    const exportSection = document.createElement('div');
    exportSection.className = 'bubble-section';
    const detectedFmt = _detectFormat(data.query);

    // If this response has no rows, export the last data response instead
    const hasData   = data.rows?.length > 0;
    const exportRef = hasData ? data : (_lastDataMsg || data);
    const exportPanel = buildExportPanel(exportRef.session_id, detectedFmt, exportRef.message_id);
    exportPanel.classList.add('export-panel--highlighted');
    exportSection.appendChild(exportPanel);
    bubble.appendChild(exportSection);
  }

  // ── Follow-up questions ────────────────────────────────────
  if (data.follow_up_questions?.length) {
    bubble.appendChild(_buildDivider());
    const followUpSection = document.createElement('div');
    followUpSection.className = 'bubble-section follow-up-section';

    const label = document.createElement('div');
    label.className = 'section-label';
    label.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 015.83 1c0 2-3 3-3 3M12 17h.01"/></svg> Suggested Follow-ups`;
    followUpSection.appendChild(label);

    const chips = document.createElement('div');
    chips.className = 'follow-up-chips';
    for (const q of data.follow_up_questions) {
      const chip = document.createElement('button');
      chip.className = 'follow-up-chip';
      chip.textContent = q;
      chip.addEventListener('click', () => sendQuery(q));
      chips.appendChild(chip);
    }
    followUpSection.appendChild(chips);
    bubble.appendChild(followUpSection);
  }

  // ── Meta footer ────────────────────────────────────────────
  const meta = document.createElement('div');
  meta.className = 'message-meta';
  const cacheStr = data.cache_hit ? '⚡ cached' : `${data.llm_provider}`;
  const latStr   = `${data.latency_ms}ms`;
  const tokStr   = data.cache_hit ? '' : `${data.tokens_used} tokens`;
  meta.innerHTML = [cacheStr, latStr, tokStr].filter(Boolean)
    .map(s => `<span>${_escapeHtml(s)}</span>`).join('<span class="meta-sep">·</span>');
  bubble.appendChild(meta);

  row.appendChild(avatar);
  row.appendChild(bubble);
  return row;
}

function _buildDivider() {
  const d = document.createElement('div');
  d.className = 'bubble-divider';
  return d;
}

function _renderErrorBubble(message) {
  const row = document.createElement('div');
  row.className = 'message-row assistant';
  const err = document.createElement('div');
  err.className = 'error-message';
  err.innerHTML = `<span>⚠️</span><span>${_escapeHtml(message)}</span>`;
  row.appendChild(err);
  return row;
}

function _renderTyping() {
  const row = document.createElement('div');
  row.className = 'message-row assistant';
  row.id = 'typing-row';

  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar msg-avatar--bot';
  avatar.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.93 4.93l2.12 2.12M16.95 16.95l2.12 2.12M4.93 19.07l2.12-2.12M16.95 7.05l2.12-2.12"/></svg>`;

  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';
  bubble.innerHTML = `<div class="typing-indicator"><div class="typing-dot"></div><div class="typing-dot"></div><div class="typing-dot"></div></div>`;

  row.appendChild(avatar);
  row.appendChild(bubble);
  return row;
}

function _scrollToBottom() {
  requestAnimationFrame(() => {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  });
}

function _setInputState(enabled) {
  inputEl.disabled = !enabled;
  sendBtn.disabled = !enabled;
  isLoading = !enabled;
}

// ── Pinned sessions (localStorage) ────────────────────────────────────────
const PINNED_KEY = 'pinned_sessions';
function _getPinned() {
  try { return JSON.parse(localStorage.getItem(PINNED_KEY) || '[]'); } catch { return []; }
}
function _setPinned(ids) { localStorage.setItem(PINNED_KEY, JSON.stringify(ids)); }
function _isPinned(id) { return _getPinned().includes(id); }
function _togglePin(id) {
  const pinned = _getPinned();
  const idx = pinned.indexOf(id);
  if (idx === -1) pinned.push(id); else pinned.splice(idx, 1);
  _setPinned(pinned);
}

// ── Session list ──────────────────────────────────────────────────────────

async function loadSessions() {
  try {
    const res = await get('/chat/sessions');
    if (!res.ok) return;
    const sessions = await res.json();
    renderSessionList(sessions);
  } catch { /* silent */ }
}

function renderSessionList(sessions) {
  sessionListEl.innerHTML = '';
  if (!sessions.length) {
    sessionListEl.innerHTML = '<div class="session-empty">No chats yet.<br>Start a new conversation above.</div>';
    return;
  }
  const pinned = _getPinned();
  const sorted = [...sessions].sort((a, b) => {
    const ap = pinned.includes(a.id) ? 0 : 1;
    const bp = pinned.includes(b.id) ? 0 : 1;
    return ap - bp;
  });
  for (const s of sorted) {
    sessionListEl.appendChild(_buildSessionItem(s));
  }
}

function _buildSessionItem(s) {
  const pinned = _isPinned(s.id);
  const item = document.createElement('div');
  item.className = 'session-item' +
    (s.id === currentSessionId ? ' active' : '') +
    (pinned ? ' pinned' : '');
  item.dataset.id = s.id;

  // Chat icon
  const chatIcon = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  chatIcon.setAttribute('width', '13'); chatIcon.setAttribute('height', '13');
  chatIcon.setAttribute('viewBox', '0 0 24 24'); chatIcon.setAttribute('fill', 'none');
  chatIcon.setAttribute('stroke', 'currentColor'); chatIcon.setAttribute('stroke-width', '2');
  chatIcon.innerHTML = '<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>';
  item.appendChild(chatIcon);

  // Pin indicator (shows when pinned)
  const pinIndicator = document.createElement('span');
  pinIndicator.className = 'session-pin-indicator';
  pinIndicator.innerHTML = `<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17 5.8 21.3l2.4-7.4L2 9.4h7.6z"/></svg>`;
  item.appendChild(pinIndicator);

  // Title
  const titleSpan = document.createElement('span');
  titleSpan.className = 'session-title';
  titleSpan.textContent = s.title;
  item.appendChild(titleSpan);

  // Action buttons
  const actions = document.createElement('div');
  actions.className = 'session-actions';

  // Pin button
  const pinBtn = document.createElement('button');
  pinBtn.className = 'session-action-btn pin-btn';
  pinBtn.title = pinned ? 'Unpin' : 'Pin to top';
  pinBtn.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="${pinned ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17 5.8 21.3l2.4-7.4L2 9.4h7.6z"/></svg>`;
  pinBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    _togglePin(s.id);
    loadSessions();
  });

  // Edit button
  const editBtn = document.createElement('button');
  editBtn.className = 'session-action-btn edit-btn';
  editBtn.title = 'Rename';
  editBtn.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>`;
  editBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    _startRename(s.id, titleSpan);
  });

  // Delete button
  const delBtn = document.createElement('button');
  delBtn.className = 'session-action-btn del-btn';
  delBtn.title = 'Delete';
  delBtn.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>`;
  delBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    deleteSession(s.id, item);
  });

  actions.appendChild(pinBtn);
  actions.appendChild(editBtn);
  actions.appendChild(delBtn);
  item.appendChild(actions);

  item.addEventListener('click', () => loadSessionHistory(s.id, s.title));
  return item;
}

async function _startRename(sessionId, titleSpan) {
  const original = titleSpan.textContent;
  titleSpan.contentEditable = 'true';
  titleSpan.focus();

  // Select all text
  const range = document.createRange();
  range.selectNodeContents(titleSpan);
  window.getSelection().removeAllRanges();
  window.getSelection().addRange(range);

  const commit = async () => {
    titleSpan.contentEditable = 'false';
    const newTitle = titleSpan.textContent.trim() || original;
    titleSpan.textContent = newTitle;
    if (newTitle === original) return;
    try {
      await post(`/chat/sessions/${sessionId}/rename`, { title: newTitle });
      if (currentSessionId === sessionId) headerTitleEl.textContent = newTitle;
    } catch {
      titleSpan.textContent = original;
      showToast('Rename failed', 'error');
    }
  };

  titleSpan.addEventListener('blur', commit, { once: true });
  titleSpan.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); titleSpan.blur(); }
    if (e.key === 'Escape') { titleSpan.textContent = original; titleSpan.blur(); }
  }, { once: true });
}

async function loadSessionHistory(sessionId, title) {
  currentSessionId = sessionId;
  _lastDataMsg = null;   // reset when switching sessions
  _csvTable = null;
  _csvFilename = null;
  document.getElementById('csv-badge-area').innerHTML = '';
  document.getElementById('upload-csv-btn')?.classList.remove('active');
  headerTitleEl.textContent = title;
  document.querySelectorAll('.session-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === sessionId);
  });

  messagesEl.innerHTML = '';
  welcomeEl?.remove();

  try {
    const res = await get(`/chat/sessions/${sessionId}`);
    if (!res.ok) return;
    const data = await res.json();
    const pairs = [];
    const msgs  = data.messages;

    for (let i = 0; i < msgs.length; i++) {
      if (msgs[i].role === 'user') {
        messagesEl.appendChild(_renderUserBubble(msgs[i].content));
      } else {
        // Re-build a partial response object for the assistant message
        messagesEl.appendChild(_renderAssistantBubble({
          session_id:    sessionId,
          summary:       msgs[i].content,
          generated_sql: msgs[i].generated_sql || '',
          rows: [], columns: [],   // rows not stored; show summary only
          tokens_used:   msgs[i].tokens_used || 0,
          cache_hit:     msgs[i].cache_hit,
          llm_provider:  msgs[i].llm_provider || '—',
          latency_ms:    0,
        }));
      }
    }
    _scrollToBottom();
  } catch { showToast('Failed to load session', 'error'); }
}

async function deleteSession(sessionId, itemEl) {
  if (!confirm('Delete this chat?')) return;
  try {
    const res = await del(`/chat/sessions/${sessionId}`);
    if (res.ok) {
      itemEl.remove();
      if (currentSessionId === sessionId) {
        currentSessionId = null;
        messagesEl.innerHTML = '';
        headerTitleEl.textContent = 'New Chat';
        _showWelcome();
      }
    }
  } catch { showToast('Failed to delete session', 'error'); }
}

// ── Query dispatch ────────────────────────────────────────────────────────

async function sendQuery(text) {
  if (!text.trim() || isLoading) return;

  welcomeEl?.remove();
  welcomeEl = null;

  // Append user bubble
  messagesEl.appendChild(_renderUserBubble(text));
  inputEl.value = '';
  inputEl.style.height = 'auto';
  _scrollToBottom();

  // Typing indicator
  _setInputState(false);
  const typingRow = _renderTyping();
  messagesEl.appendChild(typingRow);
  _scrollToBottom();

  // ── Pure export/download request → skip backend entirely ────────────
  const _PURE_EXPORT = /^(give\s+(me\s+)?(a\s+)?)?(pdf|word|docx|ppt|pptx|powerpoint|download|export)(\s+(report|file|document|presentation))?[.!?]?$/i;
  if (_lastDataMsg && _EXPORT_KEYWORDS.test(text) && _PURE_EXPORT.test(text.trim())) {
    await new Promise(r => setTimeout(r, 300));
    typingRow.remove();
    const detectedFmt = _detectFormat(text);
    const panel = buildExportPanel(_lastDataMsg.session_id, detectedFmt, _lastDataMsg.message_id);
    panel.classList.add('export-panel--highlighted');
    const row = document.createElement('div');
    row.className = 'message-row assistant';
    const bubble = document.createElement('div');
    bubble.className = 'message-bubble';
    const sec = document.createElement('div');
    sec.className = 'bubble-section';
    sec.appendChild(panel);
    bubble.appendChild(sec);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    _scrollToBottom();
    _setInputState(true);
    inputEl.focus();
    return;
  }

  // ── Chart/viz request with existing data → skip backend entirely ──────
  if (_lastDataMsg?.rows?.length && _CHART_KEYWORDS.test(text)) {
    await new Promise(r => setTimeout(r, 400)); // brief typing effect
    typingRow.remove();
    messagesEl.appendChild(_renderAssistantBubble({
      session_id:          currentSessionId || _lastDataMsg.session_id,
      message_id:          _lastDataMsg.message_id,
      query:               text,
      generated_sql:       '',
      rows:                _lastDataMsg.rows,
      columns:             _lastDataMsg.columns,
      summary:             'Here is the chart based on the previous query results.',
      key_insights:        [],
      follow_up_questions: [],
      tokens_used:         0,
      cache_hit:           true,
      llm_provider:        'local',
      latency_ms:          0,
    }));
    _scrollToBottom();
    _setInputState(true);
    inputEl.focus();
    return;
  }

  try {
    // ── SSE streaming request ──────────────────────────────────────────
    const res = await postStream('/chat/stream', {
      query:        text,
      session_id:   currentSessionId || undefined,
      upload_table: _csvTable || undefined,
    });

    typingRow.remove();

    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      messagesEl.appendChild(_renderErrorBubble(body.detail || 'Something went wrong.'));
      _scrollToBottom();
      return;
    }

    // Build the streaming bubble and mount it immediately
    const {
      row: streamRow, bubble,
      statusBar, sqlWrap, resultWrap,
      summaryDivider, summarySection, summaryP, cursor,
      footerWrap,
    } = _createStreamingBubble();
    messagesEl.appendChild(streamRow);
    _scrollToBottom();

    let streamColumns = [], streamRows = [];

    // ── Token queue — drains at a capped rate for smooth typewriter effect
    const tokenQueue = [];
    let   draining   = false;
    const TOKEN_INTERVAL_MS = 30; // ~33 chars/sec regardless of LLM speed

    function drainTokens() {
      if (draining || tokenQueue.length === 0) return;
      draining = true;
      (function step() {
        if (tokenQueue.length === 0) { draining = false; return; }
        // First token: transition from status bar to summary section
        if (summarySection.style.display === 'none') {
          statusBar.style.display      = 'none';
          summaryDivider.style.display = '';
          summarySection.style.display = '';
        }
        summaryP.textContent += tokenQueue.shift();
        _scrollToBottom();
        setTimeout(step, TOKEN_INTERVAL_MS);
      })();
    }

    // ── Read SSE chunks ────────────────────────────────────────────────
    const reader  = res.body.getReader();
    const decoder = new TextDecoder();
    let   buffer  = '';

    outer: while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE messages are separated by double newline
      const parts = buffer.split('\n\n');
      buffer = parts.pop();   // keep the incomplete trailing chunk

      for (const part of parts) {
        const line = part.trim();
        if (!line.startsWith('data: ')) continue;
        let evt;
        try { evt = JSON.parse(line.slice(6)); } catch { continue; }

        switch (evt.type) {

          case 'status':
            statusBar.querySelector('.stream-status-text').textContent = evt.message;
            break;

          case 'sql':
            if (evt.sql) {
              sqlWrap.appendChild(_buildDivider());
              const sqlSec = document.createElement('div');
              sqlSec.className = 'bubble-section';
              sqlSec.appendChild(_buildSqlDisclosure(evt.sql, evt.confidence, evt.reasoning));
              sqlWrap.appendChild(sqlSec);
              sqlWrap.style.display = '';
            }
            break;

          case 'result':
            if (evt.columns?.length && evt.rows?.length) {
              streamColumns = evt.columns;
              streamRows    = evt.rows;
              resultWrap.appendChild(_buildDivider());
              const tbl = _buildResultTable(evt.columns, evt.rows);
              if (tbl) resultWrap.appendChild(tbl);
              const chart = _buildChart(evt.columns, evt.rows, text);
              if (chart) {
                const chartSec = document.createElement('div');
                chartSec.className = 'bubble-section';
                chartSec.appendChild(chart);
                resultWrap.appendChild(chartSec);
              }
              resultWrap.style.display = '';
              _scrollToBottom();
            }
            break;

          case 'token':
            tokenQueue.push(evt.content);
            drainTokens();
            break;

          case 'done': {
            // Wait for any queued tokens to finish typing before showing footer
            await new Promise(resolve => {
              (function waitDrain() {
                tokenQueue.length === 0 ? resolve() : setTimeout(waitDrain, TOKEN_INTERVAL_MS);
              })();
            });
            cursor.remove();
            currentSessionId = evt.session_id;

            // Persist last data msg for chart/export reuse
            if (streamRows.length > 0) {
              _lastDataMsg = {
                message_id: evt.message_id,
                session_id: evt.session_id,
                rows:       streamRows,
                columns:    streamColumns,
              };
            }

            // Follow-up questions
            if (evt.follow_up_questions?.length) {
              footerWrap.appendChild(_buildDivider());
              const fqSec = document.createElement('div');
              fqSec.className = 'bubble-section follow-up-section';
              fqSec.innerHTML = `<div class="section-label">
                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 015.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                Follow-up
              </div>`;
              const fqList = document.createElement('div');
              fqList.className = 'followup-list';
              for (const q of evt.follow_up_questions) {
                const btn = document.createElement('button');
                btn.className  = 'followup-btn';
                btn.textContent = q;
                btn.addEventListener('click', () => { inputEl.value = q; inputEl.focus(); });
                fqList.appendChild(btn);
              }
              fqSec.appendChild(fqList);
              footerWrap.appendChild(fqSec);
            }

            // Export panel
            footerWrap.appendChild(_buildDivider());
            const expSec = document.createElement('div');
            expSec.className = 'bubble-section';
            expSec.appendChild(buildExportPanel(evt.session_id, _detectFormat(text), evt.message_id));
            footerWrap.appendChild(expSec);
            footerWrap.style.display = '';

            headerTitleEl.textContent = text.slice(0, 60);
            await loadSessions();
            fetchAndUpdateQuota();
            _scrollToBottom();
            break outer;
          }

          case 'error': {
            cursor.remove();
            statusBar.style.display = 'none';
            const errSec = document.createElement('div');
            errSec.className = 'bubble-section stream-error';
            errSec.textContent = evt.message;
            bubble.appendChild(errSec);
            _scrollToBottom();
            break outer;
          }
        }
      }
    }

  } catch (err) {
    typingRow.remove();
    const msg = err.status === 429
      ? 'Token quota or rate limit reached. Please wait.'
      : (err.message || 'Network error');
    messagesEl.appendChild(_renderErrorBubble(msg));
    _scrollToBottom();
  } finally {
    _setInputState(true);
    inputEl.focus();
  }
}

// ── Welcome screen ────────────────────────────────────────────────────────

const SUGGESTIONS = [
  'Show total revenue by region',
  'Who are the top 5 customers by spend?',
  'Which product categories have the best margin?',
  'Compare Q1 vs Q2 2024 sales',
  'Which sales rep exceeded their quarterly target?',
  'Show monthly order trends for 2024',
];

function _showWelcome() {
  const el = document.createElement('div');
  el.className = 'welcome-screen';
  el.id = 'welcome-screen';
  el.innerHTML = `
    <div class="welcome-icon">💬</div>
    <h2>Ask anything about your sales data</h2>
    <p>I'll convert your question to SQL, fetch the results, and summarize the insights.</p>
    <div class="suggestion-chips"></div>
  `;
  const chips = el.querySelector('.suggestion-chips');
  for (const s of SUGGESTIONS) {
    const chip = document.createElement('button');
    chip.className = 'chip';
    chip.textContent = s;
    chip.addEventListener('click', () => sendQuery(s));
    chips.appendChild(chip);
  }
  messagesEl.appendChild(el);
  welcomeEl = el;
}

// ── Init ──────────────────────────────────────────────────────────────────

export function initChat() {
  messagesEl    = document.getElementById('messages');
  inputEl       = document.getElementById('chat-input');
  sendBtn       = document.getElementById('send-btn');
  sessionListEl = document.getElementById('session-list');
  headerTitleEl = document.getElementById('chat-title');

  // Send on button click
  sendBtn.addEventListener('click', () => sendQuery(inputEl.value));

  // Send on Enter (Shift+Enter = newline)
  inputEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendQuery(inputEl.value);
    }
  });

  // Auto-resize textarea
  inputEl.addEventListener('input', () => {
    inputEl.style.height = 'auto';
    inputEl.style.height = Math.min(inputEl.scrollHeight, 180) + 'px';
  });

  // New chat button
  document.getElementById('new-chat-btn')?.addEventListener('click', () => {
    currentSessionId = null;
    _lastDataMsg = null;
    _csvTable = null;
    _csvFilename = null;
    document.getElementById('csv-badge-area').innerHTML = '';
    document.getElementById('upload-csv-btn').classList.remove('active');
    messagesEl.innerHTML = '';
    headerTitleEl.textContent = 'New Chat';
    document.querySelectorAll('.session-item').forEach(el => el.classList.remove('active'));
    _showWelcome();
    inputEl.focus();
  });

  // ── CSV upload ────────────────────────────────────────────────────────
  const csvInput   = document.getElementById('csv-file-input');
  const uploadBtn  = document.getElementById('upload-csv-btn');
  const badgeArea  = document.getElementById('csv-badge-area');

  uploadBtn.addEventListener('click', () => csvInput.click());

  csvInput.addEventListener('change', async () => {
    const file = csvInput.files[0];
    if (!file) return;
    csvInput.value = '';

    uploadBtn.disabled = true;
    uploadBtn.innerHTML = `<span class="spinner spinner-dark" style="width:14px;height:14px;border-width:1.5px"></span>`;

    try {
      const { uploadFile } = await import('./api.js');
      const res = await uploadFile('/upload/csv', file, {
        session_id: currentSessionId || undefined,
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        showToast(body.detail || 'Upload failed', 'error');
        return;
      }
      const data = await res.json();
      _csvTable    = data.table_name;
      _csvFilename = data.original_filename;

      // Show badge
      _renderCsvBadge(badgeArea, data.original_filename, data.columns, data.row_count, data.upload_id);
      uploadBtn.classList.add('active');

      // Show preview bubble in chat
      _showCsvPreviewBubble(data);
      showToast(`${data.original_filename} loaded (${data.row_count} rows)`, 'success');
    } catch (err) {
      showToast(err.message || 'Upload error', 'error');
    } finally {
      uploadBtn.disabled = false;
      uploadBtn.innerHTML = `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>`;
    }
  });

  function _renderCsvBadge(container, filename, columns, rowCount, uploadId) {
    container.innerHTML = '';
    const badge = document.createElement('div');
    badge.className = 'csv-badge';
    badge.innerHTML = `
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
      <span class="csv-badge-name" title="${_escapeHtml(filename)}">${_escapeHtml(filename)}</span>
      <span style="color:#86efac;font-size:.7rem">${rowCount.toLocaleString()} rows · ${columns.length} cols</span>
      <button class="csv-badge-close" title="Remove CSV">✕</button>
    `;
    badge.querySelector('.csv-badge-close').addEventListener('click', async () => {
      _csvTable    = null;
      _csvFilename = null;
      container.innerHTML = '';
      uploadBtn.classList.remove('active');
      if (uploadId) {
        try { await del('/upload/' + uploadId); } catch (e) { /* best-effort */ }
      }
    });
    container.appendChild(badge);
  }

  function _showCsvPreviewBubble(data) {
    welcomeEl?.remove();
    welcomeEl = null;
    const div = document.createElement('div');
    div.className = 'csv-preview-bubble';
    const cols = data.columns.slice(0, 12).map(c =>
      `<span class="csv-col-pill">${_escapeHtml(c)}</span>`
    ).join('');
    const more = data.columns.length > 12 ? `<span class="csv-col-pill">+${data.columns.length - 12} more</span>` : '';
    div.innerHTML = `
      <strong>📂 ${_escapeHtml(data.original_filename)}</strong> uploaded —
      <strong>${data.row_count.toLocaleString()}</strong> rows, <strong>${data.columns.length}</strong> columns.<br>
      <div class="csv-preview-cols">${cols}${more}</div>
      <div style="margin-top:8px;font-size:.78rem;color:#166534">Ask any question about this data.</div>
    `;
    messagesEl.appendChild(div);
    _scrollToBottom();
  }

  _showWelcome();
  loadSessions();
}
