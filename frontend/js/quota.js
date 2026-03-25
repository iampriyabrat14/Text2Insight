/**
 * quota.js — Token usage bar. Polls /auth/me and updates the header bar.
 */
import { get } from './api.js';

let _pollTimer = null;

export function updateQuotaBar(quota) {
  const fill  = document.getElementById('quota-fill');
  const label = document.getElementById('quota-label');
  if (!fill || !label) return;

  const pct = Math.min(100, quota.percent_used);
  fill.style.width = `${pct}%`;
  fill.classList.remove('warn', 'danger');
  if (pct >= 90)      fill.classList.add('danger');
  else if (pct >= 65) fill.classList.add('warn');

  const remaining = quota.remaining.toLocaleString();
  const limit     = quota.token_limit.toLocaleString();
  label.textContent = `${remaining} / ${limit} tokens`;
  label.title = `${quota.percent_used}% used this month (${quota.year_month})`;
}

export async function fetchAndUpdateQuota() {
  try {
    const res = await get('/auth/me');
    if (res.ok) {
      const data = await res.json();
      updateQuotaBar(data.quota);
    }
  } catch { /* silent */ }
}

/** Start polling every 30 s */
export function startQuotaPolling(intervalMs = 30_000) {
  fetchAndUpdateQuota();
  _pollTimer = setInterval(fetchAndUpdateQuota, intervalMs);
}

export function stopQuotaPolling() {
  if (_pollTimer) clearInterval(_pollTimer);
}
