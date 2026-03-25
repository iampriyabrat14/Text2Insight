/**
 * api.js — Central fetch wrapper.
 * - Injects Authorization: Bearer header on every request.
 * - On 401: attempts silent token refresh, then retries once.
 * - On 429: surfaces quota / rate-limit message via toast.
 */

// When served by FastAPI on the same origin, use relative paths.
// Override with full URL only when running the frontend from a different port (e.g. Live Server).
const API_BASE = window.location.port === '8000' || window.location.port === ''
  ? ''
  : 'http://127.0.0.1:8000';

const TOKEN_KEY   = 'access_token';
const REFRESH_KEY = 'refresh_token';

export const storage = {
  get: key => localStorage.getItem(key),
  set: (key, val) => localStorage.setItem(key, val),
  del: key => localStorage.removeItem(key),
  clear: () => { localStorage.removeItem(TOKEN_KEY); localStorage.removeItem(REFRESH_KEY); },
};

let _refreshPromise = null;

async function _doRefresh() {
  const rt = storage.get(REFRESH_KEY);
  if (!rt) throw new Error('No refresh token');

  const res = await fetch(`${API_BASE}/auth/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refresh_token: rt }),
  });
  if (!res.ok) throw new Error('Refresh failed');

  const data = await res.json();
  storage.set(TOKEN_KEY, data.access_token);
  storage.set(REFRESH_KEY, data.refresh_token);
  return data.access_token;
}

async function _getToken() {
  return storage.get(TOKEN_KEY);
}

/**
 * Core request helper.
 * @param {string} path
 * @param {RequestInit} opts
 * @param {boolean} _retry   internal flag to prevent infinite refresh loop
 */
export async function request(path, opts = {}, _retry = false) {
  const token = await _getToken();

  const headers = {
    'Content-Type': 'application/json',
    ...(opts.headers || {}),
  };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, { ...opts, headers });

  // ── 401 → try refresh once ───────────────────────────────────────────
  if (res.status === 401 && !_retry) {
    try {
      if (!_refreshPromise) _refreshPromise = _doRefresh().finally(() => { _refreshPromise = null; });
      await _refreshPromise;
      return request(path, opts, true);
    } catch {
      storage.clear();
      window.location.href = '/index.html';
      throw new Error('Session expired');
    }
  }

  // ── 429 → rate limit / quota ─────────────────────────────────────────
  if (res.status === 429) {
    const body = await res.json().catch(() => ({}));
    const detail = body.detail || 'Too many requests. Please slow down.';
    throw Object.assign(new Error(detail), { status: 429 });
  }

  return res;
}

/** Convenience helpers */
export const get  = (path, opts) => request(path, { method: 'GET', ...opts });
export const post = (path, body, opts) =>
  request(path, { method: 'POST', body: JSON.stringify(body), ...opts });
export const del  = (path, opts) => request(path, { method: 'DELETE', ...opts });

/**
 * POST for Server-Sent Events — returns the raw Response so the caller can
 * read res.body as a ReadableStream.  Inherits auth + 401-refresh from request().
 */
export const postStream = (path, body) =>
  request(path, { method: 'POST', body: JSON.stringify(body) });

/** Upload a file (multipart/form-data) */
export async function uploadFile(path, file, extraParams = {}) {
  const token = storage.get(TOKEN_KEY);
  const formData = new FormData();
  formData.append('file', file);

  // Build query string from extraParams
  const qs = Object.entries(extraParams)
    .filter(([, v]) => v != null)
    .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
    .join('&');
  const url = `${API_BASE}${path}${qs ? '?' + qs : ''}`;

  const res = await fetch(url, {
    method: 'POST',
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    body: formData,
  });
  return res;
}

/** Download a binary file — handles auth headers manually */
export async function download(path, filename) {
  const token = storage.get(TOKEN_KEY);
  const res = await fetch(`${API_BASE}${path}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || 'Download failed');
  }
  const blob = await res.blob();
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename || 'export';
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 2000);
}
