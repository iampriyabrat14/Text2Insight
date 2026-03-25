/**
 * auth.js — Login / Register / Logout logic.
 */
import { post, storage } from './api.js';
import { showToast } from './toast.js';

const TOKEN_KEY   = 'access_token';
const REFRESH_KEY = 'refresh_token';
const USER_KEY    = 'user_info';

export function isLoggedIn() {
  return !!storage.get(TOKEN_KEY);
}

export function getUser() {
  const raw = storage.get(USER_KEY);
  return raw ? JSON.parse(raw) : null;
}

function _saveTokens(data) {
  storage.set(TOKEN_KEY, data.access_token);
  storage.set(REFRESH_KEY, data.refresh_token);
}

async function _fetchAndCacheUser() {
  const { get } = await import('./api.js');
  const res = await get('/auth/me');
  if (res.ok) {
    const user = await res.json();
    storage.set(USER_KEY, JSON.stringify(user));
    return user;
  }
  return null;
}

/** Called from login form submit */
export async function handleLogin(username, password, setLoading) {
  setLoading(true);
  try {
    const res = await post('/auth/login', { username, password });
    if (!res.ok) {
      const body = await res.json();
      throw new Error(body.detail || 'Login failed');
    }
    _saveTokens(await res.json());
    await _fetchAndCacheUser();
    window.location.href = '/chat.html';
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    setLoading(false);
  }
}

/** Called from register form submit */
export async function handleRegister(fields, setLoading) {
  setLoading(true);
  try {
    const res = await post('/auth/register', fields);
    if (!res.ok) {
      const body = await res.json();
      const detail = body.detail?.map?.(e => e.msg).join(', ') || body.detail || 'Registration failed';
      throw new Error(detail);
    }
    _saveTokens(await res.json());
    await _fetchAndCacheUser();
    showToast('Account created! Redirecting…', 'success');
    setTimeout(() => { window.location.href = '/chat.html'; }, 800);
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    setLoading(false);
  }
}

/** Logout — revokes tokens server-side then clears storage */
export async function logout() {
  try {
    const { post: apiPost } = await import('./api.js');
    await apiPost('/auth/logout', {});
  } catch { /* best-effort */ }
  storage.clear();
  storage.del(USER_KEY);
  window.location.href = '/index.html';
}

/** Guard: redirect to login if not authenticated */
export function requireAuth() {
  if (!isLoggedIn()) {
    window.location.href = '/index.html';
    return false;
  }
  return true;
}
