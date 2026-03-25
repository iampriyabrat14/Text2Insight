/**
 * toast.js — lightweight in-page notifications.
 */

function _ensureContainer() {
  let c = document.getElementById('toast-container');
  if (!c) {
    c = document.createElement('div');
    c.id = 'toast-container';
    document.body.appendChild(c);
  }
  return c;
}

/**
 * @param {string} message
 * @param {'info'|'success'|'error'} type
 * @param {number} duration ms
 */
export function showToast(message, type = 'info', duration = 4000) {
  const container = _ensureContainer();
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;

  const icons = { info: 'ℹ️', success: '✅', error: '❌' };
  toast.innerHTML = `<span>${icons[type] || ''}</span><span>${message}</span>`;

  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity .3s';
    setTimeout(() => toast.remove(), 320);
  }, duration);
}
