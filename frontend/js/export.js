/**
 * export.js — Export button handlers.
 */
import { download } from './api.js';
import { showToast } from './toast.js';

/**
 * Trigger a file export download.
 * @param {string} sessionId
 * @param {'pdf'|'word'|'ppt'} format
 * @param {HTMLElement} btn  — button to show loading state on
 */
export async function exportSession(sessionId, format, btn, messageId = null) {
  const ext = { pdf: 'pdf', word: 'docx', ppt: 'pptx' }[format] || format;
  const origHTML = btn.innerHTML;
  btn.classList.add('loading');
  btn.innerHTML = `<span class="spinner" style="width:12px;height:12px;border-width:1.5px"></span> Generating…`;

  try {
    let url = `/export/${sessionId}?format=${format}`;
    if (messageId) url += `&message_id=${messageId}`;
    await download(url, `report.${ext}`);
    showToast(`${format.toUpperCase()} downloaded`, 'success');
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    btn.classList.remove('loading');
    btn.innerHTML = origHTML;
  }
}

/**
 * Build the export panel DOM element for a given sessionId.
 */
export function buildExportPanel(sessionId, onlyFormat = null, messageId = null) {
  const panel = document.createElement('div');
  panel.className = 'export-panel';

  const allFormats = [
    { fmt: 'pdf',  icon: '📄', label: 'Download PDF'  },
    { fmt: 'word', icon: '📝', label: 'Download Word' },
    { fmt: 'ppt',  icon: '📊', label: 'Download PPT'  },
  ];

  const formats = onlyFormat
    ? allFormats.filter(f => f.fmt === onlyFormat)
    : allFormats;

  for (const { fmt, icon, label } of formats) {
    const btn = document.createElement('button');
    btn.className = 'btn-export';
    btn.innerHTML = `${icon} <span>${label}</span>`;
    btn.addEventListener('click', () => exportSession(sessionId, fmt, btn, messageId));
    panel.appendChild(btn);
  }

  return panel;
}
