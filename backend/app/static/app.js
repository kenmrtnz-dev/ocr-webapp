const uploadArea = document.getElementById('uploadArea');
const browseButton = document.getElementById('browseButton');
const fileInput = document.getElementById('fileInput');
const fileInfo = document.getElementById('fileInfo');
const fileName = document.getElementById('fileName');
const fileSize = document.getElementById('fileSize');
const removeFileBtn = document.getElementById('removeFile');
const progressFill = document.getElementById('progressFill');
const progressPercent = document.getElementById('progressPercent');
const progressLabel = document.getElementById('progressLabel');
const progressStep = document.getElementById('progressStep');
const progressElapsed = document.getElementById('progressElapsed');
const progressModel = document.getElementById('progressModel');
const analyzerStatus = document.getElementById('analyzerStatus');
const progressAnalyzerRow = document.getElementById('progressAnalyzerRow');
const analyzerModel = document.getElementById('analyzerModel');
const analyzerProfile = document.getElementById('analyzerProfile');
const analyzerReason = document.getElementById('analyzerReason');
const downloadCSV = document.getElementById('downloadCSV');
const exportExcelBtn = document.getElementById('exportExcelBtn');
const viewDetails = document.getElementById('viewDetails');
const finishSave = document.getElementById('finishSave');
const tableBody = document.querySelector('.table-body');
const totalTransactions = document.getElementById('totalTransactions');
const totalDebitTransactions = document.getElementById('totalDebitTransactions');
const totalCreditTransactions = document.getElementById('totalCreditTransactions');
const endingBalance = document.getElementById('endingBalance');
const accountNameSummary = document.getElementById('accountNameSummary');
const accountNumberSummary = document.getElementById('accountNumberSummary');
const previewImage = document.getElementById('previewImage');
const previewCanvas = document.getElementById('previewCanvas');
const prevPageBtn = document.getElementById('prevPageBtn');
const nextPageBtn = document.getElementById('nextPageBtn');
const flattenModeBtn = document.getElementById('flattenModeBtn');
const applyFlattenBtn = document.getElementById('applyFlattenBtn');
const resetFlattenBtn = document.getElementById('resetFlattenBtn');
const resetViewBtn = document.getElementById('resetViewBtn');
const zoomOutBtn = document.getElementById('zoomOutBtn');
const zoomInBtn = document.getElementById('zoomInBtn');
const pageIndicator = document.getElementById('pageIndicator');
const pageSelect = document.getElementById('pageSelect');
const previewWrap = document.querySelector('.preview-canvas-wrap');
const zoomLevel = document.getElementById('zoomLevel');
const resultsSection = document.querySelector('.results-section');
const ocrToggle = document.getElementById('ocrToggle');
const modeToggleText = document.getElementById('modeToggleText');
const legacyMain = document.getElementById('legacyMain');
const authEmail = document.getElementById('authEmail');
const authPassword = document.getElementById('authPassword');
const authLoginBtn = document.getElementById('authLoginBtn');
const authLogoutBtn = document.getElementById('authLogoutBtn');
const authUserBadge = document.getElementById('authUserBadge');
const agentWorkflowPanel = document.getElementById('agentWorkflowPanel');
const evaluatorWorkflowPanel = document.getElementById('evaluatorWorkflowPanel');
const agentFileInput = document.getElementById('agentFileInput');
const agentBrowseButton = document.getElementById('agentBrowseButton');
const agentDropZone = document.getElementById('agentDropZone');
const agentSelectedFiles = document.getElementById('agentSelectedFiles');
const agentSearchInput = document.getElementById('agentSearchInput');
const agentUploadProgressWrap = document.getElementById('agentUploadProgressWrap');
const agentUploadProgressFill = document.getElementById('agentUploadProgressFill');
const agentUploadProgressLabel = document.getElementById('agentUploadProgressLabel');
const agentUploadProgressPercent = document.getElementById('agentUploadProgressPercent');
const agentBorrowerName = document.getElementById('agentBorrowerName');
const agentLeadReference = document.getElementById('agentLeadReference');
const agentSubmitBtn = document.getElementById('agentSubmitBtn');
const agentRefreshBtn = document.getElementById('agentRefreshBtn');
const agentSubmissionList = document.getElementById('agentSubmissionList');
const evaluatorRefreshBtn = document.getElementById('evaluatorRefreshBtn');
const evaluatorSearchInput = document.getElementById('evaluatorSearchInput');
const evaluatorSubmissionList = document.getElementById('evaluatorSubmissionList');
const evaluatorActionBar = document.getElementById('evaluatorActionBar');
const evaluatorStartBtn = document.getElementById('evaluatorStartBtn');
const evaluatorOpenBtn = document.getElementById('evaluatorOpenBtn');
const evaluatorSaveBtn = document.getElementById('evaluatorSaveBtn');
const evaluatorAnalyzeBtn = document.getElementById('evaluatorAnalyzeBtn');
const evaluatorReportBtn = document.getElementById('evaluatorReportBtn');
const evaluatorReadyBtn = document.getElementById('evaluatorReadyBtn');
const forcedPageRole = (document.body && document.body.dataset && document.body.dataset.pageRole) || 'all';

let selectedFile = null;
let currentJobId = null;
let pageList = [];
let parsedRows = [];
let rowsByPage = {};
let boundsByPage = {};
let pageRowToGlobal = {};
let identityBoundsByPage = {};
let currentPageIndex = 0;
let activeRowKey = null;
let shouldAutoScrollToResults = false;
let hasSeenInFlightStatus = false;
let rowKeyCounter = 1;
let flattenMode = false;
let flattenPoints = [];
let flattenBusy = false;
let ocrStarted = false;
let pageImageVersion = {};
let elapsedStartMs = 0;
let elapsedTimer = null;
let previewOverlayDataUrl = '';
const TABLE_EDIT_FIELDS = ['date', 'description', 'debit', 'credit', 'balance'];
let pendingFocusActiveRow = false;
const PREVIEW_ZOOM_MIN = 1;
const PREVIEW_ZOOM_MAX = 3.5;
const PREVIEW_ZOOM_STEP = 0.12;
let previewZoom = 1;
let previewPanX = 0;
let previewPanY = 0;
let isPreviewPanning = false;
let panStartX = 0;
let panStartY = 0;
let panOriginX = 0;
let panOriginY = 0;
const prefetchedPreviewSrcs = new Set();
let authToken = localStorage.getItem('auth_token') || '';
let authRole = localStorage.getItem('auth_role') || '';
let authUserEmail = localStorage.getItem('auth_email') || '';
let evaluatorSelectedSubmission = null;
let isAgentSubmitting = false;
let agentSubmissionsCache = [];
let agentSubmissionsPage = 1;
const AGENT_SUBMISSIONS_PAGE_SIZE = 15;
let evaluatorSubmissionsCache = [];
let evaluatorSubmissionsPage = 1;
const EVALUATOR_SUBMISSIONS_PAGE_SIZE = 15;

if ((forcedPageRole === 'agent' || forcedPageRole === 'credit_evaluator') && (!authToken || !authRole)) {
  window.location.href = '/login';
}
if (forcedPageRole === 'agent' && authRole && authRole !== 'agent') {
  if (authRole === 'credit_evaluator') {
    window.location.href = '/evaluator';
  } else if (authRole === 'admin') {
    window.location.href = '/admin';
  } else {
    window.location.href = '/login';
  }
}
if (forcedPageRole === 'credit_evaluator' && authRole && authRole !== 'credit_evaluator') {
  if (authRole === 'agent') {
    window.location.href = '/agent';
  } else if (authRole === 'admin') {
    window.location.href = '/admin';
  } else {
    window.location.href = '/login';
  }
}

function getSelectedParseMode() {
  return ocrToggle && ocrToggle.checked ? 'ocr' : 'text';
}

function syncModeToggleText() {
  if (!modeToggleText) return;
  modeToggleText.textContent = getSelectedParseMode().toUpperCase();
}

browseButton.addEventListener('click', (e) => {
  e.stopPropagation();
  openFilePicker();
});

if (ocrToggle) {
  ocrToggle.addEventListener('click', (e) => e.stopPropagation());
  ocrToggle.addEventListener('change', syncModeToggleText);
  const toggleWrap = ocrToggle.closest('.mode-toggle');
  if (toggleWrap) {
    toggleWrap.addEventListener('click', (e) => e.stopPropagation());
  }
}

syncModeToggleText();

function setLegacyEditorVisible(visible) {
  if (!legacyMain) return;
  legacyMain.style.display = visible ? '' : 'none';
}

async function fetchAuthed(url, opts = {}) {
  const headers = new Headers(opts.headers || {});
  if (authToken) headers.set('Authorization', `Bearer ${authToken}`);
  return fetch(url, { ...opts, headers });
}

function setAuthState(token, role, email) {
  authToken = token || '';
  authRole = role || '';
  authUserEmail = email || '';
  if (authToken) {
    localStorage.setItem('auth_token', authToken);
    localStorage.setItem('auth_role', authRole);
    localStorage.setItem('auth_email', authUserEmail);
  } else {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_role');
    localStorage.removeItem('auth_email');
  }
  updateWorkflowVisibility();
}

function updateWorkflowVisibility() {
  if (authUserBadge) {
    const roleLabel = String(authRole || '')
      .split('_')
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
    authUserBadge.textContent = authToken ? `${authUserEmail || 'User'} (${roleLabel || 'User'})` : 'Not logged in';
  }
  if (authLoginBtn) authLoginBtn.style.display = authToken ? 'none' : '';
  if (authLogoutBtn) authLogoutBtn.style.display = authToken ? '' : 'none';
  const allowAgentPanel = forcedPageRole === 'all' || forcedPageRole === 'agent';
  const allowEvaluatorPanel = forcedPageRole === 'all' || forcedPageRole === 'credit_evaluator';
  if (agentWorkflowPanel) {
    agentWorkflowPanel.style.display = allowAgentPanel && authRole === 'agent' ? '' : 'none';
  }
  if (evaluatorWorkflowPanel) {
    evaluatorWorkflowPanel.style.display = allowEvaluatorPanel && authRole === 'credit_evaluator' ? '' : 'none';
  }
  const showAgentMain = false;
  const showEvaluatorMain = authRole === 'credit_evaluator' && forcedPageRole === 'credit_evaluator' && Boolean(evaluatorSelectedSubmission);
  const showUnifiedMain = authRole === 'credit_evaluator' && forcedPageRole === 'all' && Boolean(evaluatorSelectedSubmission);
  setLegacyEditorVisible(showAgentMain || showEvaluatorMain || showUnifiedMain);
}

async function doLogin() {
  const email = (authEmail && authEmail.value || '').trim();
  const password = (authPassword && authPassword.value || '').trim();
  if (!email || !password) {
    alert('Enter email and password.');
    return;
  }
  const res = await fetch('/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Login failed');
    return;
  }
  const nextRole = body.role || '';
  if (nextRole === 'agent' && forcedPageRole === 'credit_evaluator') {
    window.location.href = '/agent';
    return;
  }
  if (nextRole === 'credit_evaluator' && forcedPageRole === 'agent') {
    window.location.href = '/evaluator';
    return;
  }
  if (nextRole === 'admin') {
    window.location.href = '/admin';
    return;
  }
  setAuthState(body.access_token || '', body.role || '', email);
  if (authRole === 'agent') {
    await loadAgentSubmissions();
  } else if (authRole === 'credit_evaluator') {
    await loadEvaluatorSubmissions();
  }
}

function doLogout() {
  evaluatorSelectedSubmission = null;
  setAuthState('', '', '');
  setLegacyEditorVisible(false);
  window.location.href = '/login';
}

function renderWorkflowItem(item, rightActionsHtml = '') {
  const title = item.lead_reference || item.borrower_name || item.id;
  const job = item.current_job_id ? `Job: ${item.current_job_id}` : 'No job';
  return `<div class="workflow-item">
    <div class="workflow-item-meta">
      <div class="workflow-item-title">${escapeHtml(title)}</div>
      <div class="workflow-item-sub">Status: ${escapeHtml(item.status || '-')} | ${escapeHtml(job)}</div>
    </div>
    <div class="workflow-item-actions">${rightActionsHtml}</div>
  </div>`;
}

function formatAgentSubmissionDate(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '-';
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function getSubmissionFilename(item) {
  const original = String((item && item.original_filename) || '').trim();
  if (original) return original;
  const key = String((item && item.input_pdf_key) || '').trim();
  if (!key) return '-';
  const parts = key.split('/').filter(Boolean);
  return parts.length ? parts[parts.length - 1] : key;
}

function renderAgentSubmissionTable(items) {
  if (!items.length) {
    return '<div class="table-empty">No submissions yet</div>';
  }
  const totalPages = Math.max(1, Math.ceil(items.length / AGENT_SUBMISSIONS_PAGE_SIZE));
  if (agentSubmissionsPage > totalPages) agentSubmissionsPage = totalPages;
  if (agentSubmissionsPage < 1) agentSubmissionsPage = 1;
  const start = (agentSubmissionsPage - 1) * AGENT_SUBMISSIONS_PAGE_SIZE;
  const pageItems = items.slice(start, start + AGENT_SUBMISSIONS_PAGE_SIZE);

  const rowHtml = pageItems.map((item) => {
    return `<tr>
      <td>${escapeHtml(formatAgentSubmissionDate(item.created_at))}</td>
      <td>${escapeHtml(getSubmissionFilename(item))}</td>
      <td>${escapeHtml(item.borrower_name || '-')}</td>
      <td>${escapeHtml(item.lead_reference || '-')}</td>
      <td class="agent-submission-id" title="${escapeHtml(item.id || '-')}">${escapeHtml(item.id || '-')}</td>
    </tr>`;
  });
  const fillerCount = Math.max(0, AGENT_SUBMISSIONS_PAGE_SIZE - pageItems.length);
  for (let i = 0; i < fillerCount; i += 1) {
    rowHtml.push('<tr class="agent-row-filler"><td colspan="5"></td></tr>');
  }
  const rows = rowHtml.join('');

  const pages = Array.from({ length: totalPages }, (_, idx) => idx + 1);
  const pageButtons = pages.map((p) => {
    const active = p === agentSubmissionsPage ? ' is-active' : '';
    return `<button class="agent-page-btn${active}" data-page="${p}">${p}</button>`;
  }).join('');

  return `<div class="agent-submissions-table-inner">
    <table class="agent-submissions-grid agent-submissions-grid-agent">
      <thead>
        <tr>
          <th>Date</th>
          <th>Filename</th>
          <th>Borrower Name</th>
          <th>Lead Reference</th>
          <th>Submission ID</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="agent-pagination">
      <button class="agent-page-btn" data-page-nav="prev" ${agentSubmissionsPage <= 1 ? 'disabled' : ''}>Prev</button>
      <div class="agent-page-list">${pageButtons}</div>
      <button class="agent-page-btn" data-page-nav="next" ${agentSubmissionsPage >= totalPages ? 'disabled' : ''}>Next</button>
    </div>
  </div>`;
}

function renderAgentSubmissionTableFiltered() {
  const query = (agentSearchInput && agentSearchInput.value || '').trim().toLowerCase();
  if (!query) {
    agentSubmissionList.innerHTML = renderAgentSubmissionTable(agentSubmissionsCache);
    return;
  }
  const filtered = agentSubmissionsCache.filter((item) => {
    const borrower = String(item.borrower_name || '').toLowerCase();
    return borrower.includes(query);
  });
  agentSubmissionList.innerHTML = renderAgentSubmissionTable(filtered);
}

function renderEvaluatorQueueRow(item) {
  const title = item.lead_reference || item.borrower_name || item.id;
  const status = String(item.status || '-');
  const statusLabel = formatSubmissionStatus(status);
  const statusClass = `workflow-status workflow-status-${status.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  const jobShort = item.current_job_id ? item.current_job_id.slice(0, 8) : '-';
  const assigned = Boolean(item.assigned_evaluator_id);
  const assignBtn = assigned ? '' : `<button class="preview-nav workflow-mini-btn" data-action="assign" data-id="${item.id}">Assign</button>`;
  const openBtn = assigned ? `<button class="preview-nav workflow-mini-btn" data-action="open" data-id="${item.id}">Open</button>` : '';
  return `<div class="workflow-queue-row">
    <div class="workflow-queue-col workflow-queue-col-title" title="${escapeHtml(item.id)}">${escapeHtml(title)}</div>
    <div class="workflow-queue-col"><span class="${statusClass}">${escapeHtml(statusLabel)}</span></div>
    <div class="workflow-queue-col workflow-queue-col-job" title="${escapeHtml(item.current_job_id || '-')}">${escapeHtml(jobShort)}</div>
    <div class="workflow-queue-col workflow-item-actions">${assignBtn}${openBtn}</div>
  </div>`;
}

function renderEvaluatorSubmissionTable(items) {
  if (!items.length) {
    return '<div class="table-empty">No submissions in queue</div>';
  }
  const totalPages = Math.max(1, Math.ceil(items.length / EVALUATOR_SUBMISSIONS_PAGE_SIZE));
  if (evaluatorSubmissionsPage > totalPages) evaluatorSubmissionsPage = totalPages;
  if (evaluatorSubmissionsPage < 1) evaluatorSubmissionsPage = 1;
  const start = (evaluatorSubmissionsPage - 1) * EVALUATOR_SUBMISSIONS_PAGE_SIZE;
  const pageItems = items.slice(start, start + EVALUATOR_SUBMISSIONS_PAGE_SIZE);

  const rowHtml = pageItems.map((item) => {
    const status = String(item.status || '-');
    const statusLabel = formatSubmissionStatus(status);
    const statusClass = `workflow-status workflow-status-${status.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
    const assignBtn = item.assigned_evaluator_id ? '' : `<button class="preview-nav workflow-mini-btn" data-action="assign" data-id="${item.id}">Assign</button>`;
    const openBtn = item.assigned_evaluator_id ? `<button class="preview-nav workflow-mini-btn" data-action="open" data-id="${item.id}">Open</button>` : '';
    const agentLabel = item.agent_email || item.agent_id || '-';
    return `<tr>
      <td>${escapeHtml(formatAgentSubmissionDate(item.created_at))}</td>
      <td title="${escapeHtml(agentLabel)}">${escapeHtml(agentLabel)}</td>
      <td>${escapeHtml(item.borrower_name || '-')}</td>
      <td>${escapeHtml(item.lead_reference || '-')}</td>
      <td class="agent-submission-id" title="${escapeHtml(item.id || '-')}">${escapeHtml(item.id || '-')}</td>
      <td><span class="${statusClass}">${escapeHtml(statusLabel)}</span></td>
      <td class="workflow-item-actions">${assignBtn}${openBtn}</td>
    </tr>`;
  });
  const fillerCount = Math.max(0, EVALUATOR_SUBMISSIONS_PAGE_SIZE - pageItems.length);
  for (let i = 0; i < fillerCount; i += 1) {
    rowHtml.push('<tr class="agent-row-filler"><td colspan="7"></td></tr>');
  }
  const rows = rowHtml.join('');

  const pages = Array.from({ length: totalPages }, (_, idx) => idx + 1);
  const pageButtons = pages.map((p) => {
    const active = p === evaluatorSubmissionsPage ? ' is-active' : '';
    return `<button class="agent-page-btn${active}" data-evaluator-page="${p}">${p}</button>`;
  }).join('');

  return `<div class="agent-submissions-table-inner">
    <table class="agent-submissions-grid agent-submissions-grid-evaluator">
      <thead>
        <tr>
          <th>Date</th>
          <th>Agent</th>
          <th>Borrower Name</th>
          <th>Lead Reference</th>
          <th>Submission ID</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="agent-pagination">
      <button class="agent-page-btn" data-evaluator-page-nav="prev" ${evaluatorSubmissionsPage <= 1 ? 'disabled' : ''}>Prev</button>
      <div class="agent-page-list">${pageButtons}</div>
      <button class="agent-page-btn" data-evaluator-page-nav="next" ${evaluatorSubmissionsPage >= totalPages ? 'disabled' : ''}>Next</button>
    </div>
  </div>`;
}

function renderEvaluatorSubmissionTableFiltered() {
  const query = (evaluatorSearchInput && evaluatorSearchInput.value || '').trim().toLowerCase();
  if (!query) {
    evaluatorSubmissionList.innerHTML = renderEvaluatorSubmissionTable(evaluatorSubmissionsCache);
    return;
  }
  const filtered = evaluatorSubmissionsCache.filter((item) => {
    const borrower = String(item.borrower_name || '').toLowerCase();
    return borrower.includes(query);
  });
  evaluatorSubmissionList.innerHTML = renderEvaluatorSubmissionTable(filtered);
}

async function loadAgentSubmissions() {
  if (!agentSubmissionList || !authToken) return;
  const res = await fetchAuthed('/agent/submissions');
  const body = await safeParseJson(res);
  if (!res.ok) {
    agentSubmissionList.innerHTML = `<div class="table-empty">${escapeHtml((body && body.detail) || 'Failed to load submissions')}</div>`;
    return;
  }
  agentSubmissionsCache = (body && body.items) || [];
  renderAgentSubmissionTableFiltered();
}

function getAgentSelectedFiles() {
  return agentFileInput && agentFileInput.files ? Array.from(agentFileInput.files) : [];
}

function updateAgentSubmitButtonState() {
  if (!agentSubmitBtn) return;
  if (isAgentSubmitting) return;
  const count = getAgentSelectedFiles().length;
  agentSubmitBtn.disabled = count === 0;
  agentSubmitBtn.textContent = count > 0 ? `Submit Files (${count})` : 'Submit Files (0)';
}

function setAgentUploadProgress(percent, label) {
  const safe = Math.max(0, Math.min(100, Math.round(percent)));
  if (agentUploadProgressFill) agentUploadProgressFill.style.width = `${safe}%`;
  if (agentUploadProgressPercent) agentUploadProgressPercent.textContent = `${safe}%`;
  if (agentUploadProgressLabel && label) agentUploadProgressLabel.textContent = label;
}

function setAgentSubmittingState(active) {
  isAgentSubmitting = active;
  if (agentSubmitBtn) {
    agentSubmitBtn.disabled = active || getAgentSelectedFiles().length === 0;
    agentSubmitBtn.classList.toggle('is-loading', active);
    if (active) {
      agentSubmitBtn.textContent = 'Submitting';
    } else {
      updateAgentSubmitButtonState();
    }
  }
  if (agentUploadProgressWrap) {
    agentUploadProgressWrap.classList.toggle('is-hidden', !active);
  }
  if (!active) {
    setAgentUploadProgress(0, 'Uploading...');
  }
}

function uploadAgentFile(file, parseMode, borrowerName, leadReference, onProgress) {
  return new Promise((resolve, reject) => {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('mode', parseMode);
    if (borrowerName) fd.append('borrower_name', borrowerName);
    if (leadReference) fd.append('lead_reference', leadReference);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/agent/submissions');
    if (authToken) xhr.setRequestHeader('Authorization', `Bearer ${authToken}`);
    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      const ratio = e.total > 0 ? (e.loaded / e.total) : 0;
      if (onProgress) onProgress(Math.max(0, Math.min(1, ratio)));
    };
    xhr.onerror = () => reject(new Error('Network error during upload'));
    xhr.onload = () => {
      let body = null;
      try {
        body = xhr.responseText ? JSON.parse(xhr.responseText) : null;
      } catch (_err) {
        body = null;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(body || {});
      } else {
        reject(new Error((body && body.detail) || `Upload failed for ${file.name}`));
      }
    };
    xhr.send(fd);
  });
}

function setAgentSelectedFiles(files) {
  if (!agentFileInput) return;
  const dt = new DataTransfer();
  files.forEach((f) => dt.items.add(f));
  agentFileInput.files = dt.files;
}

function renderAgentSelectedFiles() {
  if (!agentSelectedFiles) {
    updateAgentSubmitButtonState();
    return;
  }
  const files = getAgentSelectedFiles();
  if (!files.length) {
    agentSelectedFiles.innerHTML = '<div class="agent-files-empty">No file selected</div>';
    updateAgentSubmitButtonState();
    return;
  }
  agentSelectedFiles.innerHTML = files.map((file, idx) => `
    <div class="agent-file-item">
      <i class="far fa-file-pdf"></i>
      <div class="agent-file-meta">
        <div class="agent-file-name">${escapeHtml(file.name)}</div>
        <div class="agent-file-size">${formatFileSize(file.size)}</div>
      </div>
      <button class="agent-file-remove" data-index="${idx}" aria-label="Remove file">×</button>
    </div>
  `).join('');
  updateAgentSubmitButtonState();
}

async function submitAgentFile() {
  const files = getAgentSelectedFiles();
  if (!files.length) {
    alert('Select at least one PDF first.');
    return;
  }

  const borrowerName = agentBorrowerName && agentBorrowerName.value.trim() ? agentBorrowerName.value.trim() : '';
  const leadReference = agentLeadReference && agentLeadReference.value.trim() ? agentLeadReference.value.trim() : '';
  const parseMode = getSelectedParseMode();

  setAgentSubmittingState(true);
  let submitted = 0;
  try {
    const total = files.length;
    for (let i = 0; i < total; i += 1) {
      const file = files[i];
      await uploadAgentFile(file, parseMode, borrowerName, leadReference, (singleRatio) => {
        const overall = ((i + singleRatio) / total) * 100;
        setAgentUploadProgress(overall, `Uploading ${i + 1}/${total}`);
      });
      submitted += 1;
      setAgentUploadProgress(((i + 1) / total) * 100, `Uploading ${i + 1}/${total}`);
    }
  } catch (err) {
    setAgentSubmittingState(false);
    alert(err.message || 'Submission failed');
    return;
  }

  agentFileInput.value = '';
  renderAgentSelectedFiles();
  if (agentBorrowerName) agentBorrowerName.value = '';
  if (agentLeadReference) agentLeadReference.value = '';
  setAgentSubmittingState(false);
  await loadAgentSubmissions();
  alert(`Submitted ${submitted} file(s).`);
}

async function loadEvaluatorSubmissions() {
  if (!evaluatorSubmissionList || !authToken) return;
  const res = await fetchAuthed('/evaluator/submissions?include_unassigned=true');
  const body = await safeParseJson(res);
  if (!res.ok) {
    evaluatorSubmissionList.innerHTML = `<div class="table-empty">${escapeHtml((body && body.detail) || 'Failed to load queue')}</div>`;
    return;
  }
  evaluatorSubmissionsCache = (body && body.items) || [];
  renderEvaluatorSubmissionTableFiltered();
}

async function assignEvaluatorSubmission(submissionId) {
  const res = await fetchAuthed(`/evaluator/submissions/${submissionId}/assign`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Assign failed');
    return;
  }
  await loadEvaluatorSubmissions();
}

async function openEvaluatorSubmission(submissionId) {
  const res = await fetchAuthed(`/evaluator/submissions/${submissionId}`);
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Open failed');
    return;
  }
  evaluatorSelectedSubmission = body.submission || null;
  if (!evaluatorSelectedSubmission) return;
  updateWorkflowVisibility();
  scrollToResults();
  currentJobId = evaluatorSelectedSubmission.current_job_id;
  if (!currentJobId) {
    parsedRows = (body.transactions || []).map((row, idx) => ({
      row_key: createRowKey(),
      global_row_id: String(idx + 1).padStart(3, '0'),
      row_id: String(idx + 1).padStart(3, '0'),
      date: row.date || '',
      description: row.description || '',
      debit: row.debit != null ? String(row.debit) : '',
      credit: row.credit != null ? String(row.credit) : '',
      balance: row.balance != null ? String(row.balance) : '',
      page: row.page || '',
      page_row_id: '',
    }));
    renderRows(parsedRows);
    return;
  }

  const shouldStartProcessing = String(evaluatorSelectedSubmission.status || '').toLowerCase() === 'for_review';
  if (shouldStartProcessing) {
    const startRes = await fetch(`/jobs/${currentJobId}/start`, { method: 'POST' });
    const startBody = await safeParseJson(startRes);
    if (!startRes.ok) {
      alert((startBody && startBody.detail) || 'Failed to start processing');
      return;
    }
    await loadEvaluatorSubmissions();
    updateProgressUI(0, 'Processing started', 'processing');
    startElapsedTimer();
    try {
      await pollJobUntilDone();
    } finally {
      stopElapsedTimer();
    }
    return;
  }

  const statusRes = await fetch(`/jobs/${currentJobId}`);
  const statusBody = await safeParseJson(statusRes);
  if (statusRes.ok && statusBody && statusBody.status === 'done') {
    stopElapsedTimer();
    await loadResults();
  } else if (statusRes.ok && statusBody && statusBody.status === 'failed') {
    stopElapsedTimer();
    alert('Job failed. Check diagnostics.');
  } else if (statusRes.ok && statusBody && statusBody.status === 'processing') {
    startElapsedTimer();
    try {
      await pollJobUntilDone();
    } finally {
      stopElapsedTimer();
    }
  } else {
    const startRes = await fetch(`/jobs/${currentJobId}/start`, { method: 'POST' });
    const startBody = await safeParseJson(startRes);
    if (!startRes.ok) {
      alert((startBody && startBody.detail) || 'Failed to start processing');
      return;
    }
    await loadEvaluatorSubmissions();
    updateProgressUI(0, 'Processing started', 'processing');
    startElapsedTimer();
    try {
      await pollJobUntilDone();
    } finally {
      stopElapsedTimer();
    }
  }
}

async function runEvaluatorStartProcessing() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.current_job_id) {
    alert('Open a submission first.');
    return;
  }
  currentJobId = evaluatorSelectedSubmission.current_job_id;
  const res = await fetch(`/jobs/${currentJobId}/start`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to start processing');
    return;
  }
  startElapsedTimer();
  try {
    await pollJobUntilDone();
  } finally {
    stopElapsedTimer();
  }
}

async function saveEvaluatorEdits() {
  if (!evaluatorSelectedSubmission) {
    alert('Open a submission first.');
    return;
  }
  const rows = parsedRows.map((r, idx) => ({
    row_id: r.row_id || String(idx + 1).padStart(3, '0'),
    page: r.page || currentPageKey(),
    date: r.date || '',
    description: r.description || '',
    debit: r.debit || '',
    credit: r.credit || '',
    balance: r.balance || '',
  }));
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/transactions`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rows }),
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Save failed');
    return;
  }
  alert('Edits saved.');
}

async function runEvaluatorAnalyze() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/analyze`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Analyze failed');
    return;
  }
  alert('Summary recomputed.');
}

async function runEvaluatorReport() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/reports`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Report generation failed');
    return;
  }
  if (body.download_url) {
    window.open(body.download_url, '_blank');
  }
}

async function runEvaluatorSummaryReady() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/mark-summary-ready`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to mark summary ready');
    return;
  }
  evaluatorSelectedSubmission = body;
  await loadEvaluatorSubmissions();
  alert('Marked as summary_ready.');
}

if (authLoginBtn) authLoginBtn.addEventListener('click', doLogin);
if (authLogoutBtn) authLogoutBtn.addEventListener('click', doLogout);
if (agentSubmitBtn) agentSubmitBtn.addEventListener('click', submitAgentFile);
if (agentRefreshBtn) agentRefreshBtn.addEventListener('click', loadAgentSubmissions);
if (agentBrowseButton && agentFileInput) {
  agentBrowseButton.addEventListener('click', () => {
    agentFileInput.click();
  });
}
if (agentFileInput) {
  agentFileInput.addEventListener('change', () => {
    renderAgentSelectedFiles();
  });
}
if (agentSearchInput) {
  agentSearchInput.addEventListener('input', () => {
    agentSubmissionsPage = 1;
    renderAgentSubmissionTableFiltered();
  });
}
if (evaluatorSearchInput) {
  evaluatorSearchInput.addEventListener('input', () => {
    evaluatorSubmissionsPage = 1;
    renderEvaluatorSubmissionTableFiltered();
  });
}
if (agentSubmissionList) {
  agentSubmissionList.addEventListener('click', (e) => {
    const pageBtn = e.target.closest('button.agent-page-btn');
    if (!pageBtn) return;
    const nav = pageBtn.dataset.pageNav || '';
    if (nav) {
      if (nav === 'prev') agentSubmissionsPage -= 1;
      if (nav === 'next') agentSubmissionsPage += 1;
      renderAgentSubmissionTableFiltered();
      return;
    }
    const page = Number.parseInt(pageBtn.dataset.page || '', 10);
    if (Number.isFinite(page) && page > 0) {
      agentSubmissionsPage = page;
      renderAgentSubmissionTableFiltered();
    }
  });
}
if (agentSelectedFiles) {
  agentSelectedFiles.addEventListener('click', (e) => {
    const btn = e.target.closest('.agent-file-remove');
    if (!btn) return;
    const idx = Number.parseInt(btn.dataset.index || '-1', 10);
    if (!Number.isFinite(idx) || idx < 0) return;
    const files = getAgentSelectedFiles();
    files.splice(idx, 1);
    setAgentSelectedFiles(files);
    renderAgentSelectedFiles();
  });
}
if (agentDropZone && agentFileInput) {
  agentDropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    agentDropZone.classList.add('dragover');
  });
  agentDropZone.addEventListener('dragleave', () => {
    agentDropZone.classList.remove('dragover');
  });
  agentDropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    agentDropZone.classList.remove('dragover');
    const dropped = e.dataTransfer && e.dataTransfer.files ? Array.from(e.dataTransfer.files) : [];
    if (!dropped.length) return;
    const existing = getAgentSelectedFiles();
    const nextFiles = existing.concat(dropped.filter((f) => /\.pdf$/i.test(f.name)));
    setAgentSelectedFiles(nextFiles);
    renderAgentSelectedFiles();
  });
}
renderAgentSelectedFiles();
updateAgentSubmitButtonState();
if (evaluatorRefreshBtn) evaluatorRefreshBtn.addEventListener('click', loadEvaluatorSubmissions);
if (evaluatorSubmissionList) {
  evaluatorSubmissionList.addEventListener('click', async (e) => {
    const pageBtn = e.target.closest('button.agent-page-btn');
    if (pageBtn) {
      const nav = pageBtn.dataset.evaluatorPageNav || '';
      if (nav) {
        if (nav === 'prev') evaluatorSubmissionsPage -= 1;
        if (nav === 'next') evaluatorSubmissionsPage += 1;
        renderEvaluatorSubmissionTableFiltered();
        return;
      }
      const page = Number.parseInt(pageBtn.dataset.evaluatorPage || '', 10);
      if (Number.isFinite(page) && page > 0) {
        evaluatorSubmissionsPage = page;
        renderEvaluatorSubmissionTableFiltered();
      }
      return;
    }
    const btn = e.target.closest('button[data-action]');
    if (!btn) return;
    const submissionId = btn.dataset.id;
    const action = btn.dataset.action;
    if (!submissionId || !action) return;
    if (action === 'assign') {
      await assignEvaluatorSubmission(submissionId);
      return;
    }
    if (action === 'open') {
      await openEvaluatorSubmission(submissionId);
    }
  });
}

if (authToken && authRole) {
  if (forcedPageRole === 'agent' && authRole !== 'agent') {
    if (authRole === 'credit_evaluator') {
      window.location.href = '/evaluator';
    } else {
      window.location.href = '/admin';
    }
  }
  if (forcedPageRole === 'credit_evaluator' && authRole !== 'credit_evaluator') {
    if (authRole === 'agent') {
      window.location.href = '/agent';
    } else {
      window.location.href = '/admin';
    }
  }
  updateWorkflowVisibility();
  if (authRole === 'agent') {
    loadAgentSubmissions();
  } else if (authRole === 'credit_evaluator') {
    loadEvaluatorSubmissions();
  }
} else {
  updateWorkflowVisibility();
  setLegacyEditorVisible(false);
}

fileInput.addEventListener('change', () => {
  if (!fileInput.files || !fileInput.files[0]) return;
  handleFileSelection(fileInput.files[0]);
});

uploadArea.addEventListener('dragover', (e) => {
  e.preventDefault();
  uploadArea.classList.add('dragover');
});

uploadArea.addEventListener('dragleave', () => {
  uploadArea.classList.remove('dragover');
});

uploadArea.addEventListener('drop', (e) => {
  e.preventDefault();
  uploadArea.classList.remove('dragover');
  if (!e.dataTransfer.files || !e.dataTransfer.files[0]) return;

  const file = e.dataTransfer.files[0];
  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(file);
  fileInput.files = dataTransfer.files;
  handleFileSelection(file);
});

removeFileBtn.addEventListener('click', () => {
  selectedFile = null;
  currentJobId = null;
  fileInput.value = '';
  fileInfo.style.display = 'none';
  shouldAutoScrollToResults = false;
  resetProgressUI();
  resetResults();
});

viewDetails.addEventListener('click', () => {
  if (!currentJobId) {
    alert('Upload a statement first.');
    return;
  }
  alert(`OCR job ID: ${currentJobId}`);
});

finishSave.addEventListener('click', () => {
  if (!currentJobId) {
    alert('Upload a statement first.');
    return;
  }
  if (!ocrStarted) {
    startProcessingFromDraft();
    return;
  }
  if (!parsedRows.length) {
    alert('OCR is still running or no rows extracted yet.');
    return;
  }
  alert('OCR complete. You can review rows and download CSV.');
});

if (downloadCSV) {
  downloadCSV.addEventListener('click', async () => {
    await exportToPdf();
  });
}

if (exportExcelBtn) {
  exportExcelBtn.addEventListener('click', async () => {
    await exportToExcel();
  });
}

prevPageBtn.addEventListener('click', () => {
  if (currentPageIndex <= 0) return;
  currentPageIndex -= 1;
  renderCurrentPage();
});

nextPageBtn.addEventListener('click', () => {
  if (currentPageIndex >= pageList.length - 1) return;
  currentPageIndex += 1;
  renderCurrentPage();
});

if (pageSelect) {
  pageSelect.addEventListener('change', () => {
    const idx = Number.parseInt(pageSelect.value, 10);
    if (!Number.isFinite(idx) || idx < 0 || idx >= pageList.length) return;
    currentPageIndex = idx;
    renderCurrentPage();
  });
}

previewImage.addEventListener('load', () => {
  previewImage.style.display = 'block';
  resetPreviewTransform();
  drawBoundingBoxes();
  maybeFocusActiveRowInPreview();
});

previewImage.addEventListener('error', () => {
  setPreviewEmptyState();
});

window.addEventListener('resize', () => {
  if (pageList.length) {
    drawBoundingBoxes();
  }
  clampPreviewPan();
  applyPreviewTransform();
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.row-menu')) {
    closeAllRowMenus();
  }
});

previewCanvas.addEventListener('click', (e) => {
  if (!flattenMode || flattenBusy) return;
  if (!previewImage.naturalWidth || !previewWrap) return;

  const wrapRect = previewWrap.getBoundingClientRect();
  const baseRect = getRenderedImageRect(previewImage);
  const baseLeft = baseRect.left - wrapRect.left;
  const baseTop = baseRect.top - wrapRect.top;
  const baseW = baseRect.width;
  const baseH = baseRect.height;
  if (baseW <= 0 || baseH <= 0) return;

  const localX = e.clientX - wrapRect.left;
  const localY = e.clientY - wrapRect.top;
  const cx = baseLeft + (baseW / 2) + previewPanX;
  const cy = baseTop + (baseH / 2) + previewPanY;
  const xOnBase = ((localX - cx) / previewZoom) + (baseW / 2);
  const yOnBase = ((localY - cy) / previewZoom) + (baseH / 2);
  const x = xOnBase / baseW;
  const y = yOnBase / baseH;
  if (x < 0 || x > 1 || y < 0 || y > 1) return;

  if (flattenPoints.length >= 4) return;
  flattenPoints.push({ x, y });
  updateFlattenButtons();
  drawBoundingBoxes();
});

if (previewWrap) {
  previewWrap.addEventListener('mousemove', (e) => {
    if (isPreviewPanning) {
      const dx = e.clientX - panStartX;
      const dy = e.clientY - panStartY;
      previewPanX = panOriginX + dx;
      previewPanY = panOriginY + dy;
      clampPreviewPan();
      applyPreviewTransform();
      drawBoundingBoxes();
      return;
    }
  });

  previewWrap.addEventListener('mouseleave', () => {
    stopPreviewPan();
  });

  previewWrap.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    if (flattenMode || !pageList.length) return;
    isPreviewPanning = true;
    panStartX = e.clientX;
    panStartY = e.clientY;
    panOriginX = previewPanX;
    panOriginY = previewPanY;
    previewWrap.classList.add('panning');
    e.preventDefault();
  });

  previewWrap.addEventListener('wheel', (e) => {
    if (flattenMode || !pageList.length || !previewImage.naturalWidth) return;
    e.preventDefault();
    const direction = e.deltaY < 0 ? 1 : -1;
    stepPreviewZoom(direction * PREVIEW_ZOOM_STEP);
  }, { passive: false });
}

window.addEventListener('mouseup', () => {
  stopPreviewPan();
});

if (resetViewBtn) {
  resetViewBtn.addEventListener('click', () => {
    resetPreviewTransform();
  });
}

if (zoomInBtn) {
  zoomInBtn.addEventListener('click', () => {
    stepPreviewZoom(PREVIEW_ZOOM_STEP);
  });
}

if (zoomOutBtn) {
  zoomOutBtn.addEventListener('click', () => {
    stepPreviewZoom(-PREVIEW_ZOOM_STEP);
  });
}

if (flattenModeBtn) {
  flattenModeBtn.addEventListener('click', () => {
    if (!pageList.length || flattenBusy) return;
    flattenMode = !flattenMode;
    if (flattenMode) {
      resetPreviewTransform();
    }
    flattenPoints = [];
    updateFlattenButtons();
    drawBoundingBoxes();
  });
}

if (applyFlattenBtn) {
  applyFlattenBtn.addEventListener('click', async () => {
    if (!currentJobId || !pageList.length || flattenBusy) return;
    if (flattenPoints.length !== 4) {
      alert('Select exactly 4 corner points first.');
      return;
    }
    try {
      flattenBusy = true;
      updateFlattenButtons();
      const pageFile = pageList[currentPageIndex];
      const pageKey = pageFile.replace('.png', '');

      const res = await fetch(`/jobs/${currentJobId}/pages/${pageKey}/flatten`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ points: flattenPoints }),
      });
      if (!res.ok) {
        const err = await safeParseJson(res);
        throw new Error((err && err.detail) || 'Flatten failed');
      }

      await refreshCurrentPageData(pageKey);
      flattenPoints = [];
      flattenMode = false;
      updateFlattenButtons();
      renderCurrentPage();
    } catch (err) {
      alert(err.message || 'Flatten failed');
    } finally {
      flattenBusy = false;
      updateFlattenButtons();
    }
  });
}

if (resetFlattenBtn) {
  resetFlattenBtn.addEventListener('click', async () => {
    if (!currentJobId || !pageList.length || flattenBusy) return;
    try {
      flattenBusy = true;
      updateFlattenButtons();
      const pageFile = pageList[currentPageIndex];
      const pageKey = pageFile.replace('.png', '');

      const res = await fetch(`/jobs/${currentJobId}/pages/${pageKey}/flatten/reset`, {
        method: 'POST',
      });
      if (!res.ok) {
        const err = await safeParseJson(res);
        throw new Error((err && err.detail) || 'Reset failed');
      }

      await refreshCurrentPageData(pageKey);
      flattenPoints = [];
      flattenMode = false;
      updateFlattenButtons();
      renderCurrentPage();
    } catch (err) {
      alert(err.message || 'Reset failed');
    } finally {
      flattenBusy = false;
      updateFlattenButtons();
    }
  });
}

function handleFileSelection(file) {
  if (!file.name.toLowerCase().endsWith('.pdf')) {
    alert('Please upload a PDF file.');
    return;
  }

  selectedFile = file;
  fileName.textContent = file.name;
  fileSize.textContent = formatFileSize(file.size);
  fileInfo.style.display = 'flex';
  resetResults();
  createDraftJob();
}

async function createDraftJob() {
  if (!selectedFile) return;

  shouldAutoScrollToResults = false;
  hasSeenInFlightStatus = false;
  ocrStarted = false;
  finishSave.textContent = 'Start OCR';
  startElapsedTimer();
  updateProgressUI(4, 'Uploading file...', 'uploading');

  try {
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('mode', getSelectedParseMode());

    const res = await fetch('/jobs', { method: 'POST', body: formData });
    if (!res.ok) {
      const error = await safeParseJson(res);
      throw new Error((error && error.detail) || 'Failed to upload file');
    }

    const data = await res.json();
    currentJobId = data.job_id;
    ocrStarted = true;
    shouldAutoScrollToResults = true;
    hasSeenInFlightStatus = false;
    finishSave.textContent = 'Running OCR...';
    await pollJobUntilDone();
    stopElapsedTimer();
    finishSave.textContent = 'OCR Done';
  } catch (err) {
    stopElapsedTimer();
    ocrStarted = false;
    finishSave.textContent = 'Start OCR';
    updateProgressUI(0, err.message || 'Upload failed', 'failed');
    alert(err.message || 'Upload failed.');
  }
}

async function pollDraftUntilReady() {
  if (!currentJobId) return;

  while (true) {
    const res = await fetch(`/jobs/${currentJobId}`);
    if (!res.ok) {
      throw new Error('Failed to read draft status');
    }

    const status = await res.json();
    const step = status.step || status.status || 'draft_queued';
    const progress = Number.isFinite(status.progress) ? status.progress : inferProgress(status.status, step);
    const currentOcrModel = status.ocr_backend || null;
    const currentMode = status.parse_mode || getSelectedParseMode();

    if (status.status === 'failed') {
      throw new Error(status.message || 'Draft preparation failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel, currentMode, status);

    if (status.status === 'draft' && step === 'ready_for_edit') {
      stopElapsedTimer();
      await loadDraftPages();
      updateProgressUI(100, 'Edit pages then click Start OCR', 'ready_for_edit', currentOcrModel, currentMode, status);
      return;
    }

    await sleep(900);
  }
}

async function loadDraftPages() {
  const cleanedRes = await fetch(`/jobs/${currentJobId}/cleaned`);
  if (!cleanedRes.ok) throw new Error('Failed to load draft pages');

  const cleanedData = await cleanedRes.json();
  pageList = cleanedData.pages || [];
  currentPageIndex = 0;
  pageImageVersion = {};
  pageList.forEach((fileName) => {
    pageImageVersion[fileName.replace('.png', '')] = 0;
  });
  prefetchedPreviewSrcs.clear();
  renderRows([]);
  renderCurrentPage();
}

async function startProcessingFromDraft() {
  if (!currentJobId || ocrStarted) return;
  try {
    ocrStarted = true;
    shouldAutoScrollToResults = true;
    hasSeenInFlightStatus = false;
    finishSave.textContent = 'Running OCR...';
    startElapsedTimer();

    const res = await fetch(`/jobs/${currentJobId}/start`, { method: 'POST' });
    if (!res.ok) {
      const error = await safeParseJson(res);
      throw new Error((error && error.detail) || 'Failed to start OCR');
    }
    await pollJobUntilDone();
    stopElapsedTimer();
    finishSave.textContent = 'OCR Done';
  } catch (err) {
    stopElapsedTimer();
    ocrStarted = false;
    finishSave.textContent = 'Start OCR';
    updateProgressUI(0, err.message || 'Failed to start OCR', 'failed');
    alert(err.message || 'Failed to start OCR');
  }
}

async function pollJobUntilDone() {
  if (!currentJobId) return;

  while (true) {
    const res = await fetch(`/jobs/${currentJobId}`);
    if (!res.ok) throw new Error('Failed to read job status');

    const status = await res.json();
    const step = status.step || status.status || 'processing';
    const progress = Number.isFinite(status.progress) ? status.progress : inferProgress(status.status, step);
    const currentOcrModel = status.ocr_backend || null;
    const currentMode = status.parse_mode || getSelectedParseMode();
    if (status.status === 'queued' || status.status === 'processing') {
      hasSeenInFlightStatus = true;
    }

    if (status.status === 'done') {
      updateProgressUI(100, 'Results ready', step, currentOcrModel, currentMode, status);
      await loadResults();
      if (shouldAutoScrollToResults && hasSeenInFlightStatus) {
        shouldAutoScrollToResults = false;
        hasSeenInFlightStatus = false;
        scrollToResults();
      }
      return;
    }

    if (status.status === 'failed') {
      updateProgressUI(progress, status.message || 'OCR job failed', step, currentOcrModel, currentMode, status);
      throw new Error(status.message || 'OCR job failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel, currentMode, status);
    await sleep(1200);
  }
}

async function loadResults() {
  const cleanedRes = await fetch(`/jobs/${currentJobId}/cleaned`);
  if (!cleanedRes.ok) throw new Error('Failed to read processed pages');

  const cleanedData = await cleanedRes.json();
  pageList = cleanedData.pages || [];

  rowsByPage = {};
  boundsByPage = {};
  pageRowToGlobal = {};
  identityBoundsByPage = {};
  parsedRows = [];
  activeRowKey = null;
  currentPageIndex = 0;
  rowKeyCounter = 1;
  pageImageVersion = {};
  prefetchedPreviewSrcs.clear();

  if (!pageList.length) {
    renderRows([]);
    renderCurrentPage();
    return;
  }

  let globalCounter = 1;
  let parsedAll = null;
  let boundsAll = null;

  try {
    const [parsedAllRes, boundsAllRes] = await Promise.all([
      fetch(`/jobs/${currentJobId}/parsed`),
      fetch(`/jobs/${currentJobId}/bounds`)
    ]);
    if (parsedAllRes.ok) parsedAll = await parsedAllRes.json();
    if (boundsAllRes.ok) boundsAll = await boundsAllRes.json();
  } catch (_) {
    parsedAll = null;
    boundsAll = null;
  }

  for (let i = 0; i < pageList.length; i += 1) {
    const pageKey = pageList[i].replace('.png', '');
    let rows = parsedAll && Array.isArray(parsedAll[pageKey]) ? parsedAll[pageKey] : null;
    let bounds = boundsAll && Array.isArray(boundsAll[pageKey]) ? boundsAll[pageKey] : null;

    if (!rows) {
      const parsedRes = await fetch(`/jobs/${currentJobId}/parsed/${pageKey}`);
      if (!parsedRes.ok) {
        throw new Error(`Failed to read parsed rows for ${pageKey}`);
      }
      rows = await parsedRes.json();
    }
    if (!bounds) {
      const boundsRes = await fetch(`/jobs/${currentJobId}/rows/${pageKey}/bounds`);
      bounds = boundsRes.ok ? await boundsRes.json() : [];
    }

    rowsByPage[pageKey] = Array.isArray(rows) ? rows : [];
    boundsByPage[pageKey] = Array.isArray(bounds) ? bounds : [];

    rowsByPage[pageKey].forEach((row) => {
      const globalId = String(globalCounter).padStart(3, '0');
      globalCounter += 1;

      const normalizedRow = {
        ...row,
        row_key: createRowKey(),
        global_row_id: globalId,
        page: pageKey,
        page_row_id: row.row_id
      };
      normalizeRowDisplayValues(normalizedRow);
      parsedRows.push(normalizedRow);

      pageRowToGlobal[`${pageKey}|${row.row_id}`] = globalId;
    });
  }

  rebuildPageRowMap();
  renderRows(parsedRows);
  flattenMode = false;
  flattenPoints = [];
  updateFlattenButtons();
  renderCurrentPage();
  await loadAccountSummary();
  drawBoundingBoxes();
}

async function loadAccountSummary() {
  if (!accountNameSummary || !accountNumberSummary || !currentJobId) return;
  accountNameSummary.textContent = '-';
  accountNumberSummary.textContent = '-';
  try {
    let job = null;
    const identityRes = await fetch(`/jobs/${currentJobId}/account-identity`);
    if (identityRes.ok) {
      job = await identityRes.json();
    } else {
      const res = await fetch(`/jobs/${currentJobId}/diagnostics`);
      if (!res.ok) return;
      const diagnostics = await res.json();
      job = diagnostics && diagnostics.job ? diagnostics.job : {};
    }
    if (!job) return;
    accountNameSummary.textContent = (job.account_name || '-').toString();
    accountNumberSummary.textContent = (job.account_number || '-').toString();
    identityBoundsByPage = {};
    const nameBox = job.account_name_bbox;
    const numberBox = job.account_number_bbox;
    if (nameBox && nameBox.page) {
      identityBoundsByPage[nameBox.page] = identityBoundsByPage[nameBox.page] || [];
      identityBoundsByPage[nameBox.page].push({ ...nameBox, kind: 'account_name' });
    }
    if (numberBox && numberBox.page) {
      identityBoundsByPage[numberBox.page] = identityBoundsByPage[numberBox.page] || [];
      identityBoundsByPage[numberBox.page].push({ ...numberBox, kind: 'account_number' });
    }
  } catch (_) {
    accountNameSummary.textContent = '-';
    accountNumberSummary.textContent = '-';
    identityBoundsByPage = {};
  }
}

function renderRows(rows) {
  tableBody.innerHTML = '';

  if (!rows.length) {
    tableBody.innerHTML = '<div class="table-empty">No transactions extracted</div>';
    totalTransactions.textContent = '0';
    totalDebitTransactions.textContent = '0';
    totalCreditTransactions.textContent = '0';
    endingBalance.textContent = '-';
    return;
  }

  rows.forEach((row) => {
    const el = document.createElement('div');
    el.className = 'table-row';
    el.dataset.rowKey = row.row_key;
    el.dataset.page = row.page;
    el.dataset.pageRowId = row.page_row_id || '';
    el.innerHTML = '';

    const rowNoCell = document.createElement('div');
    rowNoCell.className = 'table-cell';
    rowNoCell.textContent = row.global_row_id || '';
    el.appendChild(rowNoCell);

    el.appendChild(makeEditableCell(row, 'date'));
    el.appendChild(makeEditableCell(row, 'description'));
    el.appendChild(makeEditableCell(row, 'debit', true));
    el.appendChild(makeEditableCell(row, 'credit', true));
    el.appendChild(makeEditableCell(row, 'balance', true));

    const actionsCell = document.createElement('div');
    actionsCell.className = 'table-cell table-actions';
    actionsCell.appendChild(makeRowActionsMenu(row.row_key));
    el.appendChild(actionsCell);

    el.addEventListener('click', () => {
      selectRow(row.row_key);
    });

    tableBody.appendChild(el);
  });

  updateSummaryFromRows(rows);
}

function selectRow(rowKey) {
  activeRowKey = rowKey;
  const row = parsedRows.find((r) => r.row_key === rowKey);
  if (!row) return;

  const pagePos = pageList.findIndex((p) => p.replace('.png', '') === row.page);
  if (pagePos >= 0) {
    currentPageIndex = pagePos;
  }

  highlightSelectedTableRow();
  renderCurrentPage();
}

function highlightSelectedTableRow() {
  document.querySelectorAll('.table-row').forEach((rowEl) => {
    const isActive = rowEl.dataset.rowKey === activeRowKey;
    rowEl.classList.toggle('selected', isActive);
  });
}

function renderCurrentPage() {
  if (!pageList.length) {
    pageIndicator.textContent = 'Page 0 / 0';
    prevPageBtn.disabled = true;
    nextPageBtn.disabled = true;
    syncPageSelect();
    setPreviewEmptyState();
    return;
  }

  const pageFile = pageList[currentPageIndex];
  const pageKey = pageFile.replace('.png', '');
  pageIndicator.textContent = `Page ${currentPageIndex + 1} / ${pageList.length}`;
  prevPageBtn.disabled = currentPageIndex === 0;
  nextPageBtn.disabled = currentPageIndex >= pageList.length - 1;
  syncPageSelect();

  const src = `/jobs/${currentJobId}/preview/${pageKey}?v=${pageImageVersion[pageKey] || 0}`;
  if (previewImage.dataset.src !== src) {
    resetPreviewTransform();
    previewImage.style.display = 'none';
    previewImage.dataset.src = src;
    previewImage.src = src;
    if (previewImage.complete && previewImage.naturalWidth > 0) {
      previewImage.style.display = 'block';
      drawBoundingBoxes();
      applyPreviewTransform();
    }
  } else {
    previewImage.style.display = 'block';
    drawBoundingBoxes();
    applyPreviewTransform();
  }

  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (activeRow && activeRow.page !== pageKey) {
    highlightSelectedTableRow();
  }
  prefetchPreviewNeighbors();
  updateFlattenButtons();
}

function maybeFocusActiveRowInPreview() {
  // Disabled: selecting a table row should highlight only, without zoom/pan.
}

function focusPreviewOnActiveRow() {
  const pageFile = pageList[currentPageIndex];
  if (!pageFile || !previewWrap || !previewImage.naturalWidth) return false;

  const pageKey = pageFile.replace('.png', '');
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (!activeRow || activeRow.page !== pageKey || !activeRow.global_row_id) return false;

  const bounds = boundsByPage[pageKey] || [];
  const activeBound = bounds.find((b) => pageRowToGlobal[`${pageKey}|${b.row_id}`] === activeRow.global_row_id);
  if (!activeBound) return false;

  const fillZoom = getFillPreviewZoom();
  const minFocusZoom = Math.min(PREVIEW_ZOOM_MAX, fillZoom + 0.35);
  if (previewZoom < minFocusZoom) {
    previewZoom = minFocusZoom;
  }

  const rect = getRenderedImageRect(previewImage);
  const wrapW = previewWrap.clientWidth || 1;
  const wrapH = previewWrap.clientHeight || 1;
  if (rect.width <= 0 || rect.height <= 0) return false;

  const wrapRect = previewWrap.getBoundingClientRect();
  const baseLeft = rect.left - wrapRect.left;
  const baseTop = rect.top - wrapRect.top;

  const rowCx = ((Number(activeBound.x1) + Number(activeBound.x2)) / 2) * rect.width;
  const rowCy = ((Number(activeBound.y1) + Number(activeBound.y2)) / 2) * rect.height;
  const targetLocalX = wrapW / 2;
  const targetLocalY = wrapH * 0.42;

  previewPanX = targetLocalX - baseLeft - (rect.width / 2) - ((rowCx - (rect.width / 2)) * previewZoom);
  previewPanY = targetLocalY - baseTop - (rect.height / 2) - ((rowCy - (rect.height / 2)) * previewZoom);

  clampPreviewPan();
  applyPreviewTransform();
  drawBoundingBoxes();
  return true;
}

function drawBoundingBoxes() {
  const ctx = previewCanvas.getContext('2d');
  const pageFile = pageList[currentPageIndex];
  if (!ctx || !pageFile || !previewImage.naturalWidth) {
    clearCanvas();
    return;
  }

  const previewWrap = previewCanvas.parentElement;
  if (!previewWrap) {
    clearCanvas();
    return;
  }

  const wrapRect = previewWrap.getBoundingClientRect();
  const rect = getRenderedImageRect(previewImage);
  const dpr = window.devicePixelRatio || 1;
  const baseLeft = rect.left - wrapRect.left;
  const baseTop = rect.top - wrapRect.top;
  const centerX = baseLeft + (rect.width / 2) + previewPanX;
  const centerY = baseTop + (rect.height / 2) + previewPanY;
  const drawW = Math.max(1, rect.width * previewZoom);
  const drawH = Math.max(1, rect.height * previewZoom);
  const canvasLeft = centerX - (drawW / 2);
  const canvasTop = centerY - (drawH / 2);

  previewCanvas.width = Math.round(drawW * dpr);
  previewCanvas.height = Math.round(drawH * dpr);
  previewCanvas.style.width = `${Math.round(drawW)}px`;
  previewCanvas.style.height = `${Math.round(drawH)}px`;
  previewCanvas.style.left = `${Math.round(canvasLeft)}px`;
  previewCanvas.style.top = `${Math.round(canvasTop)}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  ctx.clearRect(0, 0, previewCanvas.width, previewCanvas.height);

  // Show only active row highlight in preview (no full bbox set, no row labels).
  const pageKey = pageFile.replace('.png', '');
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  const activeOcrRowId = (activeRow && activeRow.page === pageKey && activeRow.global_row_id)
    ? activeRow.global_row_id
    : null;
  if (activeOcrRowId) {
    const bounds = boundsByPage[pageKey] || [];
    const activeBound = bounds.find((b) => pageRowToGlobal[`${pageKey}|${b.row_id}`] === activeOcrRowId);
    if (activeBound) {
      const x1 = activeBound.x1 * drawW;
      const y1 = activeBound.y1 * drawH;
      const x2 = activeBound.x2 * drawW;
      const y2 = activeBound.y2 * drawH;
      const width = Math.max(1, x2 - x1);
      const height = Math.max(1, y2 - y1);

      ctx.fillStyle = 'rgba(34, 197, 94, 0.18)';
      ctx.fillRect(x1, y1, width, height);
      ctx.strokeStyle = 'rgba(22, 163, 74, 0.9)';
      ctx.lineWidth = 1;
      ctx.strokeRect(x1, y1, width, height);
    }
  }

  if (flattenMode) {
    drawFlattenOverlay(ctx);
  }

  previewOverlayDataUrl = '';
}

function getRenderedImageRect(imgEl) {
  const container = previewWrap || imgEl.parentElement;
  const rect = container ? container.getBoundingClientRect() : imgEl.getBoundingClientRect();
  const boxW = container ? container.clientWidth : rect.width;
  const boxH = container ? container.clientHeight : rect.height;
  const naturalW = imgEl.naturalWidth || boxW;
  const naturalH = imgEl.naturalHeight || boxH;
  if (!boxW || !boxH || !naturalW || !naturalH) {
    return rect;
  }

  const boxAspect = boxW / boxH;
  const imageAspect = naturalW / naturalH;
  let renderW = boxW;
  let renderH = boxH;
  let offsetX = 0;
  let offsetY = 0;

  if (imageAspect > boxAspect) {
    renderW = boxW;
    renderH = boxW / imageAspect;
    offsetY = (boxH - renderH) / 2;
  } else {
    renderH = boxH;
    renderW = boxH * imageAspect;
    offsetX = (boxW - renderW) / 2;
  }

  return {
    left: rect.left + offsetX,
    top: rect.top + offsetY,
    width: renderW,
    height: renderH,
    right: rect.left + offsetX + renderW,
    bottom: rect.top + offsetY + renderH,
  };
}

function syncPageSelect() {
  if (!pageSelect) return;
  const needsRebuild = pageSelect.options.length !== (pageList.length + 1);
  if (needsRebuild) {
    pageSelect.innerHTML = '<option value="">Page</option>';
    pageList.forEach((_, idx) => {
      const opt = document.createElement('option');
      opt.value = String(idx);
      opt.textContent = `Page ${idx + 1}`;
      pageSelect.appendChild(opt);
    });
  }
  pageSelect.disabled = pageList.length === 0;
  pageSelect.value = pageList.length ? String(currentPageIndex) : '';
}

function applyPreviewTransform() {
  const transform = `translate(${Math.round(previewPanX)}px, ${Math.round(previewPanY)}px) scale(${previewZoom.toFixed(3)})`;
  previewImage.style.transform = transform;
  previewCanvas.style.transform = 'none';
  if (zoomLevel) {
    zoomLevel.textContent = `${Math.round(previewZoom * 100)}%`;
  }
  if (previewWrap) {
    previewWrap.classList.toggle('pannable', previewZoom > getFillPreviewZoom() + 0.001 && !flattenMode);
    if (!isPreviewPanning) {
      previewWrap.classList.remove('panning');
    }
  }
}

function resetPreviewTransform() {
  previewZoom = PREVIEW_ZOOM_MIN;
  previewPanX = 0;
  previewPanY = 0;
  stopPreviewPan();
  applyPreviewTransform();
  if (pageList.length && previewImage.naturalWidth) {
    drawBoundingBoxes();
  }
}

function clampPreviewPan() {
  const fillZoom = getFillPreviewZoom();
  if (!previewWrap || !previewImage.naturalWidth || previewZoom < fillZoom - 0.001) {
    previewPanX = 0;
    previewPanY = 0;
    return;
  }
  const rect = getRenderedImageRect(previewImage);
  const maxPanX = Math.max(0, ((rect.width * previewZoom) - rect.width) / 2);
  const maxPanY = Math.max(0, ((rect.height * previewZoom) - rect.height) / 2);
  previewPanX = clamp(previewPanX, -maxPanX, maxPanX);
  previewPanY = clamp(previewPanY, -maxPanY, maxPanY);
}

function stopPreviewPan() {
  if (!isPreviewPanning) return;
  isPreviewPanning = false;
  if (previewWrap) {
    previewWrap.classList.remove('panning');
  }
}

function getFillPreviewZoom() {
  if (!previewWrap || !previewImage.naturalWidth) return 1;
  const rect = getRenderedImageRect(previewImage);
  const wrapW = previewWrap.clientWidth || 1;
  const wrapH = previewWrap.clientHeight || 1;
  if (rect.width <= 0 || rect.height <= 0) return 1;
  const fillZoom = Math.max(wrapW / rect.width, wrapH / rect.height);
  return Math.max(PREVIEW_ZOOM_MIN, Math.min(PREVIEW_ZOOM_MAX, fillZoom));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getDefaultTopPanY(zoom) {
  if (!previewWrap || !previewImage.naturalWidth) return 0;
  const rect = getRenderedImageRect(previewImage);
  const maxPanY = Math.max(0, ((rect.height * zoom) - rect.height) / 2);
  return maxPanY > 0 ? maxPanY : 0;
}

function stepPreviewZoom(delta) {
  if (flattenMode || !pageList.length || !previewImage.naturalWidth) return;
  const fillZoom = getFillPreviewZoom();
  const nextZoom = clamp(previewZoom + delta, PREVIEW_ZOOM_MIN, PREVIEW_ZOOM_MAX);
  if (Math.abs(nextZoom - previewZoom) < 0.001) return;
  previewZoom = nextZoom;
  if (previewZoom < fillZoom - 0.001) {
    previewPanX = 0;
    previewPanY = 0;
  }
  clampPreviewPan();
  applyPreviewTransform();
  drawBoundingBoxes();
}

function clearCanvas() {
  const ctx = previewCanvas.getContext('2d');
  if (!ctx) return;
  ctx.clearRect(0, 0, previewCanvas.width, previewCanvas.height);
  previewOverlayDataUrl = '';
}

function setPreviewEmptyState() {
  previewImage.removeAttribute('src');
  previewImage.removeAttribute('data-src');
  previewImage.style.display = 'none';
  resetPreviewTransform();
  previewCanvas.style.left = '0px';
  previewCanvas.style.top = '0px';
  pageIndicator.textContent = 'Page 0 / 0';
  clearCanvas();
  updateFlattenButtons();
}

function drawFlattenOverlay(ctx) {
  const pointsPx = flattenPoints.map((p) => ({
    x: p.x * previewCanvas.width,
    y: p.y * previewCanvas.height,
  }));

  if (pointsPx.length > 1) {
    ctx.strokeStyle = '#2e4bff';
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    ctx.moveTo(pointsPx[0].x, pointsPx[0].y);
    for (let i = 1; i < pointsPx.length; i += 1) {
      ctx.lineTo(pointsPx[i].x, pointsPx[i].y);
    }
    if (pointsPx.length === 4) {
      ctx.closePath();
    }
    ctx.stroke();
  }

  pointsPx.forEach((pt, idx) => {
    ctx.fillStyle = '#2e4bff';
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = '#1f2937';
    ctx.font = '600 10px "Manrope", sans-serif';
    ctx.fillText(String(idx + 1), pt.x + 6, pt.y - 6);
  });
}

function updateFlattenButtons() {
  if (!flattenModeBtn || !applyFlattenBtn || !resetFlattenBtn) return;
  flattenModeBtn.textContent = flattenMode ? 'Cancel' : 'Flatten';
  flattenModeBtn.disabled = flattenBusy || !pageList.length;
  applyFlattenBtn.disabled = flattenBusy || !flattenMode || flattenPoints.length !== 4;
  resetFlattenBtn.disabled = flattenBusy || !pageList.length;
  previewCanvas.style.pointerEvents = flattenMode && !flattenBusy ? 'auto' : 'none';
  if (flattenMode) {
    stopPreviewPan();
  }
  applyPreviewTransform();
}

async function refreshCurrentPageData(pageKey) {
  const [parsedRes, boundsRes] = await Promise.all([
    fetch(`/jobs/${currentJobId}/parsed/${pageKey}`),
    fetch(`/jobs/${currentJobId}/rows/${pageKey}/bounds`)
  ]);

  pageImageVersion[pageKey] = (pageImageVersion[pageKey] || 0) + 1;

  if (!parsedRes.ok) {
    boundsByPage[pageKey] = [];
    renderCurrentPage();
    return;
  }

  const rows = await parsedRes.json();
  const bounds = boundsRes.ok ? await boundsRes.json() : [];

  const existingManualRows = parsedRows.filter((r) => r.page === pageKey && !r.global_row_id);
  const retainedOtherPages = parsedRows.filter((r) => r.page !== pageKey);

  const refreshedRows = [];
  rows.forEach((row) => {
    const mapped = {
      ...row,
      row_key: createRowKey(),
      global_row_id: row.row_id || '',
      page: pageKey,
      page_row_id: row.row_id || '',
    };
    normalizeRowDisplayValues(mapped);
    refreshedRows.push(mapped);
  });

  parsedRows = [...retainedOtherPages, ...refreshedRows, ...existingManualRows];
  boundsByPage[pageKey] = Array.isArray(bounds) ? bounds : [];
  rebuildPageRowMap();
  renderRows(parsedRows);
}

function scrollToResults() {
  if (!resultsSection) return;
  resultsSection.scrollIntoView({
    behavior: 'smooth',
    block: 'start'
  });
}

function computeAverageDailyBalance(rows) {
  const daily = buildDailyBalances(rows);
  if (!daily.length) return '-';

  let weightedTotal = 0;
  let totalDays = 0;

  for (let i = 0; i < daily.length; i += 1) {
    const current = daily[i];
    const nextDate = i < daily.length - 1 ? daily[i + 1].date : addDaysUTC(current.date, 1);
    const days = Math.max(1, diffDaysUTC(current.date, nextDate));
    weightedTotal += current.balance * days;
    totalDays += days;
  }

  if (!totalDays) return '-';
  return formatMoney(weightedTotal / totalDays, null, null, true);
}

function buildDailyBalances(rows) {
  const datedBalances = [];

  rows.forEach((row, idx) => {
    const balance = normalizeAmount(row.balance || '');
    const date = parseStatementDate(row.date || '');
    if (!Number.isFinite(balance) || !date) return;
    datedBalances.push({ idx, date, balance });
  });

  if (!datedBalances.length) return [];

  datedBalances.sort((a, b) => {
    const diff = a.date.getTime() - b.date.getTime();
    if (diff !== 0) return diff;
    return a.idx - b.idx;
  });

  const daily = [];
  datedBalances.forEach((entry) => {
    const key = toDateKey(entry.date);
    if (daily.length && daily[daily.length - 1].key === key) {
      daily[daily.length - 1].balance = entry.balance;
      return;
    }
    daily.push({ key, date: entry.date, balance: entry.balance });
  });
  return daily;
}

function computeMonthlySummary(rows) {
  const transactions = [];
  rows.forEach((row, idx) => {
    const date = parseStatementDate(row.date || '');
    if (!date) return;
    const debit = normalizeSummaryAmount(row, 'debit');
    const credit = normalizeSummaryAmount(row, 'credit');
    transactions.push({
      idx,
      date,
      debit,
      credit,
    });
  });
  if (!transactions.length) return [];

  transactions.sort((a, b) => {
    const diff = a.date.getTime() - b.date.getTime();
    if (diff !== 0) return diff;
    return a.idx - b.idx;
  });

  const monthMap = new Map();
  const firstDate = transactions[0].date;
  const lastDate = transactions[transactions.length - 1].date;
  seedMonthBuckets(monthMap, firstDate, lastDate);

  transactions.forEach((tx) => {
    const bucket = monthMap.get(monthKey(tx.date));
    if (!bucket) return;
    if (Number.isFinite(tx.debit)) {
      const amount = Math.abs(tx.debit);
      if (amount > 0) {
        bucket.debit += amount;
        bucket.debitCount += 1;
      }
    }
    if (Number.isFinite(tx.credit)) {
      const amount = Math.abs(tx.credit);
      if (amount > 0) {
        bucket.credit += amount;
        bucket.creditCount += 1;
      }
    }
  });

  const daily = buildDailyBalances(rows);
  for (let i = 0; i < daily.length; i += 1) {
    const current = daily[i];
    const nextDate = i < daily.length - 1 ? daily[i + 1].date : addDaysUTC(current.date, 1);
    allocateMonthlyBalanceRange(monthMap, current.date, nextDate, current.balance);
  }

  return Array.from(monthMap.values())
    .filter((bucket) => bucket.days > 0 || bucket.debit > 0 || bucket.credit > 0)
    .map((bucket) => ({
      monthLabel: bucket.label,
      debit: bucket.debit,
      credit: bucket.credit,
      avgDebit: bucket.debitCount > 0 ? (bucket.debit / bucket.debitCount) : 0,
      avgCredit: bucket.creditCount > 0 ? (bucket.credit / bucket.creditCount) : 0,
      adb: bucket.days > 0 ? (bucket.weightedBalance / bucket.days) : 0,
    }));
}

function formatMoney(parsedValue, rawA, rawB, absolute = false) {
  if (!Number.isFinite(parsedValue)) {
    const fallback = normalizeAmount((rawA || rawB || '').toString().trim());
    if (!Number.isFinite(fallback)) return '-';
    const fallbackValue = absolute ? Math.abs(fallback) : fallback;
    return formatPesoValue(fallbackValue, absolute);
  }
  const value = absolute ? Math.abs(parsedValue) : parsedValue;
  return formatPesoValue(value, absolute);
}

function formatPesoValue(value, absolute = false) {
  const formatter = new Intl.NumberFormat('en-PH', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  const sign = absolute ? '' : (value < 0 ? '-' : '+');
  return `${sign}₱${formatter.format(Math.abs(value))}`;
}

function normalizeAmount(value) {
  if (value === null || value === undefined) return NaN;
  const raw = String(value).trim();
  if (!raw) return NaN;

  const hasParenNegative = /\(\s*[\d,.\-]+\s*\)/.test(raw);
  const numericTokens = raw.match(/-?\d[\d,]*(?:\.\d+)?/g) || [];
  if (!numericTokens.length) return NaN;

  // Prefer explicit money-like tokens (decimal or comma-grouped) and use the last one.
  const moneyLike = numericTokens.filter((token) => /\.\d{1,}$/.test(token) || /,\d{3}/.test(token));
  const candidates = (moneyLike.length ? moneyLike : numericTokens).filter((token) => {
    const compact = token.replace(/[^0-9]/g, '');
    const hasDecimal = token.includes('.');
    // Ignore likely identifiers (e.g., long account/reference numbers) when not decimal amounts.
    if (!hasDecimal && compact.length >= 11) return false;
    return compact.length > 0;
  });
  if (!candidates.length) return NaN;

  const chosen = candidates[candidates.length - 1].replace(/,/g, '');
  const parsed = Number.parseFloat(chosen);
  if (!Number.isFinite(parsed)) return NaN;
  if (hasParenNegative && parsed > 0) return -parsed;
  return parsed;
}

function parseStatementDate(value) {
  const normalized = String(value || '').trim().toUpperCase().replace(/\s+/g, ' ');
  if (!normalized) return null;

  const dateOnly = normalized.split(',')[0].trim();
  const monthMap = {
    JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6,
    JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12
  };

  let m = dateOnly.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (m) return makeUtcDate(Number(m[1]), Number(m[2]), Number(m[3]));

  m = dateOnly.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return makeUtcDate(Number(m[1]), Number(m[2]), Number(m[3]));

  m = dateOnly.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (m) return makeUtcDate(normalizeYear(m[3]), Number(m[1]), Number(m[2]));

  m = dateOnly.match(/^(\d{1,2})-(\d{1,2})-(\d{2,4})$/);
  if (m) return makeUtcDate(normalizeYear(m[3]), Number(m[2]), Number(m[1]));

  m = dateOnly.match(/^(\d{1,2})\s+([A-Z]{3,9})\s+(\d{2,4})$/);
  if (m) {
    const mon = monthMap[m[2].slice(0, 3)];
    if (!mon) return null;
    return makeUtcDate(normalizeYear(m[3]), mon, Number(m[1]));
  }

  return null;
}

function normalizeYear(rawYear) {
  const n = Number(rawYear);
  if (!Number.isFinite(n)) return NaN;
  if (n >= 100) return n;
  return n <= 79 ? 2000 + n : 1900 + n;
}

function makeUtcDate(year, month, day) {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const d = new Date(Date.UTC(year, month - 1, day));
  if (d.getUTCFullYear() !== year || d.getUTCMonth() !== month - 1 || d.getUTCDate() !== day) {
    return null;
  }
  return d;
}

function diffDaysUTC(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.round((end.getTime() - start.getTime()) / msPerDay);
}

function addDaysUTC(date, days) {
  return new Date(date.getTime() + (days * 24 * 60 * 60 * 1000));
}

function toDateKey(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function monthKey(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function monthLabel(date) {
  return date.toLocaleDateString('en-US', {
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

function seedMonthBuckets(monthMap, firstDate, lastDate) {
  let cursor = new Date(Date.UTC(firstDate.getUTCFullYear(), firstDate.getUTCMonth(), 1));
  const monthCap = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), 1));
  while (cursor.getTime() <= monthCap.getTime()) {
    const key = monthKey(cursor);
    monthMap.set(key, {
      label: monthLabel(cursor),
      debit: 0,
      credit: 0,
      debitCount: 0,
      creditCount: 0,
      weightedBalance: 0,
      days: 0,
    });
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1));
  }
}

function allocateMonthlyBalanceRange(monthMap, startDate, endDate, balance) {
  if (!Number.isFinite(balance)) return;
  let cursor = new Date(startDate.getTime());

  while (cursor.getTime() < endDate.getTime()) {
    const monthStart = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth(), 1));
    const nextMonthStart = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() + 1, 1));
    const segmentEnd = endDate.getTime() < nextMonthStart.getTime() ? endDate : nextMonthStart;
    const days = Math.max(1, diffDaysUTC(cursor, segmentEnd));
    const bucket = monthMap.get(monthKey(cursor));
    if (bucket) {
      bucket.weightedBalance += balance * days;
      bucket.days += days;
    }
    cursor = segmentEnd;
  }
}

function makeEditableCell(row, field, isAmount = false) {
  const cell = document.createElement('div');
  cell.className = `table-cell ${isAmount ? 'amount-input-cell' : ''}`;
  cell.dataset.rowKey = row.row_key;
  cell.dataset.field = field;

  const input = document.createElement('input');
  input.type = 'text';
  input.className = `table-input table-input-${field}`;
  input.value = getDisplayValue(row, field);
  input.dataset.rowKey = row.row_key;
  input.dataset.field = field;
  const isExistingOcrRow = Boolean(row.global_row_id);
  const shouldHidePlaceholder = isExistingOcrRow && (field === 'debit' || field === 'credit');
  input.placeholder = shouldHidePlaceholder ? '' : field.toUpperCase();

  input.addEventListener('click', (e) => {
    e.stopPropagation();
  });

  input.addEventListener('input', () => {
    row[field] = input.value.trim();
    updateSummaryFromRows(parsedRows);
  });

  input.addEventListener('focus', () => {
    document.querySelectorAll('.table-cell.active-cell').forEach((el) => el.classList.remove('active-cell'));
    cell.classList.add('active-cell');
    if (activeRowKey !== row.row_key) {
      selectRow(row.row_key);
    }
  });

  input.addEventListener('blur', () => {
    cell.classList.remove('active-cell');
  });

  input.addEventListener('keydown', (e) => {
    handleTableInputKeydown(e, row.row_key, field);
  });

  input.addEventListener('blur', () => {
    const normalized = normalizeFieldValue(field, input.value);
    row[field] = normalized;
    input.value = normalized;
    updateSummaryFromRows(parsedRows);
  });

  input.addEventListener('change', () => {
    const normalized = normalizeFieldValue(field, input.value);
    row[field] = normalized;
    updateSummaryFromRows(parsedRows);
  });

  cell.appendChild(input);
  return cell;
}

function makeRowActionsMenu(rowKey) {
  const wrap = document.createElement('div');
  wrap.className = 'row-actions-inline';
  wrap.appendChild(makeRowActionButton('fa-solid fa-plus', 'Insert row', 'insert', () => {
    insertRowAfter(rowKey);
  }));
  wrap.appendChild(makeRowActionButton('fa-solid fa-trash', 'Delete row', 'delete', () => {
    deleteRowByKey(rowKey);
  }));
  return wrap;
}

function closeAllRowMenus() {
  // No-op for icon-based row actions.
}

function makeRowActionButton(iconClass, ariaLabel, kind, onClick) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = `row-action row-action-${kind}`;
  btn.setAttribute('aria-label', ariaLabel);
  btn.title = ariaLabel;
  btn.innerHTML = `<i class="${iconClass}" aria-hidden="true"></i>`;
  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    onClick();
  });
  return btn;
}

function insertRowAfter(rowKey) {
  const idx = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (idx < 0) return;

  const base = parsedRows[idx] || {};
  const newRow = {
    row_key: createRowKey(),
    global_row_id: '',
    row_id: '',
    date: '',
    description: '',
    debit: '',
    credit: '',
    balance: '',
    page: base.page || currentPageKey(),
    page_row_id: '',
  };
  parsedRows.splice(idx + 1, 0, newRow);
  rebuildPageRowMap();
  renderRows(parsedRows);
  highlightSelectedTableRow();
  renderCurrentPage();
}

function deleteRowByKey(rowKey) {
  const idx = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (idx < 0) return;
  parsedRows.splice(idx, 1);
  rebuildPageRowMap();

  if (!parsedRows.length) {
    activeRowKey = null;
  } else if (activeRowKey === rowKey) {
    const next = parsedRows[Math.min(idx, parsedRows.length - 1)];
    activeRowKey = next ? next.row_key : null;
  }

  renderRows(parsedRows);
  highlightSelectedTableRow();
  renderCurrentPage();
}

function rebuildPageRowMap() {
  pageRowToGlobal = {};
  identityBoundsByPage = {};
  let ocrCounter = 1;
  parsedRows.forEach((row) => {
    if (!row.page) {
      row.page = currentPageKey();
    }
    if (!row.row_key) {
      row.row_key = createRowKey();
    }
    if (row.page_row_id) {
      row.global_row_id = String(ocrCounter).padStart(3, '0');
      ocrCounter += 1;
    } else {
      row.global_row_id = '';
    }
    if (row.page && row.page_row_id) {
      pageRowToGlobal[`${row.page}|${row.page_row_id}`] = row.global_row_id;
    }
  });
}

function handleTableInputKeydown(e, rowKey, field) {
  if (!rowKey || !field) return;
  const col = TABLE_EDIT_FIELDS.indexOf(field);
  if (col < 0) return;

  if (e.key === 'Enter') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 1, 0);
    return;
  }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 1, 0);
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, -1, 0);
    return;
  }
  if (e.key === 'ArrowRight' && e.altKey) {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 0, 1);
    return;
  }
  if (e.key === 'ArrowLeft' && e.altKey) {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 0, -1);
  }
}

function focusTableCellByOffset(rowKey, colIdx, rowDelta, colDelta) {
  const rowIndex = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (rowIndex < 0) return;
  const nextRow = Math.max(0, Math.min(parsedRows.length - 1, rowIndex + rowDelta));
  const nextCol = Math.max(0, Math.min(TABLE_EDIT_FIELDS.length - 1, colIdx + colDelta));
  const targetRowKey = parsedRows[nextRow].row_key;
  const targetField = TABLE_EDIT_FIELDS[nextCol];
  const selector = `.table-input[data-row-key="${targetRowKey}"][data-field="${targetField}"]`;
  const target = document.querySelector(selector);
  if (!target) return;
  target.focus();
  if (typeof target.select === 'function') target.select();
}

function createRowKey() {
  const next = rowKeyCounter;
  rowKeyCounter += 1;
  return `row_${next}`;
}

function currentPageKey() {
  if (!pageList.length) return '';
  return pageList[Math.max(0, Math.min(currentPageIndex, pageList.length - 1))].replace('.png', '');
}

function updateSummaryFromRows(rows) {
  totalTransactions.textContent = String(rows.length);
  totalDebitTransactions.textContent = String(countAmountTransactions(rows, 'debit'));
  totalCreditTransactions.textContent = String(countAmountTransactions(rows, 'credit'));
  endingBalance.textContent = rows.length ? computeAverageDailyBalance(rows) : '-';
  renderMonthlySummary(rows);
}

function renderMonthlySummary(rows) {
  const monthlySummaryBody = document.getElementById('monthlySummaryBody');
  const monthlySummaryWrap = document.querySelector('.monthly-summary-wrap');
  if (!monthlySummaryBody) return;
  const monthly = computeMonthlySummary(rows);
  if (!monthly.length) {
    if (monthlySummaryWrap) monthlySummaryWrap.classList.add('is-empty');
    monthlySummaryBody.innerHTML = '<tr><td class="monthly-empty" colspan="6">No monthly data</td></tr>';
    return;
  }
  if (monthlySummaryWrap) monthlySummaryWrap.classList.remove('is-empty');
  monthlySummaryBody.innerHTML = monthly.map((item) => (
    `<tr>
      <td>${escapeHtml(item.monthLabel)}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.debit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.credit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.avgDebit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.avgCredit), true))}</td>
      <td>${escapeHtml(formatPesoValue(item.adb, true))}</td>
    </tr>`
  )).join('');
}

function countAmountTransactions(rows, field) {
  return rows.reduce((count, row) => {
    const amount = normalizeSummaryAmount(row, field);
    return Number.isFinite(amount) && Math.abs(amount) > 0 ? count + 1 : count;
  }, 0);
}

function normalizeSummaryAmount(row, field) {
  const amount = normalizeAmount(row && row[field]);
  if (!Number.isFinite(amount)) return NaN;
  const absAmount = Math.abs(amount);

  // Guard against reference/account-like values leaking into amount columns.
  if (absAmount >= 1_000_000_000) return NaN;

  const balance = normalizeAmount(row && row.balance);
  if (Number.isFinite(balance) && Math.abs(balance) > 0) {
    if (absAmount > Math.abs(balance) * 50) return NaN;
  }

  return amount;
}

function getDisplayValue(row, field) {
  return normalizeFieldValue(field, row[field] || '');
}

function normalizeRowDisplayValues(row) {
  row.date = normalizeFieldValue('date', row.date || '');
  row.description = normalizeFieldValue('description', row.description || '');
  row.debit = normalizeFieldValue('debit', row.debit || '');
  row.credit = normalizeFieldValue('credit', row.credit || '');
  row.balance = normalizeFieldValue('balance', row.balance || '');
}

function normalizeFieldValue(field, value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  if (field === 'date') {
    const parsed = parseStatementDate(raw);
    if (!parsed) return raw;
    const mm = String(parsed.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(parsed.getUTCDate()).padStart(2, '0');
    const yyyy = String(parsed.getUTCFullYear());
    return `${mm}/${dd}/${yyyy}`;
  }

  if (field === 'debit' || field === 'credit' || field === 'balance') {
    const parsed = normalizeAmount(raw);
    if (!Number.isFinite(parsed)) return raw;
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(parsed);
  }

  return raw;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function updateProgressUI(progress, labelText, step, ocrModel = null, parseMode = null, status = null) {
  const clamped = Math.max(0, Math.min(100, Math.round(progress)));
  progressFill.style.width = `${clamped}%`;
  progressPercent.textContent = `${clamped}%`;
  progressLabel.textContent = labelText;
  progressStep.textContent = `Step: ${stepToLabel(step)}`;
  if (progressModel) {
    const modeLabel = (parseMode || 'text').toString().toUpperCase();
    progressModel.textContent = `Mode: ${modeLabel}${modeLabel === 'OCR' ? ` | OCR Model: ${ocrModel || '-'}` : ''}`;
  }
  if (analyzerStatus && progressAnalyzerRow && analyzerModel && analyzerProfile && analyzerReason) {
    const modelText = status && status.profile_analyzer_model ? status.profile_analyzer_model : '-';
    const resultText = status && status.profile_analyzer_result ? status.profile_analyzer_result : 'idle';
    const reasonText = status && status.profile_analyzer_reason ? status.profile_analyzer_reason : '-';
    const profileText = status && status.profile_selected_after_analyzer ? status.profile_selected_after_analyzer : '-';
    const triggered = Boolean(status && status.profile_analyzer_triggered);
    analyzerStatus.textContent = `AI Analyzer: ${triggered ? capitalizeWord(resultText) : 'Idle'}`;
    analyzerModel.textContent = `Model: ${modelText}`;
    analyzerProfile.textContent = `Profile: ${profileText}`;
    analyzerReason.textContent = `Reason: ${reasonText}`;
    progressAnalyzerRow.classList.toggle('is-hidden', !triggered);
  }
  if (progressElapsed && !elapsedStartMs) {
    progressElapsed.textContent = 'Elapsed: 00:00';
  }
}

function startElapsedTimer() {
  elapsedStartMs = Date.now();
  renderElapsedTime();
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
  }
  elapsedTimer = setInterval(renderElapsedTime, 1000);
}

function stopElapsedTimer() {
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
    elapsedTimer = null;
  }
  renderElapsedTime();
}

function resetElapsedTimer() {
  elapsedStartMs = 0;
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
    elapsedTimer = null;
  }
  if (progressElapsed) {
    progressElapsed.textContent = 'Elapsed: 00:00';
  }
}

function renderElapsedTime() {
  if (!progressElapsed) return;
  if (!elapsedStartMs) {
    progressElapsed.textContent = 'Elapsed: 00:00';
    return;
  }
  const totalSecs = Math.max(0, Math.floor((Date.now() - elapsedStartMs) / 1000));
  const mins = String(Math.floor(totalSecs / 60)).padStart(2, '0');
  const secs = String(totalSecs % 60).padStart(2, '0');
  progressElapsed.textContent = `Elapsed: ${mins}:${secs}`;
}

function resetProgressUI() {
  updateProgressUI(0, 'Waiting for upload', 'queued', null);
}

function inferProgress(status, step) {
  if (step === 'draft_queued') return 2;
  if (step === 'draft_pdf_to_images') return 35;
  if (step === 'draft_image_cleaning') return 80;
  if (step === 'ready_for_edit' || status === 'draft') return 100;
  if (status === 'queued' || step === 'queued') return 0;
  if (status === 'for_review' || step === 'for_review') return 0;
  if (status === 'summary_generated') return 100;
  if (status === 'done') return 100;
  if (step === 'pdf_to_images') return 15;
  if (step === 'image_cleaning') return 40;
  if (step === 'text_extraction') return 65;
  if (step === 'page_ocr') return 80;
  if (step === 'page_text') return 80;
  if (step === 'parsing' || step === 'saving_results') return 92;
  return 10;
}

function stepToLabel(step) {
  const key = String(step || '').toLowerCase();
  if (key === 'queued') return 'Queued';
  if (key === 'for_review') return 'For Review';
  if (key === 'draft_queued') return 'Draft queued';
  if (key === 'draft_pdf_to_images') return 'Preparing preview pages';
  if (key === 'draft_image_cleaning') return 'Cleaning draft pages';
  if (key === 'ready_for_edit') return 'Ready for edit';
  if (key === 'uploading') return 'Uploading';
  if (key === 'pdf_to_images') return 'Converting PDF to images';
  if (key === 'image_cleaning') return 'Cleaning page images';
  if (key === 'text_extraction') return 'Extracting PDF text';
  if (key === 'account_identity_ai') return 'Extracting account identity';
  if (key === 'profile_analyzer') return 'Analyzing unknown bank structure';
  if (key === 'page_ocr') return 'Running OCR per page';
  if (key === 'page_text') return 'Parsing text per page';
  if (key === 'saving_results') return 'Saving results';
  if (key === 'parsing') return 'Parsing extracted rows';
  if (key === 'completed' || key === 'done') return 'Completed';
  if (key === 'summary_generated') return 'Summary Generated';
  if (key === 'failed') return 'Failed';
  return 'Processing';
}

function capitalizeWord(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatSubmissionStatus(status) {
  const raw = String(status || '').trim();
  if (!raw) return '-';
  return raw
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function resetResults() {
  totalTransactions.textContent = '0';
  totalDebitTransactions.textContent = '0';
  totalCreditTransactions.textContent = '0';
  endingBalance.textContent = '-';
  if (accountNameSummary) accountNameSummary.textContent = '-';
  if (accountNumberSummary) accountNumberSummary.textContent = '-';
  tableBody.innerHTML = '';
  pageList = [];
  parsedRows = [];
  rowsByPage = {};
  boundsByPage = {};
  pageRowToGlobal = {};
  activeRowKey = null;
  currentPageIndex = 0;
  rowKeyCounter = 1;
  pageImageVersion = {};
  prefetchedPreviewSrcs.clear();
  flattenMode = false;
  flattenPoints = [];
  flattenBusy = false;
  ocrStarted = false;
  finishSave.textContent = 'Start OCR';
  resetElapsedTimer();
  shouldAutoScrollToResults = false;
  hasSeenInFlightStatus = false;
  updateSummaryFromRows([]);
  updateFlattenButtons();
  renderCurrentPage();
}

function buildPreviewSrc(pageKey) {
  return `/jobs/${currentJobId}/preview/${pageKey}?v=${pageImageVersion[pageKey] || 0}`;
}

function prefetchPreviewPageByIndex(idx) {
  if (!currentJobId || idx < 0 || idx >= pageList.length) return;
  const pageKey = pageList[idx].replace('.png', '');
  const src = buildPreviewSrc(pageKey);
  if (prefetchedPreviewSrcs.has(src)) return;
  prefetchedPreviewSrcs.add(src);
  const img = new Image();
  img.decoding = 'async';
  img.src = src;
}

function prefetchPreviewNeighbors() {
  prefetchPreviewPageByIndex(currentPageIndex + 1);
  prefetchPreviewPageByIndex(currentPageIndex - 1);
  prefetchPreviewPageByIndex(currentPageIndex + 2);
}

function formatFileSize(bytes) {
  if (!bytes) return '0 Bytes';
  const units = ['Bytes', 'KB', 'MB', 'GB'];
  const idx = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, idx)).toFixed(1)} ${units[idx]}`;
}

async function safeParseJson(res) {
  try {
    return await res.json();
  } catch (e) {
    return null;
  }
}

async function markSummaryGenerated() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/mark-summary-generated`, {
    method: 'POST',
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    throw new Error((body && body.detail) || 'Failed to mark summary generated');
  }
  evaluatorSelectedSubmission = body;
  await loadEvaluatorSubmissions();
}

async function exportToPdf() {
  if (!parsedRows.length) {
    alert('No extracted rows to export yet.');
    return;
  }

  if (!window.jspdf || typeof window.jspdf.jsPDF !== 'function') {
    alert('PDF library is not loaded.');
    return;
  }

  const doc = new window.jspdf.jsPDF({
    orientation: 'portrait',
    unit: 'pt',
    format: 'a4'
  });

  const marginLeft = 40;
  let y = 42;

  doc.setFont('helvetica', 'bold');
  doc.setFontSize(14);
  doc.text('Account Summary', marginLeft, y);
  y += 16;

  doc.setFont('helvetica', 'normal');
  doc.setFontSize(9);
  const generatedAt = new Date();
  doc.text(
    `Generated: ${generatedAt.toLocaleDateString()} ${generatedAt.toLocaleTimeString()}`,
    marginLeft,
    y
  );
  y += 14;

  const summaryRows = [
    ['Account Name', pdfSafeText((accountNameSummary && accountNameSummary.textContent ? accountNameSummary.textContent : '-').trim())],
    ['Account Number', pdfSafeText((accountNumberSummary && accountNumberSummary.textContent ? accountNumberSummary.textContent : '-').trim())],
    ['Total Transactions', pdfSafeText((totalTransactions && totalTransactions.textContent ? totalTransactions.textContent : '0').trim())],
    ['Debit Transactions', pdfSafeText((totalDebitTransactions && totalDebitTransactions.textContent ? totalDebitTransactions.textContent : '0').trim())],
    ['Credit Transactions', pdfSafeText((totalCreditTransactions && totalCreditTransactions.textContent ? totalCreditTransactions.textContent : '0').trim())],
    ['Average Daily Balance (ADB)', formatPdfMoneyPlain(endingBalance && endingBalance.textContent ? endingBalance.textContent : '-')]
  ];

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [[{ content: 'Account Summary', colSpan: 2, styles: { halign: 'left' } }]],
      body: summaryRows,
      styles: { font: 'helvetica', fontSize: 9, cellPadding: 5 },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: { 0: { cellWidth: 180 } }
    });
    y = doc.lastAutoTable.finalY + 16;
  } else {
    summaryRows.forEach(([label, value]) => {
      doc.text(`${label}: ${value}`, marginLeft, y);
      y += 12;
    });
    y += 8;
  }

  const monthly = computeMonthlySummary(parsedRows);
  doc.setFont('helvetica', 'bold');
  doc.setFontSize(11);
  doc.text('Monthly Summary', marginLeft, y);
  y += 8;

  const monthlyRows = monthly.length
    ? monthly.map((item) => ([
      pdfSafeText(item.monthLabel),
      formatPdfMoney(item.debit, true),
      formatPdfMoney(item.credit, true),
      formatPdfMoney(item.avgDebit, true),
      formatPdfMoney(item.avgCredit, true),
      formatPdfMoney(item.adb, true),
    ]))
    : [['No monthly data', '-', '-', '-', '-', '-']];

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [['Month', 'Debit', 'Credit', 'Avg Debit', 'Avg Credit', 'ADB']],
      body: monthlyRows,
      styles: { font: 'helvetica', fontSize: 8.5, cellPadding: 4 },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: {
        0: { cellWidth: 108 },
        1: { cellWidth: 88, halign: 'right' },
        2: { cellWidth: 88, halign: 'right' },
        3: { cellWidth: 95, halign: 'right' },
        4: { cellWidth: 95, halign: 'right' },
        5: { cellWidth: 95, halign: 'right' }
      }
    });
    y = doc.lastAutoTable.finalY + 16;
  } else {
    monthlyRows.forEach(([month, debit, credit, avgDebit, avgCredit, adb]) => {
      doc.text(`${month}: ${debit} / ${credit} / ${avgDebit} / ${avgCredit} / ${adb}`, marginLeft, y);
      y += 12;
    });
    y += 8;
  }

  doc.setFont('helvetica', 'bold');
  doc.setFontSize(12);
  doc.text('Transactions', marginLeft, y);
  y += 10;

  const tableRows = parsedRows.map((row, idx) => ([
    String(row.global_row_id || row.row_id || idx + 1),
    pdfSafeText(getDisplayValue(row, 'date') || ''),
    pdfSafeText(getDisplayValue(row, 'description') || ''),
    formatPdfMoney(getDisplayValue(row, 'debit') || ''),
    formatPdfMoney(getDisplayValue(row, 'credit') || ''),
    formatPdfMoney(getDisplayValue(row, 'balance') || '')
  ]));

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [['#', 'Date', 'Description', 'Debit', 'Credit', 'Balance']],
      body: tableRows,
      styles: { font: 'helvetica', fontSize: 8.5, cellPadding: 4, overflow: 'linebreak' },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: {
        0: { cellWidth: 34, halign: 'center' },
        1: { cellWidth: 70 },
        2: { cellWidth: 170 },
        3: { cellWidth: 78, halign: 'right' },
        4: { cellWidth: 78, halign: 'right' },
        5: { cellWidth: 88, halign: 'right' }
      }
    });
  } else {
    const fallbackLines = tableRows.map((cols) => cols.join(' | '));
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8);
    fallbackLines.forEach((line) => {
      if (y > 780) {
        doc.addPage();
        y = 42;
      }
      doc.text(line, marginLeft, y);
      y += 10;
    });
  }

  doc.save(buildPdfFileName());
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      await markSummaryGenerated();
    } catch (_err) {
      // Keep export non-blocking even if status update fails.
    }
  }
}

async function exportToExcel() {
  if (!parsedRows.length) {
    alert('No extracted rows to export yet.');
    return;
  }
  if (!window.XLSX || !window.XLSX.utils || typeof window.XLSX.writeFile !== 'function') {
    alert('Excel export library is not loaded.');
    return;
  }

  const workbook = window.XLSX.utils.book_new();

  const summaryRows = [
    ['Account Name', (accountNameSummary && accountNameSummary.textContent ? accountNameSummary.textContent : '-').trim()],
    ['Account Number', (accountNumberSummary && accountNumberSummary.textContent ? accountNumberSummary.textContent : '-').trim()],
    ['Total Transactions', (totalTransactions && totalTransactions.textContent ? totalTransactions.textContent : '0').trim()],
    ['Debit Transactions', (totalDebitTransactions && totalDebitTransactions.textContent ? totalDebitTransactions.textContent : '0').trim()],
    ['Credit Transactions', (totalCreditTransactions && totalCreditTransactions.textContent ? totalCreditTransactions.textContent : '0').trim()],
    ['Average Daily Balance (ADB)', (endingBalance && endingBalance.textContent ? endingBalance.textContent : '-').trim()],
  ];
  const summarySheet = window.XLSX.utils.aoa_to_sheet([
    ['Account Summary', ''],
    ...summaryRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, summarySheet, 'Summary');

  const monthly = computeMonthlySummary(parsedRows);
  const monthlyRows = monthly.length
    ? monthly.map((item) => ([
      item.monthLabel,
      formatPesoValue(Math.abs(item.debit), true),
      formatPesoValue(Math.abs(item.credit), true),
      formatPesoValue(Math.abs(item.avgDebit), true),
      formatPesoValue(Math.abs(item.avgCredit), true),
      formatPesoValue(item.adb, true),
    ]))
    : [['No monthly data', '-', '-', '-', '-', '-']];
  const monthlySheet = window.XLSX.utils.aoa_to_sheet([
    ['Month', 'Debit', 'Credit', 'Avg Debit', 'Avg Credit', 'ADB'],
    ...monthlyRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, monthlySheet, 'Monthly Summary');

  const txRows = parsedRows.map((row, idx) => ([
    String(row.global_row_id || row.row_id || idx + 1),
    getDisplayValue(row, 'date') || '',
    getDisplayValue(row, 'description') || '',
    getDisplayValue(row, 'debit') || '',
    getDisplayValue(row, 'credit') || '',
    getDisplayValue(row, 'balance') || '',
  ]));
  const txSheet = window.XLSX.utils.aoa_to_sheet([
    ['#', 'Date', 'Description', 'Debit', 'Credit', 'Balance'],
    ...txRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, txSheet, 'Transactions');

  const stamp = currentJobId ? String(currentJobId).slice(0, 8) : 'export';
  window.XLSX.writeFile(workbook, `statement_${stamp}.xlsx`);
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      await markSummaryGenerated();
    } catch (_err) {
      // Keep export non-blocking even if status update fails.
    }
  }
}

function buildPdfFileName() {
  const raw = selectedFile && selectedFile.name ? selectedFile.name : 'statement';
  const base = raw.replace(/\.pdf$/i, '').replace(/[^a-zA-Z0-9-_]+/g, '_').replace(/^_+|_+$/g, '');
  return `${base || 'statement'}_export.pdf`;
}

function formatPdfMoney(value, absolute = false) {
  const n = typeof value === 'number' ? value : normalizeAmount(String(value || ''));
  if (!Number.isFinite(n)) return pdfSafeText(String(value || '-').trim() || '-');
  const formatted = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(n));
  const sign = absolute ? '' : (n < 0 ? '-' : '');
  return `${sign}${formatted}`;
}

function formatPdfMoneyPlain(value, absolute = false) {
  const n = typeof value === 'number' ? value : normalizeAmount(String(value || ''));
  if (!Number.isFinite(n)) return pdfSafeText(String(value || '-').trim() || '-');
  const formatted = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(n));
  const sign = absolute ? '' : (n < 0 ? '-' : '');
  return `${sign}${formatted}`;
}

function pdfSafeText(value) {
  return String(value || '')
    .replace(/₱/g, 'PHP ')
    .replace(/[^\x20-\x7E\n\r\t]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function openFilePicker() {
  if (typeof fileInput.showPicker === 'function') {
    fileInput.showPicker();
    return;
  }
  fileInput.click();
}

resetProgressUI();
resetResults();
updateFlattenButtons();

if ('scrollRestoration' in history) {
  history.scrollRestoration = 'manual';
}
window.scrollTo(0, 0);
