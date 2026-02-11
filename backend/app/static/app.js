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

downloadCSV.addEventListener('click', () => {
  exportToPdf();
});

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

  const pageKey = pageFile.replace('.png', '');
  const bounds = boundsByPage[pageKey] || [];
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  const activeOcrRowId = (activeRow && activeRow.page === pageKey && activeRow.global_row_id)
    ? activeRow.global_row_id
    : null;

  bounds.forEach((b) => {
    const mappedGlobalId = pageRowToGlobal[`${pageKey}|${b.row_id}`];
    if (!mappedGlobalId) {
      // Row was deleted from table (or is otherwise unmapped), so hide its bbox.
      return;
    }

    const x1 = b.x1 * drawW;
    const y1 = b.y1 * drawH;
    const x2 = b.x2 * drawW;
    const y2 = b.y2 * drawH;

    const width = Math.max(1, x2 - x1);
    const height = Math.max(1, y2 - y1);
    const globalId = mappedGlobalId;
    const isActive = activeOcrRowId !== null && globalId === activeOcrRowId;

    ctx.strokeStyle = isActive ? '#16a34a' : '#22c55e';
    ctx.lineWidth = isActive ? 1.6 : 1.0;
    ctx.strokeRect(x1, y1, width, height);

    ctx.fillStyle = isActive ? '#16a34a' : '#22c55e';
    const labelSize = Math.max(11, Math.min(18, 11 + ((previewZoom - 1) * 4)));
    ctx.font = `600 ${labelSize}px "Manrope", sans-serif`;
    const textWidth = ctx.measureText(globalId).width;
    const labelX = Math.max(2, x1 - textWidth - 6);
    const labelY = Math.min(drawH - 2, Math.max(labelSize, y1 + (height / 2) + 4));
    ctx.fillText(globalId, labelX, labelY);
  });

  const identityBoxes = identityBoundsByPage[pageKey] || [];
  identityBoxes.forEach((b) => {
    const x1 = Number(b.x1 || 0) * drawW;
    const y1 = Number(b.y1 || 0) * drawH;
    const x2 = Number(b.x2 || 0) * drawW;
    const y2 = Number(b.y2 || 0) * drawH;
    const width = Math.max(1, x2 - x1);
    const height = Math.max(1, y2 - y1);
    const label = b.kind === 'account_number' ? 'ACC NO' : 'ACC NAME';

    ctx.strokeStyle = '#0ea5e9';
    ctx.lineWidth = 1.2;
    ctx.strokeRect(x1, y1, width, height);

    ctx.fillStyle = '#0ea5e9';
    ctx.font = '600 11px "Manrope", sans-serif';
    ctx.fillText(label, Math.max(2, x1 + 2), Math.max(11, y1 - 3));
  });

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
  previewZoom = getFillPreviewZoom();
  previewPanX = 0;
  previewPanY = getDefaultTopPanY(previewZoom);
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
    transactions.push({
      idx,
      date,
      debit: normalizeAmount(row.debit || ''),
      credit: normalizeAmount(row.credit || ''),
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
    if (Number.isFinite(tx.debit)) bucket.debit += Math.abs(tx.debit);
    if (Number.isFinite(tx.credit)) bucket.credit += Math.abs(tx.credit);
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
  const cleaned = String(value).replace(/[^0-9.-]/g, '');
  if (!cleaned) return NaN;
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : NaN;
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

  const input = document.createElement('input');
  input.type = 'text';
  input.className = `table-input table-input-${field}`;
  input.value = getDisplayValue(row, field);
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
    monthlySummaryBody.innerHTML = '<tr><td class="monthly-empty" colspan="4">No monthly data</td></tr>';
    return;
  }
  if (monthlySummaryWrap) monthlySummaryWrap.classList.remove('is-empty');
  monthlySummaryBody.innerHTML = monthly.map((item) => (
    `<tr>
      <td>${escapeHtml(item.monthLabel)}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.debit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.credit), true))}</td>
      <td>${escapeHtml(formatPesoValue(item.adb, true))}</td>
    </tr>`
  )).join('');
}

function countAmountTransactions(rows, field) {
  return rows.reduce((count, row) => {
    const amount = normalizeAmount(row[field]);
    return Number.isFinite(amount) && Math.abs(amount) > 0 ? count + 1 : count;
  }, 0);
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
  if (key === 'failed') return 'Failed';
  return 'Processing';
}

function capitalizeWord(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.charAt(0).toUpperCase() + text.slice(1);
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

function exportToPdf() {
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
      formatPdfMoney(item.adb, true),
    ]))
    : [['No monthly data', '-', '-', '-']];

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [['Month', 'Debit', 'Credit', 'ADB']],
      body: monthlyRows,
      styles: { font: 'helvetica', fontSize: 8.5, cellPadding: 4 },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: {
        0: { cellWidth: 145 },
        1: { cellWidth: 123, halign: 'right' },
        2: { cellWidth: 123, halign: 'right' },
        3: { cellWidth: 124, halign: 'right' }
      }
    });
    y = doc.lastAutoTable.finalY + 16;
  } else {
    monthlyRows.forEach(([month, debit, credit, adb]) => {
      doc.text(`${month}: ${debit} / ${credit} / ${adb}`, marginLeft, y);
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
