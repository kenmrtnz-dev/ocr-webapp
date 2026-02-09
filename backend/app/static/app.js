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
const downloadCSV = document.getElementById('downloadCSV');
const viewDetails = document.getElementById('viewDetails');
const finishSave = document.getElementById('finishSave');
const tableBody = document.querySelector('.table-body');
const totalTransactions = document.getElementById('totalTransactions');
const totalDebitTransactions = document.getElementById('totalDebitTransactions');
const totalCreditTransactions = document.getElementById('totalCreditTransactions');
const endingBalance = document.getElementById('endingBalance');
const previewImage = document.getElementById('previewImage');
const previewCanvas = document.getElementById('previewCanvas');
const prevPageBtn = document.getElementById('prevPageBtn');
const nextPageBtn = document.getElementById('nextPageBtn');
const flattenModeBtn = document.getElementById('flattenModeBtn');
const applyFlattenBtn = document.getElementById('applyFlattenBtn');
const resetFlattenBtn = document.getElementById('resetFlattenBtn');
const pageIndicator = document.getElementById('pageIndicator');
const pageSelect = document.getElementById('pageSelect');
const previewWrap = document.querySelector('.preview-canvas-wrap');
const previewMagnifier = document.getElementById('previewMagnifier');
const resultsSection = document.querySelector('.results-section');

let selectedFile = null;
let currentJobId = null;
let pageList = [];
let parsedRows = [];
let rowsByPage = {};
let boundsByPage = {};
let pageRowToGlobal = {};
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
const MAGNIFIER_ZOOM = 2.2;

browseButton.addEventListener('click', (e) => {
  e.stopPropagation();
  openFilePicker();
});

uploadArea.addEventListener('click', () => openFilePicker());

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
  if (!parsedRows.length) {
    alert('No extracted rows to export yet.');
    return;
  }

  const csv = buildCsv(parsedRows);
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `ocr_result_${currentJobId || 'job'}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
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
  drawBoundingBoxes();
  hideMagnifier();
});

previewImage.addEventListener('error', () => {
  setPreviewEmptyState();
});

window.addEventListener('resize', () => {
  if (pageList.length) {
    drawBoundingBoxes();
  }
  hideMagnifier();
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.row-menu')) {
    closeAllRowMenus();
  }
});

previewCanvas.addEventListener('click', (e) => {
  if (!flattenMode || flattenBusy) return;
  if (!previewCanvas.width || !previewCanvas.height) return;

  const rect = previewCanvas.getBoundingClientRect();
  const x = (e.clientX - rect.left) / rect.width;
  const y = (e.clientY - rect.top) / rect.height;
  if (x < 0 || x > 1 || y < 0 || y > 1) return;

  if (flattenPoints.length >= 4) return;
  flattenPoints.push({ x, y });
  updateFlattenButtons();
  drawBoundingBoxes();
});

if (previewWrap) {
  previewWrap.addEventListener('mousemove', (e) => {
    updateMagnifier(e);
  });

  previewWrap.addEventListener('mouseleave', () => {
    hideMagnifier();
  });
}

flattenModeBtn.addEventListener('click', () => {
  if (!pageList.length || flattenBusy) return;
  flattenMode = !flattenMode;
  flattenPoints = [];
  updateFlattenButtons();
  drawBoundingBoxes();
});

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
  updateProgressUI(4, 'Uploading for pre-processing...', 'uploading');

  try {
    const formData = new FormData();
    formData.append('file', selectedFile);

    const res = await fetch('/jobs/draft', { method: 'POST', body: formData });
    if (!res.ok) {
      const error = await safeParseJson(res);
      throw new Error((error && error.detail) || 'Failed to upload file');
    }

    const data = await res.json();
    currentJobId = data.job_id;
    await pollDraftUntilReady();
  } catch (err) {
    stopElapsedTimer();
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

    if (status.status === 'failed') {
      throw new Error(status.message || 'Draft preparation failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel);

    if (status.status === 'draft' && step === 'ready_for_edit') {
      stopElapsedTimer();
      await loadDraftPages();
      updateProgressUI(100, 'Edit pages then click Start OCR', 'ready_for_edit', currentOcrModel);
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
    if (status.status === 'queued' || status.status === 'processing') {
      hasSeenInFlightStatus = true;
    }

    if (status.status === 'done') {
      updateProgressUI(100, 'Results ready', step, currentOcrModel);
      await loadResults();
      if (shouldAutoScrollToResults && hasSeenInFlightStatus) {
        shouldAutoScrollToResults = false;
        hasSeenInFlightStatus = false;
        scrollToResults();
      }
      return;
    }

    if (status.status === 'failed') {
      updateProgressUI(progress, status.message || 'OCR job failed', step, currentOcrModel);
      throw new Error(status.message || 'OCR job failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel);
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
  parsedRows = [];
  activeRowKey = null;
  currentPageIndex = 0;
  rowKeyCounter = 1;
  pageImageVersion = {};

  if (!pageList.length) {
    renderRows([]);
    renderCurrentPage();
    return;
  }

  let globalCounter = 1;

  for (let i = 0; i < pageList.length; i += 1) {
    const pageKey = pageList[i].replace('.png', '');

    const [parsedRes, boundsRes] = await Promise.all([
      fetch(`/jobs/${currentJobId}/parsed/${pageKey}`),
      fetch(`/jobs/${currentJobId}/rows/${pageKey}/bounds`)
    ]);

    if (!parsedRes.ok) {
      throw new Error(`Failed to read parsed rows for ${pageKey}`);
    }

    const rows = await parsedRes.json();
    const bounds = boundsRes.ok ? await boundsRes.json() : [];

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

  const src = `/jobs/${currentJobId}/cleaned/${pageFile}?v=${pageImageVersion[pageKey] || 0}`;
  if (previewImage.dataset.src !== src) {
    hideMagnifier();
    previewImage.style.display = 'none';
    previewImage.dataset.src = src;
    previewImage.src = src;
  } else {
    previewImage.style.display = 'block';
    drawBoundingBoxes();
  }

  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (activeRow && activeRow.page !== pageKey) {
    highlightSelectedTableRow();
  }
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
  previewCanvas.width = Math.round(rect.width);
  previewCanvas.height = Math.round(rect.height);
  previewCanvas.style.width = `${Math.round(rect.width)}px`;
  previewCanvas.style.height = `${Math.round(rect.height)}px`;
  previewCanvas.style.left = `${Math.round(rect.left - wrapRect.left)}px`;
  previewCanvas.style.top = `${Math.round(rect.top - wrapRect.top)}px`;

  ctx.clearRect(0, 0, previewCanvas.width, previewCanvas.height);

  const pageKey = pageFile.replace('.png', '');
  const bounds = boundsByPage[pageKey] || [];
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  const activeOcrRowId = (activeRow && activeRow.page === pageKey && activeRow.global_row_id)
    ? activeRow.global_row_id
    : null;

  bounds.forEach((b) => {
    const x1 = b.x1 * previewCanvas.width;
    const y1 = b.y1 * previewCanvas.height;
    const x2 = b.x2 * previewCanvas.width;
    const y2 = b.y2 * previewCanvas.height;

    const width = Math.max(1, x2 - x1);
    const height = Math.max(1, y2 - y1);
    const globalId = pageRowToGlobal[`${pageKey}|${b.row_id}`] || b.row_id;
    const isActive = activeOcrRowId !== null && globalId === activeOcrRowId;

    ctx.strokeStyle = isActive ? '#22c55e' : '#ef4444';
    ctx.lineWidth = isActive ? 1.4 : 0.8;
    ctx.strokeRect(x1, y1, width, height);

    ctx.fillStyle = isActive ? '#22c55e' : '#ef4444';
    ctx.font = '600 11px "Roboto Mono", monospace';
    const textWidth = ctx.measureText(globalId).width;
    const labelX = Math.max(2, x1 - textWidth - 6);
    const labelY = Math.min(previewCanvas.height - 2, Math.max(12, y1 + (height / 2) + 4));
    ctx.fillText(globalId, labelX, labelY);
  });

  if (flattenMode) {
    drawFlattenOverlay(ctx);
  }

  previewOverlayDataUrl = previewCanvas.toDataURL('image/png');
}

function getRenderedImageRect(imgEl) {
  const rect = imgEl.getBoundingClientRect();
  const boxW = rect.width;
  const boxH = rect.height;
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

function hideMagnifier() {
  if (!previewMagnifier) return;
  previewMagnifier.style.display = 'none';
}

function updateMagnifier(event) {
  if (!previewMagnifier || !previewWrap || !pageList.length || !previewImage.naturalWidth || flattenMode) {
    hideMagnifier();
    return;
  }

  const imageRect = getRenderedImageRect(previewImage);
  const wrapRect = previewWrap.getBoundingClientRect();
  const x = event.clientX;
  const y = event.clientY;
  const withinImage = x >= imageRect.left && x <= imageRect.right && y >= imageRect.top && y <= imageRect.bottom;
  if (!withinImage) {
    hideMagnifier();
    return;
  }

  const lensW = previewMagnifier.offsetWidth || 160;
  const lensH = previewMagnifier.offsetHeight || 110;
  const localX = x - imageRect.left;
  const localY = y - imageRect.top;
  const bgX = -(localX * MAGNIFIER_ZOOM - lensW / 2);
  const bgY = -(localY * MAGNIFIER_ZOOM - lensH / 2);

  const pageSrc = previewImage.currentSrc || previewImage.src;
  if (previewOverlayDataUrl) {
    previewMagnifier.style.backgroundImage = `url("${previewOverlayDataUrl}"), url("${pageSrc}")`;
  } else {
    previewMagnifier.style.backgroundImage = `url("${pageSrc}")`;
  }
  previewMagnifier.style.backgroundSize = `${Math.round(imageRect.width * MAGNIFIER_ZOOM)}px ${Math.round(imageRect.height * MAGNIFIER_ZOOM)}px`;
  previewMagnifier.style.backgroundPosition = `${Math.round(bgX)}px ${Math.round(bgY)}px`;

  let lensLeft = x - wrapRect.left + 16;
  let lensTop = y - wrapRect.top + 16;
  lensLeft = Math.max(4, Math.min(lensLeft, wrapRect.width - lensW - 4));
  lensTop = Math.max(4, Math.min(lensTop, wrapRect.height - lensH - 4));

  previewMagnifier.style.left = `${Math.round(lensLeft)}px`;
  previewMagnifier.style.top = `${Math.round(lensTop)}px`;
  previewMagnifier.style.display = 'block';
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
  hideMagnifier();
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
    ctx.font = '600 10px "Roboto Mono", monospace';
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
  const datedBalances = [];

  rows.forEach((row, idx) => {
    const balance = normalizeAmount(row.balance || '');
    const date = parseStatementDate(row.date || '');
    if (!Number.isFinite(balance) || !date) return;
    datedBalances.push({ idx, date, balance });
  });

  if (!datedBalances.length) return '-';

  datedBalances.sort((a, b) => {
    const diff = a.date.getTime() - b.date.getTime();
    if (diff !== 0) return diff;
    return a.idx - b.idx;
  });

  // Keep the last known balance per transaction date.
  const daily = [];
  datedBalances.forEach((entry) => {
    const key = toDateKey(entry.date);
    if (daily.length && daily[daily.length - 1].key === key) {
      daily[daily.length - 1].balance = entry.balance;
      return;
    }
    daily.push({ key, date: entry.date, balance: entry.balance });
  });

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

function buildCsv(rows) {
  const header = ['row_id', 'page', 'date', 'description', 'debit', 'credit', 'balance'];
  const lines = [header.join(',')];

  rows.forEach((row) => {
    const fields = [
      row.global_row_id || '',
      row.page || '',
      row.date || '',
      row.description || '',
      row.debit || '',
      row.credit || '',
      row.balance || ''
    ].map(csvEscape);

    lines.push(fields.join(','));
  });

  return `${lines.join('\n')}\n`;
}

function csvEscape(value) {
  const text = String(value);
  if (!/[",\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
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

  cell.appendChild(input);
  return cell;
}

function makeRowActionsMenu(rowKey) {
  const wrap = document.createElement('div');
  wrap.className = 'row-menu';

  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.className = 'row-action row-action-more';
  trigger.setAttribute('aria-label', 'Row actions');
  trigger.title = 'Row actions';
  trigger.textContent = '...';

  const menu = document.createElement('div');
  menu.className = 'row-menu-list';
  menu.innerHTML = ''
    + '<button type="button" class="row-menu-item row-menu-insert">Insert row</button>'
    + '<button type="button" class="row-menu-item row-menu-delete">Delete row</button>';

  trigger.addEventListener('click', (e) => {
    e.stopPropagation();
    const isOpen = wrap.classList.contains('open');
    closeAllRowMenus();
    if (!isOpen) {
      wrap.classList.add('open');
    }
  });

  menu.querySelector('.row-menu-insert').addEventListener('click', (e) => {
    e.stopPropagation();
    insertRowAfter(rowKey);
    closeAllRowMenus();
  });
  menu.querySelector('.row-menu-delete').addEventListener('click', (e) => {
    e.stopPropagation();
    deleteRowByKey(rowKey);
    closeAllRowMenus();
  });

  wrap.appendChild(trigger);
  wrap.appendChild(menu);
  return wrap;
}

function closeAllRowMenus() {
  document.querySelectorAll('.row-menu.open').forEach((el) => {
    el.classList.remove('open');
  });
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

function updateProgressUI(progress, labelText, step, ocrModel = null) {
  const clamped = Math.max(0, Math.min(100, Math.round(progress)));
  progressFill.style.width = `${clamped}%`;
  progressPercent.textContent = `${clamped}%`;
  progressLabel.textContent = labelText;
  progressStep.textContent = `Step: ${stepToLabel(step)}`;
  if (progressModel) {
    progressModel.textContent = `OCR Model: ${ocrModel || '-'}`;
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
  if (key === 'page_ocr') return 'Running OCR per page';
  if (key === 'saving_results') return 'Saving OCR results';
  if (key === 'parsing') return 'Parsing extracted rows';
  if (key === 'completed' || key === 'done') return 'Completed';
  if (key === 'failed') return 'Failed';
  return 'Processing';
}

function resetResults() {
  totalTransactions.textContent = '0';
  totalDebitTransactions.textContent = '0';
  totalCreditTransactions.textContent = '0';
  endingBalance.textContent = '-';
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
