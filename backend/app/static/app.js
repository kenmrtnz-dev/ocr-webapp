/* ================================
   GLOBAL STATE
================================ */
let currentJobId = null;
let polling = false;

let currentPageIndex = 0;
let pageList = [];

let ocrItems = [];
let highlightedId = null;
let jobDone = false


/* ================================
   UPLOAD
================================ */
async function upload() {
  const fileInput = document.getElementById("file");
  const statusDiv = document.getElementById("status");

  // RESET STATE
  currentJobId = null;
  currentPageIndex = 0;
  pageList = [];
  ocrItems = [];
  highlightedId = null;

  if (!fileInput.files.length) {
    alert("Please select a PDF file");
    return;
  }

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);

  statusDiv.innerText = "Uploading...";

  const res = await fetch("/jobs", {
    method: "POST",
    body: formData
  });

  const data = await res.json();
  currentJobId = data.job_id;

  statusDiv.innerText = `Job ID: ${currentJobId} (queued)`;
  pollStatus();
}


/* ================================
   JOB STATUS POLLING
================================ */
async function pollStatus() {
  if (!currentJobId || polling) return;
  polling = true;

  const res = await fetch(`/jobs/${currentJobId}`);
  const data = await res.json();

  if (data.status === "done") {
    document.getElementById("status").innerText =
      `Job ID: ${currentJobId} — done`;

    await loadCleanedPages();
    polling = false;
    jobDone = true
    return;
  }

  document.getElementById("status").innerText =
    `Job ID: ${currentJobId} — ${data.status}`;

  polling = false;
  setTimeout(pollStatus, 1500);
}


/* ================================
   CLEANED PAGE NAVIGATION
================================ */
async function loadCleanedPages() {
  const res = await fetch(`/jobs/${currentJobId}/cleaned`);
  const data = await res.json();

  pageList = data.pages || [];
  currentPageIndex = 0;

  if (!pageList.length) return;

  showCurrentPage();
}

function nextPage() {
  if (currentPageIndex < pageList.length - 1) {
    currentPageIndex++;
    showCurrentPage();
  }
}

function prevPage() {
  if (currentPageIndex > 0) {
    currentPageIndex--;
    showCurrentPage();
  }
}


/* ================================
   PAGE + OCR LOAD
================================ */
async function showCurrentPage() {
  const page = pageList[currentPageIndex];

  document.getElementById("status").innerHTML = `
    <div style="display:flex; gap:12px; margin-top:16px">

      <!-- LEFT: OCR TABLE -->
      <div style="width:45%; overflow:auto">
        <h4>OCR Text</h4>
        <table border="1" width="100%" id="ocrTable">
          <thead id="ocrHeader"></thead>
          <tbody></tbody>
        </table>
      </div>

      <!-- RIGHT: IMAGE + BOXES -->
      <div style="width:55%; position:relative">
        <div style="margin-bottom:8px">
          <button onclick="prevPage()" ${currentPageIndex === 0 ? "disabled" : ""}>◀ Prev</button>
          <button onclick="nextPage()" ${currentPageIndex === pageList.length - 1 ? "disabled" : ""}>Next ▶</button>
          <span style="margin-left:12px">
            Page ${currentPageIndex + 1} / ${pageList.length}
          </span>
        </div>

        <img id="ocrImage"
             src="/jobs/${currentJobId}/cleaned/${page}"
             style="width:100%; border:1px solid #ccc"/>

        <canvas id="bboxCanvas"
                style="position:absolute; top:0; left:0; pointer-events:none;"></canvas>
      </div>
    </div>
  `;

  if (jobDone) {
    await loadRowView(page.replace(".png", ""));
  }

}


/* ================================
   OCR DATA
================================ */
async function loadOCR(pageName) {
  const res = await fetch(`/jobs/${currentJobId}/ocr/${pageName}`);
  ocrItems = await res.json();

  renderOCRTable();
  setTimeout(drawBoxes, 200); // wait for image render
}

function renderOCRTable() {
  const tbody = document.querySelector("#ocrTable tbody");
  tbody.innerHTML = "";

  ocrItems.forEach(item => {
    const tr = document.createElement("tr");
    tr.dataset.id = item.id;
    tr.style.cursor = "pointer";

    tr.innerHTML = `
      <td>${item.id}</td>
      <td>${item.text}</td>
      <td>${item.confidence.toFixed(2)}</td>
    `;

    tr.onclick = () => highlight(item.id);
    tbody.appendChild(tr);
  });
}


/* ================================
   BOUNDING BOX DRAWING
================================ */
function drawBoxes() {
  const img = document.getElementById("ocrImage");
  const canvas = document.getElementById("bboxCanvas");
  const ctx = canvas.getContext("2d");

  const scaleX = img.clientWidth / img.naturalWidth;
  const scaleY = img.clientHeight / img.naturalHeight;

  canvas.width = img.clientWidth;
  canvas.height = img.clientHeight;

  ctx.clearRect(0, 0, canvas.width, canvas.height);

  ocrItems.forEach(item => {
    const pts = item.bbox.map(p => [
      p[0] * scaleX,
      p[1] * scaleY
    ]);

    ctx.strokeStyle = item.id === highlightedId ? "lime" : "red";
    ctx.lineWidth = item.id === highlightedId ? 3 : 1;

    ctx.beginPath();
    ctx.moveTo(pts[0][0], pts[0][1]);
    pts.forEach(p => ctx.lineTo(p[0], p[1]));
    ctx.closePath();
    ctx.stroke();

    ctx.fillStyle = ctx.strokeStyle;
    ctx.font = "12px sans-serif";
    ctx.fillText(item.id, pts[0][0] + 2, pts[0][1] - 2);
  });
}


/* ================================
   SYNC HIGHLIGHT
================================ */
function highlight(id) {
  highlightedId = id;

  // Highlight table row
  document.querySelectorAll("#ocrTable tbody tr").forEach(tr => {
    tr.style.background = tr.dataset.id == id ? "#d4f7d4" : "";
  });

  drawBoxes();
}


let parsedRows = [];
let rowBounds = [];
let activeRowId = null;

async function loadRowView(pageName) {
  setRowTableHeader();

  const [parsedRes, boundsRes] = await Promise.all([
    fetch(`/jobs/${currentJobId}/parsed/${pageName}`),
    fetch(`/jobs/${currentJobId}/rows/${pageName}/bounds`)
  ]);

  parsedRows = await parsedRes.json();
  rowBounds = await boundsRes.json();

  console.log("parsedRows:", parsedRows);
  console.log("rowBounds:", rowBounds);


  renderRowTable();
  setTimeout(drawRowBoxes, 200);
}




function renderRowTable() {
  const tbody = document.querySelector("#ocrTable tbody");
  tbody.innerHTML = "";

  parsedRows.forEach(row => {
    const tr = document.createElement("tr");
    tr.dataset.rowId = row.row_id;
    tr.style.cursor = "pointer";

    tr.innerHTML = `
      <td>${row.row_id}</td>
      <td>${row.date || ""}</td>
      <td>${row.description || ""}</td>
      <td>${row.debit || ""}</td>
      <td>${row.credit || ""}</td>
      <td>${row.balance || ""}</td>
    `;

    tr.onclick = () => selectRow(row.row_id);
    tbody.appendChild(tr);
  });
}


function drawRowBoxes() {
  const img = document.getElementById("ocrImage");
  const canvas = document.getElementById("bboxCanvas");
  const ctx = canvas.getContext("2d");

  const scaleY = img.clientHeight / img.naturalHeight;

  canvas.width = img.clientWidth;
  canvas.height = img.clientHeight;
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  rowBounds.forEach(r => {
    const y = r.y1 * scaleY;
    const h = (r.y2 - r.y1) * scaleY;

    ctx.strokeStyle = r.row_id === activeRowId ? "lime" : "red";
    ctx.lineWidth = r.row_id === activeRowId ? 3 : 1;

    ctx.strokeRect(0, y, canvas.width, h);

    ctx.fillStyle = ctx.strokeStyle;
    ctx.font = "12px sans-serif";
    ctx.fillText(r.row_id, 4, y + 14);
  });
}

function selectRow(rowId) {
  activeRowId = rowId;

  document.querySelectorAll("#ocrTable tbody tr").forEach(tr => {
    tr.style.background =
      tr.dataset.rowId == rowId ? "#d4f7d4" : "";
  });

  drawRowBoxes();
}

function setRowTableHeader() {
  document.getElementById("ocrHeader").innerHTML = `
    <tr>
      <th>#</th>
      <th>Date</th>
      <th>Description</th>
      <th>Debit</th>
      <th>Credit</th>
      <th>Balance</th>
    </tr>
  `;
}

