from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import uuid, os, json
from app.celery_app import process_pdf

DATA_DIR = os.getenv("DATA_DIR", "/data")

app = FastAPI(title="OCR Passbook / SOA API")

# ---------------------------
# Static files & templates
# ---------------------------
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


# ---------------------------
# UI
# ---------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


# ---------------------------
# Health
# ---------------------------
@app.get("/health")
def health():
    return {"ok": True}


# ---------------------------
# Job creation
# ---------------------------
@app.post("/jobs")
async def create_job(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")

    job_id = str(uuid.uuid4())
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_dir = os.path.join(job_dir, "input")

    os.makedirs(input_dir, exist_ok=True)

    pdf_path = os.path.join(input_dir, "document.pdf")
    with open(pdf_path, "wb") as f:
        f.write(await file.read())

    # Initial status (atomic write happens in worker later)
    with open(os.path.join(job_dir, "status.json"), "w") as f:
        json.dump({"status": "queued"}, f)

    print(f"[API] Created job {job_id}")

    process_pdf.delay(job_id)

    return {"job_id": job_id}


# ---------------------------
# Job status
# ---------------------------
@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    status_path = os.path.join(DATA_DIR, "jobs", job_id, "status.json")

    if not os.path.exists(status_path):
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        with open(status_path) as f:
            return json.load(f)
    except json.JSONDecodeError:
        # Status file mid-write
        return {"status": "processing"}


# ---------------------------
# RAW pages
# ------------

@app.get("/jobs/{job_id}/cleaned")
def list_cleaned(job_id: str):
    print("HIT /cleaned endpoint")
    cleaned_dir = os.path.join(DATA_DIR, "jobs", job_id, "cleaned")
    
    # 🔥 IMPORTANT: do NOT 404
    if not os.path.exists(cleaned_dir):
        return {"pages": []}

    files = sorted(
        f for f in os.listdir(cleaned_dir)
        if f.endswith(".png")
    )
    return {"pages": files}

@app.get("/jobs/{job_id}/cleaned/{filename}")
def get_cleaned(job_id: str, filename: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "cleaned", filename)

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(path, media_type="image/png")

@app.get("/jobs/{job_id}/ocr/{page}")
def get_ocr(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "ocr", f"{page}.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="OCR not ready")

    with open(path) as f:
        return json.load(f)
    

@app.get("/jobs/{job_id}/rows/{page}/{row}")
def get_row_image(job_id: str, page: str, row: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "rows", page, row)
    return FileResponse(path, media_type="image/png")

@app.get("/jobs/{job_id}/rows/{page}/bounds")
def get_row_bounds(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "bounds.json")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Row bounds not ready")
    with open(path) as f:
        data = json.load(f)
    return data.get(page, [])


@app.get("/jobs/{job_id}/ocr/rows")
def get_row_ocr(job_id: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "ocr", "rows.json")
    with open(path) as f:
        return json.load(f)

@app.get("/jobs/{job_id}/rows/{page}")
def list_rows(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "rows", page)
    if not os.path.exists(path):
        return {"rows": []}
    return {"rows": sorted(os.listdir(path))}





