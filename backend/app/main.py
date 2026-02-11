from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
import uuid, os, json
from typing import List, Dict
import cv2
import numpy as np
import math
from pdf2image import convert_from_path
from PIL import Image

from app.celery_app import process_pdf, prepare_draft
from app.bank_profiles import detect_bank_profile, extract_account_identity, find_value_bounds, reload_profiles
from app.ocr_engine import ocr_image
from app.pdf_text_extract import extract_pdf_layout_pages
from app.statement_parser import parse_page_with_profile_fallback, is_transaction_row
from app.image_cleaner import clean_page

DATA_DIR = os.getenv("DATA_DIR", "/data")
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))

app = FastAPI(title="OCR Passbook / SOA API")


def _normalize_parse_mode(mode: str | None) -> str:
    return "ocr" if str(mode or "").strip().lower() == "ocr" else "text"

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
async def create_job(file: UploadFile = File(...), mode: str = Form("text")):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")
    parse_mode = _normalize_parse_mode(mode)

    job_id = str(uuid.uuid4())
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_dir = os.path.join(job_dir, "input")

    os.makedirs(input_dir, exist_ok=True)

    pdf_path = os.path.join(input_dir, "document.pdf")
    with open(pdf_path, "wb") as f:
        f.write(await file.read())

    # Initial status (atomic write happens in worker later)
    with open(os.path.join(job_dir, "status.json"), "w") as f:
        json.dump({"status": "queued", "step": "queued", "progress": 0, "parse_mode": parse_mode}, f)

    print(f"[API] Created job {job_id}")

    process_pdf.delay(job_id, parse_mode)

    return {"job_id": job_id, "parse_mode": parse_mode}


@app.post("/jobs/draft")
async def create_draft_job(file: UploadFile = File(...), mode: str = Form("text")):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")
    parse_mode = _normalize_parse_mode(mode)

    job_id = str(uuid.uuid4())
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_dir = os.path.join(job_dir, "input")
    pages_dir = os.path.join(job_dir, "pages")
    cleaned_dir = os.path.join(job_dir, "cleaned")
    ocr_dir = os.path.join(job_dir, "ocr")
    result_dir = os.path.join(job_dir, "result")

    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(pages_dir, exist_ok=True)
    os.makedirs(cleaned_dir, exist_ok=True)
    os.makedirs(ocr_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)

    pdf_path = os.path.join(input_dir, "document.pdf")
    with open(pdf_path, "wb") as f:
        f.write(await file.read())

    with open(os.path.join(job_dir, "status.json"), "w") as f:
        json.dump(
            {
                "status": "queued",
                "step": "draft_queued",
                "progress": 1,
                "ocr_backend": "easyocr",
                "parse_mode": parse_mode,
            },
            f,
        )

    prepare_draft.delay(job_id)
    return {"job_id": job_id}


@app.post("/jobs/{job_id}/start")
def start_job_from_draft(job_id: str):
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    if not os.path.exists(input_pdf):
        raise HTTPException(status_code=404, detail="Draft job not found")

    parse_mode = "text"
    status_path = os.path.join(job_dir, "status.json")
    if os.path.exists(status_path):
        try:
            with open(status_path) as f:
                parse_mode = _normalize_parse_mode(json.load(f).get("parse_mode"))
        except Exception:
            parse_mode = "text"

    with open(status_path, "w") as f:
        json.dump({"status": "queued", "step": "queued", "progress": 0, "parse_mode": parse_mode}, f)

    process_pdf.delay(job_id, parse_mode)
    return {"job_id": job_id, "started": True, "parse_mode": parse_mode}


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
        return {"status": "processing", "step": "processing", "progress": 0}


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
    if files:
        return {"pages": files}

    parsed_rows_path = os.path.join(DATA_DIR, "jobs", job_id, "result", "parsed_rows.json")
    if os.path.exists(parsed_rows_path):
        try:
            with open(parsed_rows_path) as f:
                parsed = json.load(f)
            page_keys = sorted(parsed.keys())
            synthetic = [f"{key}.png" for key in page_keys]
            return {"pages": synthetic}
        except Exception:
            pass

    return {"pages": files}

@app.get("/jobs/{job_id}/cleaned/{filename}")
def get_cleaned(job_id: str, filename: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "cleaned", filename)

    if not os.path.exists(path):
        generated = _generate_preview_page_if_missing(job_id, filename, path)
        if not generated:
            raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(path, media_type="image/png")


@app.get("/jobs/{job_id}/preview/{page}")
def get_page_preview(job_id: str, page: str):
    page_name = page if page.startswith("page_") else f"page_{page}"
    filename = f"{page_name}.png" if not page_name.endswith(".png") else page_name
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    cleaned_path = os.path.join(job_dir, "cleaned", filename)
    preview_dir = os.path.join(job_dir, "preview")
    preview_path = os.path.join(preview_dir, filename)

    if os.path.exists(cleaned_path):
        return FileResponse(cleaned_path, media_type="image/png")

    if not os.path.exists(preview_path):
        ok = _generate_preview_page_if_missing(job_id, filename, preview_path)
        if not ok:
            raise HTTPException(status_code=404, detail="Preview page not found")

    return FileResponse(preview_path, media_type="image/png")

@app.get("/jobs/{job_id}/ocr/{page}")
def get_ocr(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "ocr", f"{page}.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="OCR not ready")

    with open(path) as f:
        return json.load(f)
    

@app.get("/jobs/{job_id}/rows/{page}/bounds")
def get_row_bounds(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "bounds.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Row bounds not ready")

    with open(path) as f:
        data = json.load(f)

    return data.get(page, [])


@app.get("/jobs/{job_id}/bounds")
def get_all_row_bounds(job_id: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "bounds.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Row bounds not ready")

    with open(path) as f:
        return json.load(f)

@app.get("/jobs/{job_id}/rows/{page}/{row}")
def get_row_image(job_id: str, page: str, row: str):
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/parsed/{page}."
    )




@app.get("/jobs/{job_id}/ocr/rows")
def get_row_ocr(job_id: str):
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/ocr/{page}."
    )

@app.get("/jobs/{job_id}/rows/{page}")
def list_rows(job_id: str, page: str):
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/parsed/{page}."
    )

@app.get("/jobs/{job_id}/parsed/{page}")
def get_parsed_rows(job_id: str, page: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "parsed_rows.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Parsed rows not ready")

    with open(path) as f:
        data = json.load(f)

    page_rows = data.get(page, [])
    if _rows_need_description_backfill(page_rows):
        page_rows = _backfill_page_descriptions(job_id, page, page_rows)
        data[page] = page_rows
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    return page_rows


@app.get("/jobs/{job_id}/parsed")
def get_all_parsed_rows(job_id: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "parsed_rows.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Parsed rows not ready")

    with open(path) as f:
        return json.load(f)


@app.get("/jobs/{job_id}/diagnostics")
def get_parse_diagnostics(job_id: str):
    path = os.path.join(DATA_DIR, "jobs", job_id, "result", "parse_diagnostics.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Diagnostics not ready")

    with open(path) as f:
        return json.load(f)


class FlattenPoint(BaseModel):
    x: float = Field(ge=0.0, le=1.0)
    y: float = Field(ge=0.0, le=1.0)


class FlattenRequest(BaseModel):
    points: List[FlattenPoint]


@app.post("/jobs/{job_id}/pages/{page}/flatten")
def flatten_page(job_id: str, page: str, payload: FlattenRequest):
    if len(payload.points) != 4:
        raise HTTPException(status_code=400, detail="Exactly 4 points are required")

    cleaned_path = os.path.join(DATA_DIR, "jobs", job_id, "cleaned", f"{page}.png")
    if not os.path.exists(cleaned_path):
        raise HTTPException(status_code=404, detail="Page image not found")

    img = cv2.imread(cleaned_path)
    if img is None:
        raise HTTPException(status_code=400, detail="Unable to read page image")

    warped = _warp_by_points(img, payload.points)
    if warped is None:
        raise HTTPException(status_code=400, detail="Invalid corner points")

    cv2.imwrite(cleaned_path, warped)
    if _should_reparse_after_page_edit(job_id):
        _reparse_single_page(job_id, page)
    return {"ok": True, "page": page}


@app.post("/jobs/{job_id}/pages/{page}/flatten/reset")
def reset_flatten_page(job_id: str, page: str):
    raw_path = os.path.join(DATA_DIR, "jobs", job_id, "pages", f"{page}.png")
    cleaned_path = os.path.join(DATA_DIR, "jobs", job_id, "cleaned", f"{page}.png")

    if not os.path.exists(raw_path):
        raise HTTPException(status_code=404, detail="Original page not found")

    restored = clean_page(raw_path)
    cv2.imwrite(cleaned_path, restored)
    if _should_reparse_after_page_edit(job_id):
        _reparse_single_page(job_id, page)
    return {"ok": True, "page": page}


def _should_reparse_after_page_edit(job_id: str) -> bool:
    status_path = os.path.join(DATA_DIR, "jobs", job_id, "status.json")
    if not os.path.exists(status_path):
        return False
    try:
        with open(status_path) as f:
            status = json.load(f).get("status")
    except Exception:
        return False
    return status in {"processing", "done", "failed"}


def _read_job_parse_mode(job_dir: str) -> str:
    status_path = os.path.join(job_dir, "status.json")
    if not os.path.exists(status_path):
        return "text"
    try:
        with open(status_path) as f:
            data = json.load(f)
        return _normalize_parse_mode(data.get("parse_mode"))
    except Exception:
        return "text"


def _reparse_single_page(job_id: str, page: str):
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    reload_profiles()
    parse_mode = _read_job_parse_mode(job_dir)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    cleaned_path = os.path.join(job_dir, "cleaned", f"{page}.png")
    ocr_path = os.path.join(job_dir, "ocr", f"{page}.json")
    parsed_path = os.path.join(job_dir, "result", "parsed_rows.json")
    bounds_path = os.path.join(job_dir, "result", "bounds.json")
    diagnostics_path = os.path.join(job_dir, "result", "parse_diagnostics.json")

    img = cv2.imread(cleaned_path)
    if img is None:
        raise HTTPException(status_code=400, detail="Unable to read cleaned page")
    page_h, page_w = img.shape[:2]

    parser_words = []
    parser_w = page_w
    parser_h = page_h
    profile_text = ""
    source_type = "text"

    try:
        if os.path.exists(input_pdf):
            layouts = extract_pdf_layout_pages(input_pdf)
            page_num = int(str(page).replace("page_", ""))
            if 0 < page_num <= len(layouts):
                layout = layouts[page_num - 1]
                parser_words = layout.get("words", []) if isinstance(layout, dict) else []
                parser_w = float(layout.get("width", page_w)) if isinstance(layout, dict) else page_w
                parser_h = float(layout.get("height", page_h)) if isinstance(layout, dict) else page_h
                profile_text = layout.get("text", "") if isinstance(layout, dict) else ""
    except Exception:
        parser_words = []
        parser_w = page_w
        parser_h = page_h
        profile_text = ""

    profile = detect_bank_profile(profile_text)
    page_rows, page_bounds, diag = parse_page_with_profile_fallback(parser_words, parser_w, parser_h, profile)

    ocr_items = []
    if parse_mode == "ocr":
        source_type = "ocr"
        ocr_items = ocr_image(cleaned_path, backend="easyocr")
        ocr_words = _ocr_items_to_words(ocr_items)
        ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
        profile = detect_bank_profile(ocr_text or profile_text)
        page_rows, page_bounds, diag = parse_page_with_profile_fallback(ocr_words, page_w, page_h, profile)

    transaction_rows = [row for row in page_rows if is_transaction_row(row, profile)]
    id_map: Dict[str, str] = {}
    for n, row in enumerate(transaction_rows, start=1):
        old_id = str(row.get("row_id") or "")
        new_id = f"{n:03}"
        row["row_id"] = new_id
        id_map[old_id] = new_id

    filtered_bounds = []
    for b in page_bounds:
        old_id = str(b.get("row_id") or "")
        if old_id not in id_map:
            continue
        b["row_id"] = id_map[old_id]
        filtered_bounds.append(b)

    filtered_rows = []
    for row in transaction_rows:
        filtered_rows.append(
            {
                "row_id": row.get("row_id"),
                "date": row.get("date"),
                "description": row.get("description"),
                "debit": row.get("debit"),
                "credit": row.get("credit"),
                "balance": row.get("balance"),
            }
        )

    parsed_data: Dict[str, List[Dict]] = {}
    if os.path.exists(parsed_path):
        with open(parsed_path) as f:
            parsed_data = json.load(f)
    parsed_data[page] = filtered_rows
    with open(parsed_path, "w") as f:
        json.dump(parsed_data, f, indent=2)

    bounds_data: Dict[str, List[Dict]] = {}
    if os.path.exists(bounds_path):
        with open(bounds_path) as f:
            bounds_data = json.load(f)
    bounds_data[page] = filtered_bounds
    with open(bounds_path, "w") as f:
        json.dump(bounds_data, f, indent=2)

    with open(ocr_path, "w") as f:
        json.dump(ocr_items, f, indent=2)

    diagnostics_data = {"job": {"ocr_backend": "easyocr", "parse_mode": parse_mode}, "pages": {}}
    if os.path.exists(diagnostics_path):
        with open(diagnostics_path) as f:
            diagnostics_data = json.load(f)
    account_identity = extract_account_identity(profile_text, profile)
    account_name_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_identity.get("account_name"), page)
    account_number_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_identity.get("account_number"), page)
    if parse_mode == "ocr" and ocr_items:
        ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
        account_identity = extract_account_identity(ocr_text, profile)
        ocr_words = _ocr_items_to_words(ocr_items)
        account_name_bbox = find_value_bounds(ocr_words, page_w, page_h, account_identity.get("account_name"), page)
        account_number_bbox = find_value_bounds(ocr_words, page_w, page_h, account_identity.get("account_number"), page)
    diagnostics_data["job"]["account_name"] = account_identity.get("account_name")
    diagnostics_data["job"]["account_number"] = account_identity.get("account_number")
    diagnostics_data["job"]["account_name_bbox"] = account_name_bbox
    diagnostics_data["job"]["account_number_bbox"] = account_number_bbox

    diagnostics_pages = diagnostics_data.setdefault("pages", {})
    diagnostics_pages[page] = {
        "source_type": source_type,
        "ocr_backend": "easyocr",
        "parse_mode": parse_mode,
        "bank_profile": profile.name,
        "ocr_items": len(ocr_items),
        "rows_parsed": len(filtered_rows),
        "profile_detected": diag.get("profile_detected", profile.name),
        "profile_selected": diag.get("profile_selected", profile.name),
        "fallback_applied": bool(diag.get("fallback_applied", False)),
        "fallback_reason": diag.get("fallback_reason"),
        "manual_flatten": True,
    }
    with open(diagnostics_path, "w") as f:
        json.dump(diagnostics_data, f, indent=2)


def _rows_need_description_backfill(rows: List[Dict]) -> bool:
    if not rows:
        return False
    for row in rows:
        if "description" not in row or not str(row.get("description") or "").strip():
            return True
    return False


def _backfill_page_descriptions(job_id: str, page: str, page_rows: List[Dict]) -> List[Dict]:
    if not page_rows:
        return page_rows

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    cleaned_path = os.path.join(job_dir, "cleaned", f"{page}.png")
    ocr_path = os.path.join(job_dir, "ocr", f"{page}.json")

    row_desc: Dict[str, str] = {}

    try:
        page_num = int(str(page).replace("page_", ""))
    except Exception:
        page_num = 0

    # Prefer text-layer parse when available for better descriptions.
    if os.path.exists(input_pdf) and page_num > 0:
        try:
            layouts = extract_pdf_layout_pages(input_pdf)
            if 0 < page_num <= len(layouts):
                layout = layouts[page_num - 1]
                layout_words = layout.get("words", []) if isinstance(layout, dict) else []
                profile = detect_bank_profile(layout.get("text", "") if isinstance(layout, dict) else "")
                parsed, _, _ = parse_page_with_profile_fallback(
                    layout_words,
                    float(layout.get("width", 1) if isinstance(layout, dict) else 1),
                    float(layout.get("height", 1) if isinstance(layout, dict) else 1),
                    profile,
                )
                parsed = [r for r in parsed if is_transaction_row(r, profile)]
                normalized = {}
                for i, r in enumerate(parsed, start=1):
                    normalized[f"{i:03}"] = r.get("description") or ""
                row_desc = normalized
        except Exception:
            row_desc = {}

    # OCR fallback when text extraction is unavailable.
    if not row_desc and os.path.exists(cleaned_path):
        try:
            img = cv2.imread(cleaned_path)
            if img is not None:
                h, w = img.shape[:2]
                if os.path.exists(ocr_path):
                    with open(ocr_path) as f:
                        ocr_items = json.load(f)
                else:
                    ocr_items = ocr_image(cleaned_path, backend="easyocr")
                ocr_words = _ocr_items_to_words(ocr_items)
                ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
                profile = detect_bank_profile(ocr_text)
                parsed, _, _ = parse_page_with_profile_fallback(ocr_words, w, h, profile)
                parsed = [r for r in parsed if is_transaction_row(r, profile)]
                normalized = {}
                for i, r in enumerate(parsed, start=1):
                    normalized[f"{i:03}"] = r.get("description") or ""
                row_desc = normalized
        except Exception:
            row_desc = {}

    enriched = []
    for row in page_rows:
        rid = str(row.get("row_id") or "")
        existing_desc = str(row.get("description") or "").strip()
        merged = dict(row)
        merged["description"] = existing_desc or row_desc.get(rid, "")
        enriched.append(merged)
    return enriched


def _order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype=np.float32)
    s = pts.sum(axis=1)
    diff = np.diff(pts, axis=1)
    rect[0] = pts[np.argmin(s)]  # top-left
    rect[2] = pts[np.argmax(s)]  # bottom-right
    rect[1] = pts[np.argmin(diff)]  # top-right
    rect[3] = pts[np.argmax(diff)]  # bottom-left
    return rect


def _warp_by_points(img: np.ndarray, points: List[FlattenPoint]) -> np.ndarray | None:
    h, w = img.shape[:2]
    pts = np.array([[p.x * w, p.y * h] for p in points], dtype=np.float32)
    if pts.shape != (4, 2):
        return None

    rect = _order_points(pts)
    (tl, tr, br, bl) = rect

    width_a = np.linalg.norm(br - bl)
    width_b = np.linalg.norm(tr - tl)
    max_w = int(max(width_a, width_b))

    height_a = np.linalg.norm(tr - br)
    height_b = np.linalg.norm(tl - bl)
    max_h = int(max(height_a, height_b))

    if max_w < 10 or max_h < 10:
        return None

    dst = np.array(
        [[0, 0], [max_w - 1, 0], [max_w - 1, max_h - 1], [0, max_h - 1]],
        dtype=np.float32,
    )
    matrix = cv2.getPerspectiveTransform(rect, dst)
    return cv2.warpPerspective(img, matrix, (max_w, max_h))


def _ocr_items_to_words(ocr_items: List[Dict]) -> List[Dict]:
    words = []
    for item in ocr_items:
        bbox = item.get("bbox") or []
        text = (item.get("text") or "").strip()
        if len(bbox) != 4 or not text:
            continue
        xs = [pt[0] for pt in bbox]
        ys = [pt[1] for pt in bbox]
        words.append(
            {
                "text": text,
                "x1": float(min(xs)),
                "y1": float(min(ys)),
                "x2": float(max(xs)),
                "y2": float(max(ys)),
            }
        )
    return words


def _generate_preview_page_if_missing(job_id: str, filename: str, output_path: str) -> bool:
    if not filename.endswith(".png"):
        return False
    if not filename.startswith("page_"):
        return False

    page_token = filename.replace(".png", "").replace("page_", "")
    try:
        page_num = int(page_token)
    except Exception:
        return False
    if page_num <= 0:
        return False

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    if not os.path.exists(input_pdf):
        return False

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        pages = convert_from_path(
            input_pdf,
            dpi=110,
            fmt="png",
            first_page=page_num,
            last_page=page_num,
        )
        if not pages:
            return False
        page = pages[0]
        w, h = page.size
        pixels = max(1, w * h)
        if pixels > PREVIEW_MAX_PIXELS:
            scale = math.sqrt(PREVIEW_MAX_PIXELS / float(pixels))
            page = page.resize(
                (max(1, int(w * scale)), max(1, int(h * scale))),
                resample=Image.Resampling.BILINEAR,
            )
        page.save(output_path, format="PNG")
        return os.path.exists(output_path)
    except Exception:
        return False
