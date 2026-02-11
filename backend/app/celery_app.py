import json
import math
import os
from typing import Dict, List

import cv2
from celery import Celery
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

from app.bank_profiles import detect_bank_profile, extract_account_identity, find_value_bounds
from app.image_cleaner import clean_page
from app.ocr_engine import ocr_image
from app.pdf_text_extract import extract_pdf_layout_pages
from app.statement_parser import parse_page_with_profile_fallback, is_transaction_row


DATA_DIR = os.getenv("DATA_DIR", "/data")
PREVIEW_DPI = int(os.getenv("PREVIEW_DPI", "130"))
PREVIEW_DRAFT_DPI = int(os.getenv("PREVIEW_DRAFT_DPI", "100"))
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))
OCR_BACKEND = "easyocr"

celery = Celery(
    "ocr_worker",
    broker=os.getenv("CELERY_BROKER_URL"),
    backend=os.getenv("CELERY_RESULT_BACKEND"),
)


@celery.task
def prepare_draft(job_id: str):
    print(f"[WORKER] Preparing draft {job_id}")

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    pages_dir = os.path.join(job_dir, "pages")
    cleaned_dir = os.path.join(job_dir, "cleaned")
    ocr_dir = os.path.join(job_dir, "ocr")
    result_dir = os.path.join(job_dir, "result")

    os.makedirs(job_dir, exist_ok=True)
    os.makedirs(pages_dir, exist_ok=True)
    os.makedirs(cleaned_dir, exist_ok=True)
    os.makedirs(ocr_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)

    last_progress = 0

    def report(status: str, step: str, progress: int, **extra):
        nonlocal last_progress
        safe_progress = max(last_progress, min(100, int(progress)))
        last_progress = safe_progress
        update_status(job_dir, status, step=step, progress=safe_progress, **extra)

    try:
        report("processing", "draft_pdf_to_images", 3, ocr_backend=OCR_BACKEND)
        total_pages = _render_pdf_pages(
            input_pdf=input_pdf,
            pages_dir=pages_dir,
            step_name="draft_pdf_to_images",
            progress_start=3,
            progress_end=58,
            report_fn=report,
            dpi=PREVIEW_DRAFT_DPI,
        )

        page_files = sorted(f for f in os.listdir(pages_dir) if f.endswith(".png"))
        report("processing", "draft_image_cleaning", 60, pages=len(page_files), ocr_backend=OCR_BACKEND)

        for i, page_file in enumerate(page_files, start=1):
            src = os.path.join(pages_dir, page_file)
            dst = os.path.join(cleaned_dir, page_file)
            cleaned = clean_page(src)
            cv2.imwrite(dst, cleaned)
            report(
                "processing",
                "draft_image_cleaning",
                60 + int((i / max(len(page_files), 1)) * 38),
                pages=total_pages,
                ocr_backend=OCR_BACKEND,
            )

        with open(os.path.join(job_dir, "preprocess.json"), "w") as f:
            json.dump({"use_existing_cleaned": True}, f)

        update_status(
            job_dir,
            "draft",
            step="ready_for_edit",
            progress=100,
            pages=len(page_files),
            ocr_backend=OCR_BACKEND,
        )
        print(f"[WORKER] Draft ready {job_id}")
        return {"pages": len(page_files)}
    except Exception as exc:
        update_status(
            job_dir,
            "failed",
            step="failed",
            progress=last_progress,
            message=str(exc),
            ocr_backend=OCR_BACKEND,
        )
        print(f"[WORKER] Draft failed {job_id}: {exc}")
        raise


@celery.task
def process_pdf(job_id: str, parse_mode: str = "text"):
    print(f"[WORKER] Starting job {job_id}")
    parse_mode = _normalize_parse_mode(parse_mode)

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")

    os.makedirs(job_dir, exist_ok=True)

    pages_dir = os.path.join(job_dir, "pages")
    cleaned_dir = os.path.join(job_dir, "cleaned")
    ocr_dir = os.path.join(job_dir, "ocr")
    result_dir = os.path.join(job_dir, "result")

    os.makedirs(pages_dir, exist_ok=True)
    os.makedirs(cleaned_dir, exist_ok=True)
    os.makedirs(ocr_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)

    last_progress = 0

    def report(status: str, step: str, progress: int, **extra):
        nonlocal last_progress
        safe_progress = max(last_progress, min(100, int(progress)))
        last_progress = safe_progress
        update_status(job_dir, status, step=step, progress=safe_progress, parse_mode=parse_mode, **extra)

    try:
        parsed_output: Dict[str, List[Dict]] = {}
        bounds_output: Dict[str, List[Dict]] = {}
        layout_pages: List[Dict] = []
        text_extract_error = None
        try:
            layout_pages = extract_pdf_layout_pages(input_pdf)
        except Exception as exc:
            text_extract_error = str(exc)
            layout_pages = []

        preprocess_cfg = _read_preprocess_config(job_dir)
        use_existing_cleaned = bool(preprocess_cfg.get("use_existing_cleaned"))
        existing_pages = sorted(f for f in os.listdir(pages_dir) if f.endswith(".png"))
        existing_cleaned = sorted(f for f in os.listdir(cleaned_dir) if f.endswith(".png"))

        if parse_mode == "ocr":
            full_preview_pipeline = bool(use_existing_cleaned and existing_pages and existing_cleaned)
            if full_preview_pipeline:
                page_files = existing_pages
                report("processing", "pdf_to_images", 25, pages=len(page_files), ocr_backend=OCR_BACKEND)
                report("processing", "image_cleaning", 45, pages=len(page_files), ocr_backend=OCR_BACKEND)
            else:
                report("processing", "pdf_to_images", 5, ocr_backend=OCR_BACKEND)
                total_pages = _render_pdf_pages(
                    input_pdf=input_pdf,
                    pages_dir=pages_dir,
                    step_name="pdf_to_images",
                    progress_start=5,
                    progress_end=25,
                    report_fn=report,
                    dpi=PREVIEW_DPI,
                )
                page_files = [f"page_{i:03}.png" for i in range(1, total_pages + 1)]
                report("processing", "image_cleaning", 25, pages=len(page_files), ocr_backend=OCR_BACKEND)
                for i, page_file in enumerate(page_files, start=1):
                    src = os.path.join(pages_dir, page_file)
                    dst = os.path.join(cleaned_dir, page_file)
                    cleaned = clean_page(src)
                    cv2.imwrite(dst, cleaned)
                    report(
                        "processing",
                        "image_cleaning",
                        25 + int((i / max(len(page_files), 1)) * 20),
                        pages=len(page_files),
                        ocr_backend=OCR_BACKEND,
                    )
        else:
            if layout_pages:
                page_files = [f"page_{i:03}.png" for i in range(1, len(layout_pages) + 1)]
            else:
                try:
                    info = pdfinfo_from_path(input_pdf)
                    total_pages = int(info.get("Pages") or 0)
                except Exception:
                    total_pages = 0
                page_files = [f"page_{i:03}.png" for i in range(1, max(total_pages, 0) + 1)]
            report("processing", "text_extraction", 45, pages=len(page_files), ocr_backend=OCR_BACKEND)

        account_name = None
        account_number = None
        account_name_bbox = None
        account_number_bbox = None
        for layout in layout_pages[:5]:
            text = (layout or {}).get("text", "")
            if not text:
                continue
            profile = detect_bank_profile(text)
            account_identity = extract_account_identity(text, profile)
            if not account_name:
                account_name = account_identity.get("account_name")
            if not account_number:
                account_number = account_identity.get("account_number")
            if account_name and account_number:
                break

        diagnostics: Dict[str, Dict] = {
            "job": {
                "ocr_backend": OCR_BACKEND,
                "parse_mode": parse_mode,
                "pages": len(page_files),
                "text_extract_error": text_extract_error,
                "account_name": account_name,
                "account_number": account_number,
                "account_name_bbox": account_name_bbox,
                "account_number_bbox": account_number_bbox,
            },
            "pages": {},
        }

        for idx, page_file in enumerate(page_files, start=1):
            page_name = page_file.replace(".png", "")
            page_path = os.path.join(cleaned_dir, page_file)

            report(
                "processing",
                "page_ocr" if parse_mode == "ocr" else "page_text",
                46 + int((idx / max(len(page_files), 1)) * 48),
                pages=len(page_files),
                page=page_name,
                ocr_backend=OCR_BACKEND,
            )

            layout = layout_pages[idx - 1] if idx - 1 < len(layout_pages) else None
            profile_text = layout.get("text") if layout else ""
            profile = detect_bank_profile(profile_text)

            ocr_items = []
            source_type = "ocr" if parse_mode == "ocr" else "text"
            source_reason = None
            parser_words = layout.get("words", []) if layout else []
            parser_w = float(layout.get("width", 1)) if layout else 1.0
            parser_h = float(layout.get("height", 1)) if layout else 1.0
            page_w = int(parser_w) if parser_w > 1 else 1
            page_h = int(parser_h) if parser_h > 1 else 1

            if parse_mode == "ocr":
                img = cv2.imread(page_path)
                if img is None:
                    img = _ensure_cleaned_page(input_pdf, page_path, os.path.join(pages_dir, page_file), idx)
                if img is None:
                    parsed_output[page_name] = []
                    bounds_output[page_name] = []
                    diagnostics["pages"][page_name] = {
                        "source_type": "none",
                        "ocr_backend": OCR_BACKEND,
                        "fallback_reason": "image_read_failed",
                        "rows_parsed": 0,
                    }
                    with open(os.path.join(ocr_dir, f"{page_name}.json"), "w") as f:
                        json.dump([], f, indent=2)
                    continue
                page_h, page_w = img.shape[:2]
                ocr_items = ocr_image(page_path, backend=OCR_BACKEND)
                ocr_words = _ocr_items_to_words(ocr_items)
                ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
                profile = detect_bank_profile(ocr_text or profile_text)
                if not account_name or not account_number:
                    account_identity = extract_account_identity(ocr_text, profile)
                    if not account_name:
                        account_name = account_identity.get("account_name")
                    if not account_number:
                        account_number = account_identity.get("account_number")
                if not account_name_bbox and account_name:
                    account_name_bbox = find_value_bounds(ocr_words, page_w, page_h, account_name, page_name)
                if not account_number_bbox and account_number:
                    account_number_bbox = find_value_bounds(ocr_words, page_w, page_h, account_number, page_name)
                page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
                    ocr_words,
                    page_w,
                    page_h,
                    profile,
                )
            else:
                if not account_name or not account_number:
                    account_identity = extract_account_identity(profile_text, profile)
                    if not account_name:
                        account_name = account_identity.get("account_name")
                    if not account_number:
                        account_number = account_identity.get("account_number")
                if not account_name_bbox and account_name:
                    account_name_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_name, page_name)
                if not account_number_bbox and account_number:
                    account_number_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_number, page_name)
                page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
                    parser_words,
                    parser_w,
                    parser_h,
                    profile,
                )

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

            parsed_output[page_name] = filtered_rows
            bounds_output[page_name] = filtered_bounds
            diagnostics["pages"][page_name] = {
                "source_type": source_type,
                "ocr_backend": OCR_BACKEND,
                "parse_mode": parse_mode,
                "bank_profile": profile.name,
                "ocr_items": len(ocr_items),
                "rows_parsed": len(filtered_rows),
                "profile_detected": parser_diag.get("profile_detected", profile.name),
                "profile_selected": parser_diag.get("profile_selected", profile.name),
                "fallback_applied": bool(parser_diag.get("fallback_applied", False)),
                "fallback_reason": parser_diag.get("fallback_reason") or source_reason,
            }

            diagnostics["job"]["account_name"] = account_name
            diagnostics["job"]["account_number"] = account_number
            diagnostics["job"]["account_name_bbox"] = account_name_bbox
            diagnostics["job"]["account_number_bbox"] = account_number_bbox

            with open(os.path.join(ocr_dir, f"{page_name}.json"), "w") as f:
                json.dump(ocr_items, f, indent=2)

        report("processing", "saving_results", 95, pages=len(page_files), ocr_backend=OCR_BACKEND)

        with open(os.path.join(result_dir, "parsed_rows.json"), "w") as f:
            json.dump(parsed_output, f, indent=2)
        with open(os.path.join(result_dir, "bounds.json"), "w") as f:
            json.dump(bounds_output, f, indent=2)
        with open(os.path.join(result_dir, "parse_diagnostics.json"), "w") as f:
            json.dump(diagnostics, f, indent=2)

        report("done", "completed", 100, pages=len(page_files), ocr_backend=OCR_BACKEND)
        print(f"[WORKER] Finished job {job_id}")
        return {"pages": len(page_files)}
    except Exception as exc:
        update_status(
            job_dir,
            "failed",
            step="failed",
            progress=last_progress,
            message=str(exc),
            parse_mode=parse_mode,
            ocr_backend=OCR_BACKEND,
        )
        print(f"[WORKER] Failed job {job_id}: {exc}")
        raise


def update_status(job_dir, status, **extra):
    tmp_path = os.path.join(job_dir, "status.tmp")
    final_path = os.path.join(job_dir, "status.json")

    data = {"status": status, **extra}

    with open(tmp_path, "w") as f:
        json.dump(data, f)

    os.replace(tmp_path, final_path)


def _normalize_parse_mode(mode: str | None) -> str:
    return "ocr" if str(mode or "").strip().lower() == "ocr" else "text"


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


def _read_preprocess_config(job_dir: str) -> Dict:
    cfg_path = os.path.join(job_dir, "preprocess.json")
    if not os.path.exists(cfg_path):
        return {}
    try:
        with open(cfg_path) as f:
            return json.load(f)
    except Exception:
        return {}


def _render_pdf_pages(
    input_pdf: str,
    pages_dir: str,
    step_name: str,
    progress_start: int,
    progress_end: int,
    report_fn,
    dpi: int,
) -> int:
    span = max(1, progress_end - progress_start)
    total_pages = 0
    try:
        info = pdfinfo_from_path(input_pdf)
        total_pages = int(info.get("Pages") or 0)
    except Exception:
        total_pages = 0

    if total_pages <= 0:
        pages = convert_from_path(input_pdf, dpi=dpi, fmt="png")
        total_pages = max(1, len(pages))
        for i, page in enumerate(pages, start=1):
            _save_preview_page(page, os.path.join(pages_dir, f"page_{i:03}.png"))
            report_fn(
                "processing",
                step_name,
                progress_start + int((i / total_pages) * span),
                pages=total_pages,
                ocr_backend=OCR_BACKEND,
            )
        return total_pages

    for i in range(1, total_pages + 1):
        page_list = convert_from_path(
            input_pdf,
            dpi=dpi,
            fmt="png",
            first_page=i,
            last_page=i,
        )
        if page_list:
            _save_preview_page(page_list[0], os.path.join(pages_dir, f"page_{i:03}.png"))
        report_fn(
            "processing",
            step_name,
            progress_start + int((i / total_pages) * span),
            pages=total_pages,
            ocr_backend=OCR_BACKEND,
        )
    return total_pages


def _ensure_cleaned_page(input_pdf: str, cleaned_path: str, raw_path: str, page_num: int):
    try:
        page_list = convert_from_path(
            input_pdf,
            dpi=PREVIEW_DPI,
            fmt="png",
            first_page=page_num,
            last_page=page_num,
        )
        if not page_list:
            return None
        _save_preview_page(page_list[0], raw_path)
        cleaned = clean_page(raw_path)
        cv2.imwrite(cleaned_path, cleaned)
        return cv2.imread(cleaned_path)
    except Exception:
        return None


def _save_preview_page(page: Image.Image, page_path: str):
    w, h = page.size
    pixels = max(1, w * h)
    if pixels > PREVIEW_MAX_PIXELS:
        scale = math.sqrt(PREVIEW_MAX_PIXELS / float(pixels))
        page = page.resize(
            (max(1, int(w * scale)), max(1, int(h * scale))),
            resample=Image.Resampling.BILINEAR,
        )
    page.save(page_path)
