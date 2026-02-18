from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
import uuid, os, json, time
import logging
from typing import List, Dict, Optional
import shutil
import cv2
import numpy as np
import math
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from app.celery_app import process_pdf, prepare_draft
from app.bank_profiles import PROFILES, detect_bank_profile, extract_account_identity, find_value_bounds, reload_profiles
from app.ocr_engine import ocr_image
from app.pdf_text_extract import extract_pdf_layout_pages
from app.profile_analyzer import (
    analyze_account_identity_from_text,
    analyze_unknown_bank_and_apply,
    analyze_unknown_bank_and_apply_guided,
)
from app.statement_parser import normalize_amount, normalize_date, parse_page_with_profile_fallback, is_transaction_row
from app.image_cleaner import clean_page
from app.auth_service import (
    AuthUser,
    JWT_SECRET,
    ensure_default_users,
    get_current_user,
    hash_password,
    is_non_dev_env,
    is_weak_jwt_secret,
    issue_token,
    require_role,
    should_seed_default_users,
    verify_password,
)
from app.blob_store import write_blob, blob_abs_path
from app.db import Base, SessionLocal, engine
from app.workflow_models import User, Submission, SubmissionPage, JobRecord, Transaction, Report, AuditLog
from app.workflow_service import (
    assign_submission_to_evaluator,
    can_generate_exports,
    combine_submissions_for_evaluator,
    compute_summary,
    finish_review_and_build_summary,
    create_report_record,
    create_submission_with_job,
    ensure_submission_pages,
    get_submission_for_user,
    get_submission_page,
    get_submission_review_status,
    get_transactions_for_submission,
    list_submissions_for_agent,
    list_submission_pages,
    list_submissions_for_evaluator,
    mark_page_reviewed,
    persist_page_transactions,
    persist_evaluator_transactions,
    normalize_borrower_name,
    serialize_submission,
    serialize_transaction,
    set_page_parse_status,
    set_submission_summary_generated,
    set_submission_summary_ready,
)
from sqlalchemy import select, delete, func

DATA_DIR = os.getenv("DATA_DIR", "./data")
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))
AI_ANALYZER_ENABLED = str(os.getenv("AI_ANALYZER_ENABLED", "true")).strip().lower() not in {"0", "false", "no"}
AI_ANALYZER_PROVIDER = str(os.getenv("AI_ANALYZER_PROVIDER", "gemini")).strip().lower() or "gemini"
AI_ANALYZER_MODEL = str(os.getenv("AI_ANALYZER_MODEL", "gemini-2.5-flash")).strip() or "gemini-2.5-flash"
AI_ANALYZER_SAMPLE_PAGES = int(os.getenv("AI_ANALYZER_SAMPLE_PAGES", "3"))
AI_ANALYZER_MIN_ROWS = int(os.getenv("AI_ANALYZER_MIN_ROWS", "3"))
AI_ANALYZER_MIN_DATE_RATIO = float(os.getenv("AI_ANALYZER_MIN_DATE_RATIO", "0.80"))
AI_ANALYZER_MIN_BAL_RATIO = float(os.getenv("AI_ANALYZER_MIN_BAL_RATIO", "0.80"))
EDITOR_STATE_FILENAME = "editor_state.json"
TEXT_STALE_SECONDS = int(os.getenv("TEXT_STALE_SECONDS", "120"))

app = FastAPI(title="OCR Passbook / SOA API")
logger = logging.getLogger(__name__)


def _normalize_parse_mode(mode: str | None) -> str:
    return "ocr" if str(mode or "").strip().lower() == "ocr" else "text"


@app.on_event("startup")
def _startup_db_bootstrap():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "jobs"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "config"), exist_ok=True)
    app_env = os.getenv("APP_ENV", "dev")
    if is_non_dev_env(app_env) and is_weak_jwt_secret(JWT_SECRET):
        raise RuntimeError("Weak/default JWT_SECRET is not allowed outside dev/local/test environments")
    Base.metadata.create_all(bind=engine)
    if should_seed_default_users(app_env, os.getenv("SEED_DEFAULT_USERS")):
        ensure_default_users()

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
        "login.html",
        {"request": request}
    )


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request}
    )


@app.get("/app", response_class=HTMLResponse)
def app_home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "page_role": "all"}
    )


@app.get("/agent", response_class=HTMLResponse)
def agent_home(request: Request):
    return templates.TemplateResponse(
        "agent.html",
        {"request": request}
    )


@app.get("/evaluator", response_class=HTMLResponse)
def evaluator_home(request: Request):
    return templates.TemplateResponse(
        "evaluator.html",
        {"request": request}
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    return templates.TemplateResponse(
        "admin.html",
        {"request": request}
    )


# ---------------------------
# Health
# ---------------------------
@app.get("/health")
def health():
    return {"ok": True}


def _get_job_record_or_404(job_id: uuid.UUID) -> JobRecord:
    db = SessionLocal()
    try:
        job = db.scalar(select(JobRecord).where(JobRecord.id == job_id))
        if not job:
            raise HTTPException(status_code=404, detail="job_not_found")
        return job
    finally:
        db.close()


def _authorize_submission_access(submission: Submission, user: AuthUser, write: bool = False):
    if user.role == "admin":
        return
    if user.role == "credit_evaluator":
        if submission.assigned_evaluator_id != user.id:
            raise HTTPException(status_code=403, detail="forbidden_submission")
        return
    if user.role == "agent":
        if write:
            raise HTTPException(status_code=403, detail="forbidden_submission_write")
        if submission.agent_id != user.id:
            raise HTTPException(status_code=403, detail="forbidden_submission")
        return
    raise HTTPException(status_code=403, detail="forbidden_role")


def _authorize_job_access(job: JobRecord, user: AuthUser, write: bool = False):
    if job.submission_id is None:
        if user.role not in {"credit_evaluator", "admin"}:
            raise HTTPException(status_code=403, detail="forbidden_standalone_job")
        return

    submission = None
    db = SessionLocal()
    try:
        submission = db.scalar(select(Submission).where(Submission.id == job.submission_id))
        if not submission:
            raise HTTPException(status_code=404, detail="submission_not_found")
    finally:
        db.close()
    _authorize_submission_access(submission, user, write=write)


def _read_submission_original_filename(submission_payload: Dict) -> str:
    job_id = str((submission_payload or {}).get("current_job_id") or "").strip()
    if not job_id:
        return "-"
    meta_path = os.path.join(DATA_DIR, "jobs", job_id, "meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as f:
                meta = json.load(f)
            name = str((meta or {}).get("original_filename") or "").strip()
            if name:
                return name
        except Exception as exc:
            logger.warning("Failed to read submission meta filename: %s", meta_path, exc_info=exc)
    input_pdf_key = str((submission_payload or {}).get("input_pdf_key") or "").strip()
    if input_pdf_key:
        parts = [p for p in input_pdf_key.split("/") if p]
        if parts:
            return parts[-1]
    return "-"


def _read_json_if_exists(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Failed to read JSON file: %s", path, exc_info=exc)
        return default


def _coerce_progress(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _sanitize_editor_guide_state(payload: Dict) -> Dict:
    if not isinstance(payload, dict):
        return {}
    allowed_keys = {"date", "description", "debit", "credit", "balance"}
    columns = []
    for item in payload.get("column_layout") or []:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip().lower()
        if key not in allowed_keys:
            continue
        try:
            width = float(item.get("width"))
        except Exception:
            continue
        if not np.isfinite(width):
            continue
        columns.append({"key": key, "width": max(0.02, width)})
    if columns:
        total = sum(c["width"] for c in columns) or 1.0
        columns = [{"key": c["key"], "width": c["width"] / total} for c in columns]

    horizontal = []
    for item in payload.get("horizontal") or []:
        try:
            value = float(item)
        except Exception:
            continue
        if not np.isfinite(value):
            continue
        if value <= 0.0 or value >= 1.0:
            continue
        horizontal.append(value)
    horizontal = sorted(set(round(v, 6) for v in horizontal))
    return {
        "column_layout": columns,
        "horizontal": horizontal,
    }


def _get_editor_state_path(submission: Submission) -> Optional[str]:
    job_dir = _get_submission_job_dir(submission)
    if not job_dir:
        return None
    return os.path.join(job_dir, "result", EDITOR_STATE_FILENAME)


def _read_page_editor_state(submission: Submission, page_key: str) -> Dict:
    path = _get_editor_state_path(submission)
    if not path:
        return {}
    payload = _read_json_if_exists(path, {})
    if not isinstance(payload, dict):
        return {}
    pages = payload.get("pages")
    if not isinstance(pages, dict):
        return {}
    state = pages.get(page_key)
    return _sanitize_editor_guide_state(state if isinstance(state, dict) else {})


def _write_page_editor_state(submission: Submission, page_key: str, guide_state: Dict) -> None:
    path = _get_editor_state_path(submission)
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = _read_json_if_exists(path, {})
    if not isinstance(payload, dict):
        payload = {}
    pages = payload.get("pages")
    if not isinstance(pages, dict):
        pages = {}
        payload["pages"] = pages
    pages[str(page_key)] = _sanitize_editor_guide_state(guide_state)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, path)


def _sample_detected_profiles(layout_pages: List[Dict], max_pages: int) -> List[str]:
    names: List[str] = []
    for layout in layout_pages:
        text = str((layout or {}).get("text") or "").strip()
        if not text:
            continue
        names.append(detect_bank_profile(text).name)
        if len(names) >= max(1, max_pages):
            break
    return names


def _ocr_items_to_words(ocr_items: List[Dict]) -> List[Dict]:
    words: List[Dict] = []
    for item in ocr_items:
        bbox = item.get("bbox") or []
        text = str(item.get("text") or "").strip()
        if len(bbox) != 4 or not text:
            continue
        try:
            xs = [float(pt[0]) for pt in bbox]
            ys = [float(pt[1]) for pt in bbox]
        except Exception:
            continue
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


def _build_layout_pages_from_ocr(job_dir: str, sample_pages: int) -> List[Dict]:
    cleaned_dir = os.path.join(job_dir, "cleaned")
    pages_dir = os.path.join(job_dir, "pages")
    ocr_dir = os.path.join(job_dir, "ocr")

    page_files: List[str] = []
    if os.path.isdir(cleaned_dir):
        page_files = sorted(f for f in os.listdir(cleaned_dir) if f.endswith(".png"))
    if not page_files and os.path.isdir(pages_dir):
        page_files = sorted(f for f in os.listdir(pages_dir) if f.endswith(".png"))

    layouts: List[Dict] = []
    for page_file in page_files:
        if len(layouts) >= max(1, sample_pages):
            break
        page_key = page_file.replace(".png", "")
        image_path = os.path.join(cleaned_dir, page_file)
        if not os.path.exists(image_path):
            image_path = os.path.join(pages_dir, page_file)
        if not os.path.exists(image_path):
            continue

        ocr_items: List[Dict] = []
        ocr_json_path = os.path.join(ocr_dir, f"{page_key}.json")
        if os.path.exists(ocr_json_path):
            cached = _read_json_if_exists(ocr_json_path, [])
            if isinstance(cached, list):
                ocr_items = cached
        if not ocr_items:
            try:
                ocr_items = ocr_image(image_path, backend="easyocr")
            except Exception as exc:
                logger.warning("OCR sampling failed for analyzer at %s", image_path, exc_info=exc)
                ocr_items = []

        words = _ocr_items_to_words(ocr_items)
        text = " ".join(str(item.get("text") or "").strip() for item in ocr_items if str(item.get("text") or "").strip()).strip()
        if not text and not words:
            continue

        width = 1.0
        height = 1.0
        img = cv2.imread(image_path)
        if img is not None:
            h, w = img.shape[:2]
            width = float(max(w, 1))
            height = float(max(h, 1))
        elif words:
            width = float(max(max((float(w.get("x2", 0.0)) for w in words), default=1.0), 1.0))
            height = float(max(max((float(w.get("y2", 0.0)) for w in words), default=1.0), 1.0))

        layouts.append(
            {
                "text": text,
                "words": words,
                "width": width,
                "height": height,
            }
        )
    return layouts


def _persist_job_analyzer_meta(job_id: str, analyzer_meta: Dict) -> None:
    job_dir = os.path.join(DATA_DIR, "jobs", str(job_id))
    status_path = os.path.join(job_dir, "status.json")
    diagnostics_path = os.path.join(job_dir, "result", "parse_diagnostics.json")
    profile_update_path = os.path.join(job_dir, "result", "profile_update.json")

    status = _read_json_if_exists(status_path, {})
    if isinstance(status, dict):
        status.update(
            {
                "profile_analyzer_triggered": bool(analyzer_meta.get("triggered", False)),
                "profile_analyzer_provider": analyzer_meta.get("provider"),
                "profile_analyzer_model": analyzer_meta.get("model"),
                "profile_analyzer_result": analyzer_meta.get("result"),
                "profile_analyzer_reason": analyzer_meta.get("reason"),
                "profile_selected_after_analyzer": analyzer_meta.get("profile_name"),
            }
        )
        try:
            with open(status_path, "w") as f:
                json.dump(status, f)
        except Exception as exc:
            logger.warning("Failed to persist analyzer status for job %s", str(job_id), exc_info=exc)

    diagnostics = _read_json_if_exists(diagnostics_path, {"job": {}, "pages": {}})
    if not isinstance(diagnostics, dict):
        diagnostics = {"job": {}, "pages": {}}
    job_diag = diagnostics.setdefault("job", {})
    if not isinstance(job_diag, dict):
        job_diag = {}
        diagnostics["job"] = job_diag
    job_diag.update(
        {
            "profile_analyzer_triggered": bool(analyzer_meta.get("triggered", False)),
            "profile_analyzer_provider": analyzer_meta.get("provider"),
            "profile_analyzer_model": analyzer_meta.get("model"),
            "profile_analyzer_result": analyzer_meta.get("result"),
            "profile_analyzer_reason": analyzer_meta.get("reason"),
            "profile_selected_after_analyzer": analyzer_meta.get("profile_name"),
        }
    )
    try:
        os.makedirs(os.path.dirname(diagnostics_path), exist_ok=True)
        with open(diagnostics_path, "w") as f:
            json.dump(diagnostics, f, indent=2)
    except Exception as exc:
        logger.warning("Failed to persist analyzer diagnostics for job %s", str(job_id), exc_info=exc)

    if analyzer_meta.get("triggered"):
        try:
            with open(profile_update_path, "w") as f:
                json.dump(analyzer_meta, f, indent=2)
        except Exception as exc:
            logger.warning("Failed to persist profile update artifact for job %s", str(job_id), exc_info=exc)


def _run_ai_profile_analyzer_for_job(
    job_id: str,
    layout_pages: Optional[List[Dict]] = None,
    allow_ocr_fallback: bool = True,
    force: bool = False,
) -> Dict:
    job_dir = os.path.join(DATA_DIR, "jobs", str(job_id))
    diagnostics_path = os.path.join(job_dir, "result", "parse_diagnostics.json")
    status_payload = _read_json_if_exists(os.path.join(job_dir, "status.json"), {})
    diagnostics_payload = _read_json_if_exists(diagnostics_path, {})
    existing_job_diag = (diagnostics_payload or {}).get("job", {}) if isinstance(diagnostics_payload, dict) else {}
    if not isinstance(existing_job_diag, dict):
        existing_job_diag = {}

    existing_result = str(
        existing_job_diag.get("profile_analyzer_result")
        or (status_payload.get("profile_analyzer_result") if isinstance(status_payload, dict) else "")
        or ""
    ).strip().lower()
    existing_reason = str(
        existing_job_diag.get("profile_analyzer_reason")
        or (status_payload.get("profile_analyzer_reason") if isinstance(status_payload, dict) else "")
        or ""
    ).strip().lower()
    if (not force) and existing_result in {"applied", "rejected", "failed"}:
        return {
            "triggered": bool(existing_job_diag.get("profile_analyzer_triggered", False)),
            "result": existing_job_diag.get("profile_analyzer_result") or existing_result,
            "reason": existing_job_diag.get("profile_analyzer_reason") or existing_reason,
            "provider": existing_job_diag.get("profile_analyzer_provider") or AI_ANALYZER_PROVIDER,
            "model": existing_job_diag.get("profile_analyzer_model") or AI_ANALYZER_MODEL,
            "profile_name": existing_job_diag.get("profile_selected_after_analyzer"),
        }
    if (not force) and existing_result == "skipped" and existing_reason in {"disabled"}:
        return {
            "triggered": bool(existing_job_diag.get("profile_analyzer_triggered", False)),
            "result": existing_job_diag.get("profile_analyzer_result") or "skipped",
            "reason": existing_job_diag.get("profile_analyzer_reason") or existing_reason,
            "provider": existing_job_diag.get("profile_analyzer_provider") or AI_ANALYZER_PROVIDER,
            "model": existing_job_diag.get("profile_analyzer_model") or AI_ANALYZER_MODEL,
            "profile_name": existing_job_diag.get("profile_selected_after_analyzer"),
        }

    analyzer_meta = {
        "triggered": False,
        "result": "skipped",
        "reason": "disabled",
        "provider": AI_ANALYZER_PROVIDER,
        "model": AI_ANALYZER_MODEL,
        "profile_name": None,
    }
    if not AI_ANALYZER_ENABLED:
        _persist_job_analyzer_meta(job_id, analyzer_meta)
        return analyzer_meta

    candidate_layouts = []
    if isinstance(layout_pages, list):
        candidate_layouts = [p for p in layout_pages if isinstance(p, dict)]
    if not candidate_layouts and allow_ocr_fallback:
        candidate_layouts = _build_layout_pages_from_ocr(job_dir, AI_ANALYZER_SAMPLE_PAGES)

    sample_profiles = _sample_detected_profiles(candidate_layouts, AI_ANALYZER_SAMPLE_PAGES)

    if sample_profiles and all(name == "GENERIC" for name in sample_profiles):
        analyzer_meta = analyze_unknown_bank_and_apply(
            layout_pages=candidate_layouts,
            sample_pages=AI_ANALYZER_SAMPLE_PAGES,
            min_rows=AI_ANALYZER_MIN_ROWS,
            min_date_ratio=AI_ANALYZER_MIN_DATE_RATIO,
            min_balance_ratio=AI_ANALYZER_MIN_BAL_RATIO,
        )
    elif sample_profiles:
        analyzer_meta = {
            **analyzer_meta,
            "triggered": True,
            "result": "matched",
            "reason": "matched_existing_profile",
            "profile_name": sample_profiles[0],
        }
    else:
        analyzer_meta = {
            **analyzer_meta,
            "triggered": True,
            "result": "skipped",
            "reason": "no_ocr_profiles_sampled",
        }

    _persist_job_analyzer_meta(job_id, analyzer_meta)
    return analyzer_meta


def _get_submission_job_dir(submission: Submission) -> str | None:
    if not submission.current_job_id:
        return None
    return os.path.join(DATA_DIR, "jobs", str(submission.current_job_id))


def _discover_job_page_keys(job_dir: str) -> List[str]:
    page_keys: set[str] = set()
    status = _read_json_if_exists(os.path.join(job_dir, "status.json"), {})
    total_pages = int((status or {}).get("pages") or 0)
    if total_pages > 0:
        for idx in range(1, total_pages + 1):
            page_keys.add(f"page_{idx:03d}")

    parsed = _read_json_if_exists(os.path.join(job_dir, "result", "parsed_rows.json"), {})
    if isinstance(parsed, dict):
        page_keys.update(str(k) for k in parsed.keys())

    diagnostics = _read_json_if_exists(os.path.join(job_dir, "result", "parse_diagnostics.json"), {})
    diag_pages = (diagnostics or {}).get("pages", {}) if isinstance(diagnostics, dict) else {}
    if isinstance(diag_pages, dict):
        page_keys.update(str(k) for k in diag_pages.keys())

    cleaned_dir = os.path.join(job_dir, "cleaned")
    if os.path.isdir(cleaned_dir):
        for name in os.listdir(cleaned_dir):
            if name.endswith(".png"):
                page_keys.add(name.replace(".png", ""))

    return sorted({k for k in page_keys if str(k).startswith("page_")})


class LoginRequest(BaseModel):
    email: str
    password: str


class AdminCreateUserRequest(BaseModel):
    email: str
    password: str
    role: str


class AdminSetUserActiveRequest(BaseModel):
    is_active: bool


ALLOWED_APP_ROLES = {"agent", "credit_evaluator", "admin"}


@app.post("/auth/login")
def auth_login(payload: LoginRequest):
    db = SessionLocal()
    try:
        user = db.scalar(select(User).where(User.email == payload.email, User.is_active.is_(True)))
        if not user or not verify_password(payload.password, user.password_hash):
            raise HTTPException(status_code=401, detail="invalid_credentials")
        token = issue_token(user)
        return {"access_token": token, "token_type": "bearer", "role": user.role}
    finally:
        db.close()


@app.get("/auth/me")
def auth_me(user: AuthUser = Depends(get_current_user)):
    return {"id": str(user.id), "email": user.email, "role": user.role}


@app.get("/admin/users")
def admin_list_users(user: AuthUser = Depends(require_role("admin"))):
    db = SessionLocal()
    try:
        rows = list(db.scalars(select(User).order_by(User.created_at.desc())))
        items = []
        for row in rows:
            items.append(
                {
                    "id": str(row.id),
                    "email": row.email,
                    "role": row.role,
                    "is_active": bool(row.is_active),
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
            )
        return {"items": items}
    finally:
        db.close()


@app.post("/admin/users")
def admin_create_user(payload: AdminCreateUserRequest, user: AuthUser = Depends(require_role("admin"))):
    email = (payload.email or "").strip().lower()
    password = (payload.password or "").strip()
    role = (payload.role or "").strip()
    if not email or not password:
        raise HTTPException(status_code=400, detail="email_and_password_required")
    if role not in ALLOWED_APP_ROLES:
        raise HTTPException(status_code=400, detail="invalid_role")

    db = SessionLocal()
    try:
        existing = db.scalar(select(User).where(User.email == email))
        if existing:
            raise HTTPException(status_code=409, detail="email_already_exists")
        row = User(
            email=email,
            password_hash=hash_password(password),
            role=role,
            is_active=True,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return {
            "id": str(row.id),
            "email": row.email,
            "role": row.role,
            "is_active": bool(row.is_active),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
    finally:
        db.close()


@app.delete("/admin/users/{user_id}")
def admin_delete_user(user_id: uuid.UUID, user: AuthUser = Depends(require_role("admin"))):
    target_id = user_id
    if target_id == user.id:
        raise HTTPException(status_code=400, detail="cannot_delete_self")

    db = SessionLocal()
    try:
        row = db.scalar(select(User).where(User.id == target_id))
        if not row:
            raise HTTPException(status_code=404, detail="user_not_found")

        if row.role == "admin":
            active_admins = list(db.scalars(select(User).where(User.role == "admin", User.is_active.is_(True))))
            if len(active_admins) <= 1:
                raise HTTPException(status_code=400, detail="cannot_delete_last_admin")

        # Conservative delete: deactivate account instead of hard-delete to preserve FK integrity.
        row.is_active = False
        db.commit()
        return {"ok": True, "id": str(row.id), "is_active": bool(row.is_active)}
    finally:
        db.close()


@app.patch("/admin/users/{user_id}/active")
def admin_set_user_active(
    user_id: uuid.UUID,
    payload: AdminSetUserActiveRequest,
    user: AuthUser = Depends(require_role("admin")),
):
    target_id = user_id
    db = SessionLocal()
    try:
        row = db.scalar(select(User).where(User.id == target_id))
        if not row:
            raise HTTPException(status_code=404, detail="user_not_found")

        # Prevent locking yourself out.
        if row.id == user.id and payload.is_active is False:
            raise HTTPException(status_code=400, detail="cannot_deactivate_self")

        # Keep at least one active admin.
        if row.role == "admin" and payload.is_active is False:
            active_admins = list(db.scalars(select(User).where(User.role == "admin", User.is_active.is_(True))))
            if len(active_admins) <= 1 and row.is_active:
                raise HTTPException(status_code=400, detail="cannot_deactivate_last_admin")

        row.is_active = bool(payload.is_active)
        db.commit()
        return {"ok": True, "id": str(row.id), "is_active": bool(row.is_active)}
    finally:
        db.close()


@app.post("/admin/clear-submissions")
def admin_clear_submissions(user: AuthUser = Depends(require_role("admin"))):
    db = SessionLocal()
    try:
        counts = {
            "submissions": int(db.scalar(select(func.count()).select_from(Submission)) or 0),
            "jobs": int(db.scalar(select(func.count()).select_from(JobRecord)) or 0),
            "submission_pages": int(db.scalar(select(func.count()).select_from(SubmissionPage)) or 0),
            "transactions": int(db.scalar(select(func.count()).select_from(Transaction)) or 0),
            "reports": int(db.scalar(select(func.count()).select_from(Report)) or 0),
            "audit_log": int(db.scalar(select(func.count()).select_from(AuditLog)) or 0),
        }
        db.execute(delete(AuditLog))
        db.execute(delete(Report))
        db.execute(delete(Transaction))
        db.execute(delete(SubmissionPage))
        db.execute(delete(JobRecord))
        db.execute(delete(Submission))
        db.commit()
    finally:
        db.close()

    jobs_dir = os.path.join(DATA_DIR, "jobs")
    reports_dir = os.path.join(DATA_DIR, "reports")
    for root in (jobs_dir, reports_dir):
        if not os.path.exists(root):
            os.makedirs(root, exist_ok=True)
            continue
        for name in os.listdir(root):
            path = os.path.join(root, name)
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
            except Exception as exc:
                logger.warning("Failed to clear path during admin reset: %s", path, exc_info=exc)
        os.makedirs(root, exist_ok=True)

    return {"ok": True, "cleared": counts}


# ---------------------------
# Job creation
# ---------------------------
@app.post("/jobs")
async def create_job(
    file: UploadFile = File(...),
    mode: str = Form("text"),
    user: AuthUser = Depends(require_role("credit_evaluator", "admin")),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")
    parse_mode = _normalize_parse_mode(mode)

    job_uuid = uuid.uuid4()
    job_id = str(job_uuid)
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_dir = os.path.join(job_dir, "input")

    os.makedirs(input_dir, exist_ok=True)

    pdf_path = os.path.join(input_dir, "document.pdf")
    with open(pdf_path, "wb") as f:
        f.write(await file.read())

    # Initial status (atomic write happens in worker later)
    with open(os.path.join(job_dir, "status.json"), "w") as f:
        json.dump({"status": "queued", "step": "queued", "progress": 0, "parse_mode": parse_mode}, f)

    logger.info("Created standalone job %s by user %s (%s)", job_id, user.email, user.role)

    process_pdf.delay(job_id, parse_mode)
    db = SessionLocal()
    try:
        existing = db.scalar(select(JobRecord).where(JobRecord.id == job_uuid))
        if not existing:
            db.add(
                JobRecord(
                    id=job_uuid,
                    submission_id=None,
                    status="queued",
                    step="queued",
                    progress=0,
                    parse_mode=parse_mode,
                )
            )
            db.commit()
    finally:
        db.close()

    return {"job_id": job_id, "parse_mode": parse_mode}


@app.post("/jobs/draft")
async def create_draft_job(
    file: UploadFile = File(...),
    mode: str = Form("text"),
    user: AuthUser = Depends(require_role("credit_evaluator", "admin")),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")
    parse_mode = _normalize_parse_mode(mode)

    job_uuid = uuid.uuid4()
    job_id = str(job_uuid)
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
    db = SessionLocal()
    try:
        existing = db.scalar(select(JobRecord).where(JobRecord.id == job_uuid))
        if not existing:
            db.add(
                JobRecord(
                    id=job_uuid,
                    submission_id=None,
                    status="queued",
                    step="draft_queued",
                    progress=1,
                    parse_mode=parse_mode,
                )
            )
            db.commit()
    finally:
        db.close()
    logger.info("Created standalone draft job %s by user %s (%s)", job_id, user.email, user.role)
    return {"job_id": job_id}


@app.post("/jobs/{job_id}/start")
def start_job_from_draft(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=True)
    job_id_str = str(job_id)
    job_dir = os.path.join(DATA_DIR, "jobs", job_id_str)
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

    current_status = ""
    if os.path.exists(status_path):
        try:
            with open(status_path) as f:
                current_status = str(json.load(f).get("status") or "").strip().lower()
        except Exception:
            current_status = ""

    if current_status in {"queued", "processing", "done"}:
        return {"job_id": job_id_str, "started": False, "parse_mode": parse_mode}

    with open(status_path, "w") as f:
        json.dump({"status": "queued", "step": "queued", "progress": 0, "parse_mode": parse_mode}, f)

    db = SessionLocal()
    try:
        db_job = db.scalar(select(JobRecord).where(JobRecord.id == job_id))
        if db_job:
            db_job.status = "processing"
            db_job.step = "queued"
            db_job.progress = 0
            if db_job.submission_id:
                db_sub = db.scalar(select(Submission).where(Submission.id == db_job.submission_id))
                if db_sub and db_sub.status != "summary_generated":
                    db_sub.status = "processing"
            db.commit()
    finally:
        db.close()

    process_pdf.delay(job_id_str, parse_mode)
    return {"job_id": job_id_str, "started": True, "parse_mode": parse_mode}


class EvaluatorTransactionRow(BaseModel):
    row_id: Optional[str] = None
    page: Optional[str] = None
    date: Optional[str] = None
    description: Optional[str] = None
    debit: Optional[str] = None
    credit: Optional[str] = None
    balance: Optional[str] = None


class EvaluatorTransactionsPatch(BaseModel):
    rows: List[EvaluatorTransactionRow]


class EvaluatorGuideColumn(BaseModel):
    key: str
    width: float


class EvaluatorGuideState(BaseModel):
    column_layout: List[EvaluatorGuideColumn] = Field(default_factory=list)
    horizontal: List[float] = Field(default_factory=list)


class EvaluatorPageTransactionsPatch(BaseModel):
    rows: List[EvaluatorTransactionRow]
    expected_updated_at: Optional[str] = None
    guide_state: Optional[EvaluatorGuideState] = None


class EvaluatorCombineSubmissionsPayload(BaseModel):
    submission_ids: List[uuid.UUID] = Field(default_factory=list)


@app.post("/agent/submissions")
async def agent_create_submission(
    file: UploadFile = File(...),
    mode: str = Form("text"),
    borrower_name: Optional[str] = Form(None),
    lead_reference: Optional[str] = Form(None),
    notes: Optional[str] = Form(None),
    user: AuthUser = Depends(require_role("agent")),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")
    if not normalize_borrower_name(borrower_name):
        raise HTTPException(status_code=422, detail="borrower_required")
    parse_mode = _normalize_parse_mode(mode)
    job_id = uuid.uuid4()
    blob_key = f"jobs/{job_id}/input/document.pdf"
    write_blob(blob_key, await file.read())

    sub, job = create_submission_with_job(
        agent_user=user,
        input_pdf_key=blob_key,
        job_id=job_id,
        parse_mode=parse_mode,
        borrower_name=borrower_name,
        lead_reference=lead_reference,
        notes=notes,
    )

    job_dir = os.path.join(DATA_DIR, "jobs", str(job_id))
    os.makedirs(os.path.join(job_dir, "input"), exist_ok=True)
    try:
        with open(os.path.join(job_dir, "meta.json"), "w") as f:
            json.dump({"original_filename": file.filename}, f)
    except Exception as exc:
        logger.warning("Failed to write meta.json for submission job %s", job_id, exc_info=exc)
    status_path = os.path.join(job_dir, "status.json")
    with open(status_path, "w") as f:
        json.dump({"status": "for_review", "step": "for_review", "progress": 0, "parse_mode": parse_mode}, f)

    return {"submission_id": str(sub.id), "job_id": str(job.id), "status": sub.status}


@app.get("/agent/submissions")
def agent_list_submissions(user: AuthUser = Depends(require_role("agent"))):
    rows = list_submissions_for_agent(user)
    items = []
    for row in rows:
        payload = serialize_submission(row)
        payload["original_filename"] = _read_submission_original_filename(payload)
        items.append(payload)
    return {"items": items}


@app.get("/agent/submissions/{submission_id}")
def agent_get_submission(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("agent"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    return serialize_submission(sub)


@app.get("/agent/submissions/{submission_id}/status")
def agent_submission_status(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("agent"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")

    status_payload = {"status": sub.status, "submission_id": str(sub.id), "job_id": str(sub.current_job_id) if sub.current_job_id else None}
    if sub.current_job_id:
        status_path = os.path.join(DATA_DIR, "jobs", str(sub.current_job_id), "status.json")
        if os.path.exists(status_path):
            try:
                with open(status_path) as f:
                    status_payload["job"] = json.load(f)
            except Exception:
                status_payload["job"] = {"status": "processing"}
    return status_payload


@app.get("/evaluator/submissions")
def evaluator_list_submissions(
    include_unassigned: bool = False,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    rows = list_submissions_for_evaluator(user, include_unassigned=include_unassigned)
    return {"items": [serialize_submission(s) for s in rows]}


@app.post("/evaluator/submissions/{submission_id}/assign")
def evaluator_assign_submission(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = assign_submission_to_evaluator(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return serialize_submission(sub)


@app.post("/evaluator/submissions/combine")
def evaluator_combine_submissions(
    payload: EvaluatorCombineSubmissionsPayload,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        sub, job = combine_submissions_for_evaluator(user, payload.submission_ids)
    except ValueError as exc:
        detail = str(exc)
        code = 400
        if detail == "submission_not_found":
            code = 404
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    job_dir = os.path.join(DATA_DIR, "jobs", str(job.id))
    os.makedirs(os.path.join(job_dir, "input"), exist_ok=True)
    status_path = os.path.join(job_dir, "status.json")
    with open(status_path, "w") as f:
        json.dump(
            {
                "status": "for_review",
                "step": "for_review",
                "progress": 0,
                "parse_mode": str(job.parse_mode or "text"),
            },
            f,
        )
    try:
        with open(os.path.join(job_dir, "meta.json"), "w") as f:
            json.dump(
                {
                    "original_filename": "combined.pdf",
                    "source_submission_ids": [str(sid) for sid in payload.submission_ids],
                },
                f,
            )
    except Exception as exc:
        logger.warning("Failed to write combine meta.json for job %s", str(job.id), exc_info=exc)

    return {"submission_id": str(sub.id), "job_id": str(job.id), "status": sub.status}


@app.get("/evaluator/submissions/{submission_id}")
def evaluator_get_submission(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")

    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")

    tx_rows = [serialize_transaction(t) for t in get_transactions_for_submission(sub.id)]
    job_payload = None
    diagnostics = None
    parsed = None
    bounds = None
    if sub.current_job_id:
        job_dir = os.path.join(DATA_DIR, "jobs", str(sub.current_job_id))
        status_path = os.path.join(job_dir, "status.json")
        diagnostics_path = os.path.join(job_dir, "result", "parse_diagnostics.json")
        parsed_path = os.path.join(job_dir, "result", "parsed_rows.json")
        bounds_path = os.path.join(job_dir, "result", "bounds.json")
        job_payload = _read_json_if_exists(status_path, None)
        diagnostics = _read_json_if_exists(diagnostics_path, None)
        parsed = _read_json_if_exists(parsed_path, None)
        bounds = _read_json_if_exists(bounds_path, None)

    return {
        "submission": serialize_submission(sub),
        "job": job_payload,
        "transactions": tx_rows,
        "summary": sub.summary_snapshot_json,
        "diagnostics": diagnostics,
        "parsed": parsed,
        "bounds": bounds,
        "review_status": get_submission_review_status(sub.id, user),
    }


@app.get("/evaluator/submissions/{submission_id}/pages")
def evaluator_list_submission_pages(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")

    job_dir = _get_submission_job_dir(sub)
    if job_dir and os.path.exists(job_dir):
        page_keys = _discover_job_page_keys(job_dir)
        if page_keys:
            max_idx = 0
            for page_key in page_keys:
                try:
                    max_idx = max(max_idx, int(page_key.replace("page_", "")))
                except Exception:
                    continue
            if max_idx > 0:
                ensure_submission_pages(sub.id, max_idx, sub.current_job_id)
        parsed_map = _read_json_if_exists(os.path.join(job_dir, "result", "parsed_rows.json"), {})
        diagnostics = _read_json_if_exists(os.path.join(job_dir, "result", "parse_diagnostics.json"), {})
        diag_pages = diagnostics.get("pages", {}) if isinstance(diagnostics, dict) else {}
        status_payload = _read_json_if_exists(os.path.join(job_dir, "status.json"), {})
        status_name = str((status_payload or {}).get("status") or "").lower()
        for key in page_keys:
            rows = (parsed_map or {}).get(key) or []
            diag = (diag_pages or {}).get(key) or {}
            if rows:
                set_page_parse_status(sub.id, key, "done")
            elif diag:
                if str(diag.get("source_type") or "") == "none":
                    set_page_parse_status(sub.id, key, "failed", str(diag.get("fallback_reason") or "parse_failed"))
                else:
                    set_page_parse_status(sub.id, key, "done")
            elif status_name in {"queued", "processing"}:
                set_page_parse_status(sub.id, key, "processing")
            else:
                set_page_parse_status(sub.id, key, "pending")

    try:
        return list_submission_pages(sub.id, user)
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")


@app.get("/evaluator/submissions/{submission_id}/pages/{page_key}")
def evaluator_get_submission_page(
    submission_id: uuid.UUID,
    page_key: str,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")

    key = page_key if str(page_key).startswith("page_") else f"page_{str(page_key).zfill(3)}"
    job_dir = _get_submission_job_dir(sub)
    parsed_page_rows = []
    bounds_page = []
    diag_page = {}
    identity_bounds = []
    if job_dir:
        parsed_map = _read_json_if_exists(os.path.join(job_dir, "result", "parsed_rows.json"), {})
        bounds_map = _read_json_if_exists(os.path.join(job_dir, "result", "bounds.json"), {})
        diagnostics = _read_json_if_exists(os.path.join(job_dir, "result", "parse_diagnostics.json"), {})
        parsed_page_rows = (parsed_map or {}).get(key) or []
        bounds_page = (bounds_map or {}).get(key) or []
        diag_page = ((diagnostics or {}).get("pages") or {}).get(key) or {}
        job_diag = (diagnostics or {}).get("job") or {}
        for b in (job_diag.get("account_name_bbox"), job_diag.get("account_number_bbox")):
            if isinstance(b, dict) and b.get("page") == key:
                identity_bounds.append(b)

    try:
        page_data = get_submission_page(sub.id, key, user)
    except ValueError as exc:
        if str(exc) != "page_not_found":
            raise HTTPException(status_code=404, detail=str(exc))
        page_data = {"page_status": None, "rows": []}
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")

    page_rows = []
    db_rows = page_data.get("rows") or []
    if db_rows:
        for row in db_rows:
            row_index = int(row.get("row_index") or 0)
            page_rows.append(
                {
                    "row_id": str(row.get("row_id") or f"{row_index:03d}"),
                    "page": key,
                    "date": row.get("date") or "",
                    "description": row.get("description") or "",
                    "debit": "" if row.get("debit") is None else str(row.get("debit")),
                    "credit": "" if row.get("credit") is None else str(row.get("credit")),
                    "balance": "" if row.get("balance") is None else str(row.get("balance")),
                    "x1": row.get("x1"),
                    "y1": row.get("y1"),
                    "x2": row.get("x2"),
                    "y2": row.get("y2"),
                }
            )
    else:
        for row in parsed_page_rows:
            row_id = str(row.get("row_id") or "")
            row_bounds = next((b for b in bounds_page if str(b.get("row_id") or "") == row_id), {})
            page_rows.append(
                {
                    "row_id": row_id or "",
                    "page": key,
                    "date": row.get("date") or "",
                    "description": row.get("description") or "",
                    "debit": row.get("debit") or "",
                    "credit": row.get("credit") or "",
                    "balance": row.get("balance") or "",
                    "x1": row_bounds.get("x1"),
                    "y1": row_bounds.get("y1"),
                    "x2": row_bounds.get("x2"),
                    "y2": row_bounds.get("y2"),
                }
            )

    page_status = page_data.get("page_status") or {
        "page_key": key,
        "index": int(str(key).replace("page_", "")) if str(key).replace("page_", "").isdigit() else 0,
        "parse_status": "pending",
        "review_status": "pending",
        "saved_at": None,
        "rows_count": len(page_rows),
        "has_unsaved": False,
        "updated_at": None,
    }

    return {
        "rows": page_rows,
        "bounds": bounds_page,
        "identity_bounds": identity_bounds,
        "parse_diagnostics_page": diag_page,
        "page_status": page_status,
        "guide_state": _read_page_editor_state(sub, key),
    }


@app.patch("/evaluator/submissions/{submission_id}/pages/{page_key}/transactions")
def evaluator_patch_page_transactions(
    submission_id: uuid.UUID,
    page_key: str,
    payload: EvaluatorPageTransactionsPatch,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")

    key = page_key if str(page_key).startswith("page_") else f"page_{str(page_key).zfill(3)}"
    if payload.expected_updated_at:
        try:
            page_data = get_submission_page(submission_id, key, user)
            updated_at = str((page_data.get("page_status") or {}).get("updated_at") or "")
            if updated_at and updated_at != payload.expected_updated_at:
                raise HTTPException(status_code=409, detail="page_conflict_reload")
        except ValueError as exc:
            if str(exc) != "page_not_found":
                raise HTTPException(status_code=404, detail=str(exc))
    try:
        result = persist_page_transactions(
            submission_id=submission_id,
            page_key=key,
            evaluator_user=user,
            rows=[r.model_dump() for r in payload.rows],
        )
    except ValueError as exc:
        detail = str(exc)
        code = 404 if detail == "submission_not_found" else 400
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if payload.guide_state is not None:
        try:
            _write_page_editor_state(sub, key, payload.guide_state.model_dump())
        except Exception as exc:
            logger.warning(
                "Failed to persist page editor state for submission=%s page=%s",
                str(submission_id),
                key,
                exc_info=exc,
            )
            raise HTTPException(status_code=500, detail="failed_to_persist_guide_state")
    return {"ok": True, **result}


@app.post("/evaluator/submissions/{submission_id}/pages/{page_key}/parse")
def evaluator_parse_single_page(
    submission_id: uuid.UUID,
    page_key: str,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")
    if not sub.current_job_id:
        raise HTTPException(status_code=400, detail="submission_has_no_job")
    key = page_key if str(page_key).startswith("page_") else f"page_{str(page_key).zfill(3)}"
    set_page_parse_status(sub.id, key, "processing")
    try:
        _reparse_single_page(str(sub.current_job_id), key)
    except HTTPException:
        set_page_parse_status(sub.id, key, "failed", "reparse_failed")
        raise
    except Exception as exc:
        set_page_parse_status(sub.id, key, "failed", str(exc))
        raise HTTPException(status_code=500, detail="reparse_failed")
    set_page_parse_status(sub.id, key, "done")
    return {"ok": True, "page_key": key}


@app.post("/evaluator/submissions/{submission_id}/pages/{page_key}/review-complete")
def evaluator_review_complete(
    submission_id: uuid.UUID,
    page_key: str,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        result = mark_page_reviewed(submission_id, page_key, user)
    except ValueError as exc:
        detail = str(exc)
        code = 404 if detail in {"submission_not_found", "page_not_found"} else 400
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return {"ok": True, **result}


@app.get("/evaluator/submissions/{submission_id}/review-status")
def evaluator_review_status(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        return get_submission_review_status(submission_id, user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@app.post("/evaluator/submissions/{submission_id}/finish-review")
def evaluator_finish_review(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        return finish_review_and_build_summary(submission_id, user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@app.patch("/evaluator/submissions/{submission_id}/transactions")
def evaluator_patch_transactions(
    submission_id: uuid.UUID,
    payload: EvaluatorTransactionsPatch,
    user: AuthUser = Depends(require_role("credit_evaluator")),
):
    try:
        summary = persist_evaluator_transactions(
            submission_id=submission_id,
            evaluator_user=user,
            rows=[r.model_dump() for r in payload.rows],
        )
    except ValueError as exc:
        detail = str(exc)
        code = 404 if detail == "submission_not_found" else 400
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return {"ok": True, "summary": summary}


@app.post("/evaluator/submissions/{submission_id}/analyze")
def evaluator_analyze_submission(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")
    rows = [serialize_transaction(t) for t in get_transactions_for_submission(sub.id)]
    summary = compute_summary(rows)
    db = SessionLocal()
    try:
        db_sub = db.scalar(select(Submission).where(Submission.id == sub.id))
        if db_sub:
            db_sub.summary_snapshot_json = summary
            if db_sub.status != "summary_generated":
                db_sub.status = "for_review"
            db.commit()
    finally:
        db.close()
    return {"ok": True, "summary": summary}


@app.post("/evaluator/submissions/{submission_id}/reports")
def evaluator_generate_report(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")
    try:
        if not can_generate_exports(sub.id, user):
            raise HTTPException(status_code=409, detail="review_incomplete")
    except PermissionError:
        raise HTTPException(status_code=403, detail="submission_not_assigned")

    rows = [serialize_transaction(t) for t in get_transactions_for_submission(sub.id)]
    summary = sub.summary_snapshot_json or compute_summary(rows)
    report_id = uuid.uuid4()
    blob_key = f"reports/{sub.id}/{report_id}.pdf"
    pdf_bytes = _build_minimal_report_pdf(str(sub.id), summary, rows)
    write_blob(blob_key, pdf_bytes)
    report = create_report_record(sub.id, user, blob_key, report_type="executive_summary")
    return {
        "report_id": str(report.id),
        "submission_id": str(sub.id),
        "blob_key": blob_key,
        "download_url": f"/evaluator/reports/{report.id}/download",
    }


@app.post("/evaluator/submissions/{submission_id}/export-excel")
def evaluator_export_excel_gate(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = get_submission_for_user(submission_id, user)
    except ValueError:
        raise HTTPException(status_code=404, detail="submission_not_found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="forbidden_submission")
    if sub.assigned_evaluator_id != user.id:
        raise HTTPException(status_code=403, detail="submission_not_assigned")
    if not can_generate_exports(sub.id, user):
        raise HTTPException(status_code=409, detail="review_incomplete")
    return {"ok": True, "submission_id": str(sub.id)}


@app.get("/evaluator/reports/{report_id}/download")
def evaluator_download_report(report_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    db = SessionLocal()
    try:
        report = db.scalar(select(Report).where(Report.id == report_id))
        if not report:
            raise HTTPException(status_code=404, detail="report_not_found")
        sub = db.scalar(select(Submission).where(Submission.id == report.submission_id))
        if not sub or sub.assigned_evaluator_id != user.id:
            raise HTTPException(status_code=403, detail="forbidden_report")
        abs_path = blob_abs_path(report.blob_key)
        if not os.path.exists(abs_path):
            raise HTTPException(status_code=404, detail="report_blob_not_found")
        return FileResponse(abs_path, media_type="application/pdf", filename=f"summary_{report.id}.pdf")
    finally:
        db.close()


@app.post("/evaluator/submissions/{submission_id}/mark-summary-ready")
def evaluator_mark_summary_ready(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = set_submission_summary_ready(submission_id, user)
    except ValueError as exc:
        detail = str(exc)
        code = 404 if detail == "submission_not_found" else (409 if detail == "review_incomplete" else 400)
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return serialize_submission(sub)


@app.post("/evaluator/submissions/{submission_id}/mark-summary-generated")
def evaluator_mark_summary_generated(submission_id: uuid.UUID, user: AuthUser = Depends(require_role("credit_evaluator"))):
    try:
        sub = set_submission_summary_generated(submission_id, user)
    except ValueError as exc:
        detail = str(exc)
        code = 404 if detail == "submission_not_found" else (409 if detail == "review_incomplete" else 400)
        raise HTTPException(status_code=code, detail=detail)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return serialize_submission(sub)


# ---------------------------
# Job status
# ---------------------------
@app.get("/jobs/{job_id}")
def job_status(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    status_path = os.path.join(DATA_DIR, "jobs", job_id_str, "status.json")
    job_status_value = str(getattr(job, "status", None) or "queued")
    job_step_value = str(getattr(job, "step", None) or job_status_value or "queued")

    db_payload = {
        "status": job_status_value,
        "step": job_step_value,
        "progress": max(0, min(100, _coerce_progress(getattr(job, "progress", 0), 0))),
    }
    job_parse_mode = getattr(job, "parse_mode", None)
    if job_parse_mode:
        db_payload["parse_mode"] = job_parse_mode
    job_ocr_backend = getattr(job, "ocr_backend", None)
    if job_ocr_backend:
        db_payload["ocr_backend"] = job_ocr_backend

    if not os.path.exists(status_path):
        return db_payload

    payload = None
    try:
        with open(status_path) as f:
            payload = json.load(f)
    except json.JSONDecodeError:
        payload = {}
    except Exception as exc:
        logger.warning("Failed to read status file for job %s", job_id_str, exc_info=exc)
        payload = {}

    if not isinstance(payload, dict):
        payload = {}

    file_status = str(payload.get("status") or "").strip().lower()
    db_status = str(db_payload.get("status") or "").strip().lower()
    file_progress = _coerce_progress(payload.get("progress"), 0)
    db_progress = _coerce_progress(db_payload.get("progress"), 0)

    # Prefer DB truth for terminal states to avoid stale in-flight status files.
    if db_status in {"done", "failed"} and file_status not in {"done", "failed"}:
        payload["status"] = db_payload["status"]
        payload["step"] = db_payload["step"]
        payload["progress"] = max(file_progress, db_progress)
    else:
        payload["progress"] = max(file_progress, db_progress if db_status == "done" else 0)
        if not payload.get("status"):
            payload["status"] = db_payload["status"]
        if not payload.get("step"):
            payload["step"] = db_payload["step"]

    if "parse_mode" not in payload and db_payload.get("parse_mode"):
        payload["parse_mode"] = db_payload["parse_mode"]
    if "ocr_backend" not in payload and db_payload.get("ocr_backend"):
        payload["ocr_backend"] = db_payload["ocr_backend"]

    # Reconcile stale "processing" states for text mode.
    parse_mode = _normalize_parse_mode(payload.get("parse_mode"))
    if file_status in {"processing", "queued"} and parse_mode == "text":
        expected_pages = _coerce_progress(payload.get("pages"), 0)
        parsed_rows_path = os.path.join(DATA_DIR, "jobs", job_id_str, "result", "parsed_rows.json")
        parsed_pages = 0
        if os.path.exists(parsed_rows_path):
            try:
                parsed_map = _read_json_if_exists(parsed_rows_path, {})
                if isinstance(parsed_map, dict):
                    parsed_pages = len(parsed_map.keys())
            except Exception:
                parsed_pages = 0
        if expected_pages > 0 and parsed_pages >= expected_pages:
            payload["status"] = "done"
            payload["step"] = "completed"
            payload["progress"] = 100
        else:
            try:
                status_age = max(0, int(time.time() - os.path.getmtime(status_path)))
            except Exception:
                status_age = 0
            if status_age >= max(30, TEXT_STALE_SECONDS):
                payload["status"] = "failed"
                payload["step"] = "failed"
                payload["message"] = "processing_stale_timeout"
                payload["stale_seconds"] = status_age

    payload["progress"] = max(0, min(100, _coerce_progress(payload.get("progress"), 0)))
    return payload


# ---------------------------
# RAW pages
# ------------

@app.get("/jobs/{job_id}/cleaned")
def list_cleaned(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    cleaned_dir = os.path.join(DATA_DIR, "jobs", job_id_str, "cleaned")
    
    # 🔥 IMPORTANT: do NOT 404
    if not os.path.exists(cleaned_dir):
        return {"pages": []}

    files = sorted(
        f for f in os.listdir(cleaned_dir)
        if f.endswith(".png")
    )
    if files:
        return {"pages": files}

    parsed_rows_path = os.path.join(DATA_DIR, "jobs", job_id_str, "result", "parsed_rows.json")
    if os.path.exists(parsed_rows_path):
        try:
            with open(parsed_rows_path) as f:
                parsed = json.load(f)
            page_keys = sorted(parsed.keys())
            synthetic = [f"{key}.png" for key in page_keys]
            return {"pages": synthetic}
        except Exception as exc:
            logger.warning("Failed to build synthetic cleaned pages for job %s", job_id_str, exc_info=exc)

    return {"pages": files}

@app.get("/jobs/{job_id}/cleaned/{filename}")
def get_cleaned(job_id: uuid.UUID, filename: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    path = os.path.join(DATA_DIR, "jobs", job_id_str, "cleaned", filename)

    if not os.path.exists(path):
        generated = _generate_preview_page_if_missing(job_id_str, filename, path)
        if not generated:
            raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(path, media_type="image/png")


@app.get("/jobs/{job_id}/preview/{page}")
def get_page_preview(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    page_name = page if page.startswith("page_") else f"page_{page}"
    filename = f"{page_name}.png" if not page_name.endswith(".png") else page_name
    job_dir = os.path.join(DATA_DIR, "jobs", job_id_str)
    cleaned_path = os.path.join(job_dir, "cleaned", filename)
    preview_dir = os.path.join(job_dir, "preview")
    preview_path = os.path.join(preview_dir, filename)

    if os.path.exists(cleaned_path):
        return FileResponse(cleaned_path, media_type="image/png")

    if not os.path.exists(preview_path):
        ok = _generate_preview_page_if_missing(job_id_str, filename, preview_path)
        if not ok:
            raise HTTPException(status_code=404, detail="Preview page not found")

    return FileResponse(preview_path, media_type="image/png")

@app.get("/jobs/{job_id}/ocr/{page}")
def get_ocr(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    path = os.path.join(DATA_DIR, "jobs", str(job_id), "ocr", f"{page}.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="OCR not ready")

    with open(path) as f:
        return json.load(f)
    

@app.get("/jobs/{job_id}/rows/{page}/bounds")
def get_row_bounds(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    path = os.path.join(DATA_DIR, "jobs", str(job_id), "result", "bounds.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Row bounds not ready")

    with open(path) as f:
        data = json.load(f)

    return data.get(page, [])


@app.get("/jobs/{job_id}/bounds")
def get_all_row_bounds(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    path = os.path.join(DATA_DIR, "jobs", str(job_id), "result", "bounds.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Row bounds not ready")

    with open(path) as f:
        return json.load(f)

@app.get("/jobs/{job_id}/rows/{page}/{row}")
def get_row_image(job_id: uuid.UUID, page: str, row: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/parsed/{page}."
    )




@app.get("/jobs/{job_id}/ocr/rows")
def get_row_ocr(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/ocr/{page}."
    )

@app.get("/jobs/{job_id}/rows/{page}")
def list_rows(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    raise HTTPException(
        status_code=410,
        detail="Row-based processing removed. Use /jobs/{job_id}/parsed/{page}."
    )

@app.get("/jobs/{job_id}/parsed/{page}")
def get_parsed_rows(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    path = os.path.join(DATA_DIR, "jobs", job_id_str, "result", "parsed_rows.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Parsed rows not ready")

    with open(path) as f:
        data = json.load(f)

    page_rows = data.get(page, [])
    if _rows_need_description_backfill(page_rows):
        page_rows = _backfill_page_descriptions(job_id_str, page, page_rows)
        data[page] = page_rows
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    return page_rows


@app.get("/jobs/{job_id}/parsed")
def get_all_parsed_rows(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    path = os.path.join(DATA_DIR, "jobs", str(job_id), "result", "parsed_rows.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Parsed rows not ready")

    with open(path) as f:
        return json.load(f)


@app.get("/jobs/{job_id}/diagnostics")
def get_parse_diagnostics(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    path = os.path.join(DATA_DIR, "jobs", job_id_str, "result", "parse_diagnostics.json")

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Diagnostics not ready")

    with open(path) as f:
        diagnostics = json.load(f)

    diagnostics = _backfill_job_account_identity(job_id_str, diagnostics, path)
    return diagnostics


@app.get("/jobs/{job_id}/account-identity")
def get_account_identity(job_id: uuid.UUID, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)
    job_id_str = str(job_id)
    path = os.path.join(DATA_DIR, "jobs", job_id_str, "result", "parse_diagnostics.json")

    diagnostics: Dict = {"job": {}, "pages": {}}
    if os.path.exists(path):
        try:
            with open(path) as f:
                diagnostics = json.load(f)
        except Exception:
            diagnostics = {"job": {}, "pages": {}}

    diagnostics = _backfill_job_account_identity(job_id_str, diagnostics, path if os.path.exists(path) else None)
    job = diagnostics.get("job", {}) if isinstance(diagnostics, dict) else {}
    return {
        "account_name": job.get("account_name"),
        "account_number": job.get("account_number"),
        "account_name_bbox": job.get("account_name_bbox"),
        "account_number_bbox": job.get("account_number_bbox"),
        "account_identity_source": job.get("account_identity_source"),
        "account_identity_ai_attempted": job.get("account_identity_ai_attempted"),
        "account_identity_ai_result": job.get("account_identity_ai_result"),
        "account_identity_ai_reason": job.get("account_identity_ai_reason"),
    }


class FlattenPoint(BaseModel):
    x: float = Field(ge=0.0, le=1.0)
    y: float = Field(ge=0.0, le=1.0)


class FlattenRequest(BaseModel):
    points: List[FlattenPoint]


class SectionBounds(BaseModel):
    x1: float = Field(ge=0.0, le=1.0)
    y1: float = Field(ge=0.0, le=1.0)
    x2: float = Field(ge=0.0, le=1.0)
    y2: float = Field(ge=0.0, le=1.0)


class SectionOcrRequest(BaseModel):
    sections: List[SectionBounds]
    guide_state: Optional[EvaluatorGuideState] = None

class ImageToolRequest(BaseModel):
    tool: str = Field(min_length=1, max_length=32)


IMAGE_TOOLS = {
    "deskew",
    "contrast",
    "binarize",
    "denoise",
    "sharpen",
    "remove_lines",
    "reset",
}


def _normalize_page_name(page: str) -> str:
    value = str(page or "").strip()
    return value if value.startswith("page_") else f"page_{value.zfill(3)}"


def _deskew_grayscale(gray: np.ndarray) -> np.ndarray:
    try:
        inv = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
        coords = np.column_stack(np.where(inv > 0))
        if coords.size == 0:
            return gray
        angle = float(cv2.minAreaRect(coords.astype(np.float32))[-1])
        if angle < -45.0:
            angle = -(90.0 + angle)
        else:
            angle = -angle
        if abs(angle) < 0.05:
            return gray
        h, w = gray.shape[:2]
        matrix = cv2.getRotationMatrix2D((w / 2.0, h / 2.0), angle, 1.0)
        return cv2.warpAffine(
            gray,
            matrix,
            (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )
    except Exception:
        return gray


def _remove_grid_lines(gray: np.ndarray) -> np.ndarray:
    h, w = gray.shape[:2]
    if h <= 0 or w <= 0:
        return gray

    inv = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(20, w // 28), 1))
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(20, h // 28)))
    h_lines = cv2.morphologyEx(inv, cv2.MORPH_OPEN, h_kernel)
    v_lines = cv2.morphologyEx(inv, cv2.MORPH_OPEN, v_kernel)
    lines = cv2.bitwise_or(h_lines, v_lines)
    if cv2.countNonZero(lines) == 0:
        return gray
    return cv2.inpaint(gray, lines, 3, cv2.INPAINT_TELEA)


def _apply_image_tool(img_bgr: np.ndarray, tool: str) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    if tool == "deskew":
        return _deskew_grayscale(gray)

    if tool == "contrast":
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        return clahe.apply(gray)

    if tool == "binarize":
        return cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            11,
        )

    if tool == "denoise":
        return cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)

    if tool == "sharpen":
        kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]], dtype=np.float32)
        return cv2.filter2D(gray, -1, kernel)

    if tool == "remove_lines":
        return _remove_grid_lines(gray)

    raise HTTPException(status_code=400, detail="unsupported_image_tool")


@app.post("/jobs/{job_id}/pages/{page}/flatten")
def flatten_page(job_id: uuid.UUID, page: str, payload: FlattenRequest, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=True)
    job_id_str = str(job_id)
    if len(payload.points) != 4:
        raise HTTPException(status_code=400, detail="Exactly 4 points are required")

    cleaned_path = os.path.join(DATA_DIR, "jobs", job_id_str, "cleaned", f"{page}.png")
    if not os.path.exists(cleaned_path):
        raise HTTPException(status_code=404, detail="Page image not found")

    img = cv2.imread(cleaned_path)
    if img is None:
        raise HTTPException(status_code=400, detail="Unable to read page image")

    warped = _warp_by_points(img, payload.points)
    if warped is None:
        raise HTTPException(status_code=400, detail="Invalid corner points")

    cv2.imwrite(cleaned_path, warped)
    if _should_reparse_after_page_edit(job_id_str):
        _reparse_single_page(job_id_str, page)
    return {"ok": True, "page": page}


@app.post("/jobs/{job_id}/pages/{page}/flatten/reset")
def reset_flatten_page(job_id: uuid.UUID, page: str, user: AuthUser = Depends(get_current_user)):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=True)
    job_id_str = str(job_id)
    raw_path = os.path.join(DATA_DIR, "jobs", job_id_str, "pages", f"{page}.png")
    cleaned_path = os.path.join(DATA_DIR, "jobs", job_id_str, "cleaned", f"{page}.png")

    if not os.path.exists(raw_path):
        raise HTTPException(status_code=404, detail="Original page not found")

    restored = clean_page(raw_path)
    cv2.imwrite(cleaned_path, restored)
    if _should_reparse_after_page_edit(job_id_str):
        _reparse_single_page(job_id_str, page)
    return {"ok": True, "page": page}


@app.post("/jobs/{job_id}/pages/{page}/image-tool")
def apply_image_tool(
    job_id: uuid.UUID,
    page: str,
    payload: ImageToolRequest,
    user: AuthUser = Depends(get_current_user),
):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=True)

    tool = str(payload.tool or "").strip().lower()
    if tool not in IMAGE_TOOLS:
        raise HTTPException(status_code=400, detail="unsupported_image_tool")

    job_id_str = str(job_id)
    page_name = _normalize_page_name(page)
    job_dir = os.path.join(DATA_DIR, "jobs", job_id_str)
    cleaned_dir = os.path.join(job_dir, "cleaned")
    cleaned_path = os.path.join(cleaned_dir, f"{page_name}.png")
    source_path = _resolve_job_page_image_path(job_id_str, page_name, prefer_cleaned=False)

    if not source_path and not os.path.exists(cleaned_path):
        raise HTTPException(status_code=404, detail="page_image_not_found")

    os.makedirs(cleaned_dir, exist_ok=True)

    if tool == "reset":
        if not source_path:
            raise HTTPException(status_code=404, detail="original_page_not_found")
        reset_img = clean_page(source_path)
        cv2.imwrite(cleaned_path, reset_img)
    else:
        if not os.path.exists(cleaned_path):
            if not source_path:
                raise HTTPException(status_code=404, detail="page_image_not_found")
            seeded = clean_page(source_path)
            cv2.imwrite(cleaned_path, seeded)

        img = cv2.imread(cleaned_path)
        if img is None:
            raise HTTPException(status_code=400, detail="unable_to_read_page_image")

        processed = _apply_image_tool(img, tool)
        cv2.imwrite(cleaned_path, processed)

    if _should_reparse_after_page_edit(job_id_str):
        _reparse_single_page(job_id_str, page_name)

    return {"ok": True, "page": page_name, "tool": tool}


def _resolve_job_page_image_path(job_id: str, page: str, prefer_cleaned: bool = True) -> Optional[str]:
    page_name = page if str(page).startswith("page_") else f"page_{str(page).zfill(3)}"
    filename = f"{page_name}.png"
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)

    cleaned_path = os.path.join(job_dir, "cleaned", filename)
    if prefer_cleaned and os.path.exists(cleaned_path):
        return cleaned_path

    raw_path = os.path.join(job_dir, "pages", filename)
    if os.path.exists(raw_path):
        return raw_path

    preview_dir = os.path.join(job_dir, "preview")
    preview_path = os.path.join(preview_dir, filename)
    if os.path.exists(preview_path):
        return preview_path

    if _generate_preview_page_if_missing(job_id, filename, preview_path):
        return preview_path

    return None


def _ocr_section_text_tesseract(section_bgr: np.ndarray, x_offset: int = 0, y_offset: int = 0) -> Dict:
    if section_bgr is None or section_bgr.size == 0:
        return {"text": "", "word_count": 0, "confidence": None, "words": []}

    gray = cv2.cvtColor(section_bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    config = "--oem 1 --psm 6 -c preserve_interword_spaces=1"

    data = pytesseract.image_to_data(
        binary,
        output_type=pytesseract.Output.DICT,
        config=config,
        lang="eng",
    )
    words: List[str] = []
    word_boxes: List[Dict] = []
    confs: List[float] = []
    n = len(data.get("text", []))
    for i in range(n):
        text = str(data["text"][i] or "").strip()
        if not text:
            continue
        words.append(text)

        try:
            left = int(data.get("left", [0])[i])
            top = int(data.get("top", [0])[i])
            width = int(data.get("width", [0])[i])
            height = int(data.get("height", [0])[i])
        except Exception:
            left = 0
            top = 0
            width = 0
            height = 0
        if width > 0 and height > 0:
            x1 = float(max(0, x_offset + left))
            y1 = float(max(0, y_offset + top))
            x2 = float(max(x1 + 1.0, x_offset + left + width))
            y2 = float(max(y1 + 1.0, y_offset + top + height))
            word_boxes.append(
                {
                    "text": text,
                    "x1": x1,
                    "y1": y1,
                    "x2": x2,
                    "y2": y2,
                }
            )

        conf_raw = data.get("conf", ["-1"])[i]
        try:
            conf = float(conf_raw)
        except Exception:
            conf = -1.0
        if conf >= 0:
            confs.append(conf)

    text_out = " ".join(words).strip()
    if not text_out:
        fallback = pytesseract.image_to_string(binary, config=config, lang="eng")
        text_out = " ".join(str(fallback or "").split())

    avg_conf = round((sum(confs) / len(confs)) / 100.0, 4) if confs else None
    return {
        "text": text_out,
        "word_count": len(words),
        "confidence": avg_conf,
        "words": word_boxes,
    }


def _build_guided_rows_from_sections(sections_out: List[Dict], guide_state: Optional["EvaluatorGuideState"]) -> List[Dict]:
    if not sections_out:
        return []
    role_order = []
    if guide_state and isinstance(guide_state.column_layout, list):
        for col in guide_state.column_layout:
            key = str(getattr(col, "key", "") or "").strip().lower()
            if key in {"date", "description", "debit", "credit", "balance"}:
                role_order.append(key)
    if not role_order:
        role_order = ["date", "description", "debit", "credit", "balance"]

    by_row: Dict[str, List[Dict]] = {}
    for sec in sections_out:
        y1 = float(sec.get("y1") or 0.0)
        y2 = float(sec.get("y2") or 0.0)
        row_key = f"{round(y1, 6)}:{round(y2, 6)}"
        by_row.setdefault(row_key, []).append(sec)

    rows: List[Dict] = []
    row_index = 1
    for row_key in sorted(by_row.keys(), key=lambda key: float(key.split(":")[0])):
        cells = sorted(by_row[row_key], key=lambda c: float(c.get("x1") or 0.0))
        row = {
            "row_id": f"{row_index:03}",
            "date": "",
            "description": "",
            "debit": "",
            "credit": "",
            "balance": "",
            "x1": min(float(c.get("x1") or 0.0) for c in cells),
            "y1": min(float(c.get("y1") or 0.0) for c in cells),
            "x2": max(float(c.get("x2") or 0.0) for c in cells),
            "y2": max(float(c.get("y2") or 0.0) for c in cells),
        }
        for idx, cell in enumerate(cells):
            text = str(cell.get("text") or "").strip()
            if not text:
                continue
            role = role_order[idx] if idx < len(role_order) else "description"
            if role == "description":
                row["description"] = f"{row['description']} {text}".strip()
            elif role == "date":
                if not row["date"]:
                    row["date"] = text
            elif role == "debit":
                if not row["debit"]:
                    row["debit"] = text
            elif role == "credit":
                if not row["credit"]:
                    row["credit"] = text
            elif role == "balance":
                if not row["balance"]:
                    row["balance"] = text
        combined = " ".join([row["date"], row["description"], row["debit"], row["credit"], row["balance"]]).lower()
        header_hits = 0
        if "date" in combined:
            header_hits += 1
        if "description" in combined or "particular" in combined:
            header_hits += 1
        if "debit" in combined or "credit" in combined or "balance" in combined:
            header_hits += 1
        if header_hits >= 2:
            continue
        if any(str(row.get(k) or "").strip() for k in ("date", "description", "debit", "credit", "balance")):
            rows.append(row)
            row_index += 1
    return rows


def _normalize_guided_rows(rows: List[Dict], page_name: str) -> tuple[List[Dict], List[Dict]]:
    out_rows: List[Dict] = []
    out_bounds: List[Dict] = []
    for idx, row in enumerate(rows, start=1):
        date_text = str(row.get("date") or "").strip()
        desc_text = str(row.get("description") or "").strip()
        debit_text = str(row.get("debit") or "").strip()
        credit_text = str(row.get("credit") or "").strip()
        bal_text = str(row.get("balance") or "").strip()
        date_iso = normalize_date(date_text, ["mdy", "dmy", "ymd"]) if date_text else None
        debit = normalize_amount(debit_text) if debit_text else None
        credit = normalize_amount(credit_text) if credit_text else None
        balance = normalize_amount(bal_text) if bal_text else None
        # Keep rows with at least date+balance or any debit/credit amount.
        if not ((date_iso and balance) or debit or credit):
            continue
        row_id = f"{idx:03}"
        out_rows.append(
            {
                "row_id": row_id,
                "page": page_name,
                "date": date_iso or date_text,
                "description": desc_text,
                "debit": debit or "",
                "credit": credit or "",
                "balance": balance or "",
            }
        )
        out_bounds.append(
            {
                "row_id": row_id,
                "x1": row.get("x1"),
                "y1": row.get("y1"),
                "x2": row.get("x2"),
                "y2": row.get("y2"),
            }
        )
    return out_rows, out_bounds


@app.post("/jobs/{job_id}/pages/{page}/ocr-sections")
def ocr_page_sections(
    job_id: uuid.UUID,
    page: str,
    payload: SectionOcrRequest,
    user: AuthUser = Depends(get_current_user),
):
    job = _get_job_record_or_404(job_id)
    _authorize_job_access(job, user, write=False)

    if not payload.sections:
        raise HTTPException(status_code=400, detail="sections_required")
    if len(payload.sections) > 500:
        raise HTTPException(status_code=400, detail="too_many_sections")

    page_name = page if str(page).startswith("page_") else f"page_{str(page).zfill(3)}"
    image_path = _resolve_job_page_image_path(str(job_id), page_name)
    if not image_path:
        raise HTTPException(status_code=404, detail="page_image_not_found")

    image = cv2.imread(image_path)
    if image is None:
        raise HTTPException(status_code=400, detail="unable_to_read_page_image")

    img_h, img_w = image.shape[:2]
    sections_out = []
    analyzer_words: List[Dict] = []

    try:
        for idx, sec in enumerate(payload.sections, start=1):
            x1n = max(0.0, min(1.0, min(float(sec.x1), float(sec.x2))))
            x2n = max(0.0, min(1.0, max(float(sec.x1), float(sec.x2))))
            y1n = max(0.0, min(1.0, min(float(sec.y1), float(sec.y2))))
            y2n = max(0.0, min(1.0, max(float(sec.y1), float(sec.y2))))

            x1 = max(0, min(img_w - 1, int(math.floor(x1n * img_w))))
            y1 = max(0, min(img_h - 1, int(math.floor(y1n * img_h))))
            x2 = max(x1 + 1, min(img_w, int(math.ceil(x2n * img_w))))
            y2 = max(y1 + 1, min(img_h, int(math.ceil(y2n * img_h))))

            crop = image[y1:y2, x1:x2]
            ocr = _ocr_section_text_tesseract(crop, x_offset=x1, y_offset=y1)
            analyzer_words.extend(ocr.get("words", []))
            sections_out.append(
                {
                    "index": idx,
                    "x1": x1n,
                    "y1": y1n,
                    "x2": x2n,
                    "y2": y2n,
                    "pixel_bounds": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
                    "text": ocr.get("text", ""),
                    "word_count": ocr.get("word_count", 0),
                    "confidence": ocr.get("confidence"),
                }
            )
    except pytesseract.TesseractError as exc:
        logger.warning("Section OCR failed for job %s page %s", str(job_id), page_name, exc_info=exc)
        raise HTTPException(status_code=500, detail="tesseract_failed")

    combined_text = "\n".join(str(item.get("text") or "").strip() for item in sections_out if str(item.get("text") or "").strip())
    guided_rows_payload: List[Dict] = _build_guided_rows_from_sections(sections_out, payload.guide_state)
    analyzer_meta = {
        "triggered": False,
        "result": "skipped",
        "reason": "no_ocr_profiles_sampled",
        "provider": AI_ANALYZER_PROVIDER,
        "model": AI_ANALYZER_MODEL,
        "profile_name": None,
    }
    parser_profile = detect_bank_profile(combined_text)
    try:
        analyzer_layout_pages = []
        if combined_text or analyzer_words:
            analyzer_layout_pages = [
                {
                    "text": combined_text,
                    "words": analyzer_words,
                    "width": float(max(img_w, 1)),
                    "height": float(max(img_h, 1)),
                }
            ]
        analyzer_meta = _run_ai_profile_analyzer_for_job(
            str(job_id),
            analyzer_layout_pages,
            allow_ocr_fallback=False,
            force=True,
        )
        needs_guided_retry = str(analyzer_meta.get("result") or "").lower() in {"failed", "rejected", "skipped"}
        if needs_guided_retry and guided_rows_payload:
            guided_meta = analyze_unknown_bank_and_apply_guided(
                layout_pages=analyzer_layout_pages,
                guided_payload={"rows": guided_rows_payload},
                sample_pages=1,
                min_rows=max(1, min(AI_ANALYZER_MIN_ROWS, 3)),
                min_date_ratio=AI_ANALYZER_MIN_DATE_RATIO,
                min_balance_ratio=AI_ANALYZER_MIN_BAL_RATIO,
            )
            if str(guided_meta.get("result") or "").lower() in {"applied", "matched"}:
                analyzer_meta = guided_meta
                _persist_job_analyzer_meta(str(job_id), analyzer_meta)
        selected_name = str(analyzer_meta.get("profile_name") or "").strip()
        if selected_name and selected_name in PROFILES:
            parser_profile = PROFILES[selected_name]
    except Exception as exc:
        logger.warning("Deferred AI profile analyzer failed for job %s", str(job_id), exc_info=exc)

    parsed_rows: List[Dict] = []
    parsed_bounds: List[Dict] = []
    parse_diag: Dict = {}
    guided_rows: List[Dict] = []
    guided_bounds: List[Dict] = []
    try:
        page_rows, page_bounds, parse_diag = parse_page_with_profile_fallback(
            analyzer_words,
            float(max(img_w, 1)),
            float(max(img_h, 1)),
            parser_profile,
        )
        tx_rows = [row for row in page_rows if is_transaction_row(row, parser_profile)]
        id_map: Dict[str, str] = {}
        for n, row in enumerate(tx_rows, start=1):
            old_id = str(row.get("row_id") or "")
            new_id = f"{n:03}"
            row["row_id"] = new_id
            id_map[old_id] = new_id
            parsed_rows.append(
                {
                    "row_id": new_id,
                    "page": page_name,
                    "date": row.get("date") or "",
                    "description": row.get("description") or "",
                    "debit": row.get("debit") or "",
                    "credit": row.get("credit") or "",
                    "balance": row.get("balance") or "",
                }
            )
        for b in page_bounds:
            old_id = str(b.get("row_id") or "")
            if old_id not in id_map:
                continue
            parsed_bounds.append(
                {
                    "row_id": id_map[old_id],
                    "x1": b.get("x1"),
                    "y1": b.get("y1"),
                    "x2": b.get("x2"),
                    "y2": b.get("y2"),
                }
            )
        guided_rows, guided_bounds = _normalize_guided_rows(guided_rows_payload, page_name)
        if len(guided_rows) > len(parsed_rows):
            parsed_rows = guided_rows
            parsed_bounds = guided_bounds
            parse_diag = {
                **(parse_diag or {}),
                "source": "guided_sections",
                "guided_rows_count": len(guided_rows),
            }
    except Exception as exc:
        logger.warning("Section OCR parse failed for job %s page %s", str(job_id), page_name, exc_info=exc)
        parsed_rows = []
        parsed_bounds = []
        parse_diag = {"error": "section_parse_failed"}

    return {
        "page": page_name,
        "backend": "tesseract",
        "section_count": len(sections_out),
        "sections": sections_out,
        "combined_text": combined_text,
        "profile_detected": parser_profile.name if parser_profile else "GENERIC",
        "parsed_rows": parsed_rows,
        "parsed_bounds": parsed_bounds,
        "parse_diagnostics_page": parse_diag,
        "profile_analyzer": analyzer_meta,
    }


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
    if (not account_identity.get("account_name") or not account_identity.get("account_number")) and profile_text:
        ai_identity = analyze_account_identity_from_text(profile_text)
        if not account_identity.get("account_name"):
            account_identity["account_name"] = ai_identity.get("account_name")
        if not account_identity.get("account_number"):
            account_identity["account_number"] = ai_identity.get("account_number")
    account_name_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_identity.get("account_name"), page)
    account_number_bbox = find_value_bounds(parser_words, parser_w, parser_h, account_identity.get("account_number"), page)
    if parse_mode == "ocr" and ocr_items:
        ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
        account_identity = extract_account_identity(ocr_text, profile)
        if not account_identity.get("account_name") or not account_identity.get("account_number"):
            ai_identity = analyze_account_identity_from_text(ocr_text)
            if not account_identity.get("account_name"):
                account_identity["account_name"] = ai_identity.get("account_name")
            if not account_identity.get("account_number"):
                account_identity["account_number"] = ai_identity.get("account_number")
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


def _backfill_job_account_identity(job_id: str, diagnostics: Dict, diagnostics_path: Optional[str]) -> Dict:
    if not isinstance(diagnostics, dict):
        return diagnostics

    job = diagnostics.setdefault("job", {})
    account_name = str(job.get("account_name") or "").strip()
    account_number = str(job.get("account_number") or "").strip()
    if not _identity_missing(account_name) and not _identity_missing(account_number):
        return diagnostics

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")
    page = "page_001"

    first_text = ""
    first_words: List[Dict] = []
    first_w = 1.0
    first_h = 1.0

    if os.path.exists(input_pdf):
        try:
            layouts = extract_pdf_layout_pages(input_pdf)
            if layouts:
                first = layouts[0] if isinstance(layouts[0], dict) else {}
                first_text = str(first.get("text") or "").strip()
                first_words = first.get("words", []) if isinstance(first.get("words", []), list) else []
                first_w = float(first.get("width", 1) or 1)
                first_h = float(first.get("height", 1) or 1)
        except Exception:
            first_text = ""
            first_words = []

    profile = detect_bank_profile(first_text)
    identity = extract_account_identity(first_text, profile)
    if (not identity.get("account_name") or not identity.get("account_number")) and first_text:
        ai_identity = analyze_account_identity_from_text(first_text)
        if not identity.get("account_name"):
            identity["account_name"] = ai_identity.get("account_name")
        if not identity.get("account_number"):
            identity["account_number"] = ai_identity.get("account_number")
        if ai_identity.get("account_name") or ai_identity.get("account_number"):
            job["account_identity_source"] = "ai_first_page"
            job["account_identity_ai_attempted"] = True
            job["account_identity_ai_result"] = ai_identity.get("result")
            job["account_identity_ai_reason"] = ai_identity.get("reason")

    # OCR fallback when text layer is unavailable.
    if (not identity.get("account_name") or not identity.get("account_number")):
        ocr_path = os.path.join(job_dir, "ocr", f"{page}.json")
        if os.path.exists(ocr_path):
            try:
                with open(ocr_path) as f:
                    ocr_items = json.load(f)
                ocr_text = " ".join((item.get("text") or "") for item in ocr_items)
                ocr_words = _ocr_items_to_words(ocr_items)
                profile_ocr = detect_bank_profile(ocr_text)
                identity_ocr = extract_account_identity(ocr_text, profile_ocr)
                if not identity.get("account_name"):
                    identity["account_name"] = identity_ocr.get("account_name")
                if not identity.get("account_number"):
                    identity["account_number"] = identity_ocr.get("account_number")
                if (not identity.get("account_name") or not identity.get("account_number")) and ocr_text:
                    ai_identity = analyze_account_identity_from_text(ocr_text)
                    if not identity.get("account_name"):
                        identity["account_name"] = ai_identity.get("account_name")
                    if not identity.get("account_number"):
                        identity["account_number"] = ai_identity.get("account_number")
                    if ai_identity.get("account_name") or ai_identity.get("account_number"):
                        job["account_identity_source"] = "ai_first_page_ocr"
                        job["account_identity_ai_attempted"] = True
                        job["account_identity_ai_result"] = ai_identity.get("result")
                        job["account_identity_ai_reason"] = ai_identity.get("reason")
                if ocr_words:
                    first_words = ocr_words
                    max_x = max(float(w.get("x2", 0.0)) for w in ocr_words) if ocr_words else 1.0
                    max_y = max(float(w.get("y2", 0.0)) for w in ocr_words) if ocr_words else 1.0
                    first_w = max(1.0, max_x)
                    first_h = max(1.0, max_y)
            except Exception as exc:
                logger.warning("OCR fallback account identity backfill failed for job %s", job_id, exc_info=exc)

    changed = False
    if identity.get("account_name") and (_identity_missing(job.get("account_name")) or identity.get("account_name") != job.get("account_name")):
        job["account_name"] = identity.get("account_name")
        changed = True
    if identity.get("account_number") and (_identity_missing(job.get("account_number")) or identity.get("account_number") != job.get("account_number")):
        job["account_number"] = identity.get("account_number")
        changed = True

    if first_words and first_w > 0 and first_h > 0:
        name_bbox = find_value_bounds(first_words, first_w, first_h, job.get("account_name"), page)
        number_bbox = find_value_bounds(first_words, first_w, first_h, job.get("account_number"), page)
        if name_bbox and name_bbox != job.get("account_name_bbox"):
            job["account_name_bbox"] = name_bbox
            changed = True
        if number_bbox and number_bbox != job.get("account_number_bbox"):
            job["account_number_bbox"] = number_bbox
            changed = True

    if changed and diagnostics_path:
        try:
            with open(diagnostics_path, "w") as f:
                json.dump(diagnostics, f, indent=2)
        except Exception as exc:
            logger.warning("Failed to persist diagnostics backfill for job %s", job_id, exc_info=exc)
    return diagnostics


def _identity_missing(value) -> bool:
    text = str(value or "").strip().lower()
    return text in {"", "-", "none", "null", "n/a", "na"}


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


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_minimal_report_pdf(submission_id: str, summary: Dict, rows: List[Dict]) -> bytes:
    lines = [
        "Executive Summary Report",
        f"Submission: {submission_id}",
        "",
        f"Total Transactions: {summary.get('total_transactions')}",
        f"Debit Transactions: {summary.get('debit_transactions')}",
        f"Credit Transactions: {summary.get('credit_transactions')}",
        f"Total Debit: {summary.get('total_debit')}",
        f"Total Credit: {summary.get('total_credit')}",
        f"Ending Balance: {summary.get('ending_balance')}",
        f"ADB: {summary.get('adb')}",
        "",
        "Top Transactions:",
    ]
    for row in rows[:25]:
        lines.append(
            f"{row.get('date') or '-'} | {row.get('description') or '-'} | D:{row.get('debit')} C:{row.get('credit')} B:{row.get('balance')}"
        )

    content = ["BT", "/F1 11 Tf", "40 790 Td", "14 TL"]
    for idx, line in enumerate(lines):
        safe = _pdf_escape(str(line))
        if idx == 0:
            content.append(f"({safe}) Tj")
        else:
            content.append(f"T* ({safe}) Tj")
    content.append("ET")
    stream = "\n".join(content).encode("latin-1", errors="replace")

    objects = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
    )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode("ascii") + stream + b"\nendstream")

    output = bytearray(b"%PDF-1.4\n")
    xref = [0]
    for i, obj in enumerate(objects, start=1):
        xref.append(len(output))
        output.extend(f"{i} 0 obj\n".encode("ascii"))
        output.extend(obj)
        output.extend(b"\nendobj\n")

    xref_pos = len(output)
    output.extend(f"xref\n0 {len(objects)+1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for off in xref[1:]:
        output.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    output.extend(
        f"trailer << /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    )
    return bytes(output)
