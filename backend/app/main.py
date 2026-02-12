from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
import uuid, os, json
import logging
from typing import List, Dict, Optional
import shutil
import cv2
import numpy as np
import math
from pdf2image import convert_from_path
from PIL import Image

from app.celery_app import process_pdf, prepare_draft
from app.bank_profiles import detect_bank_profile, extract_account_identity, find_value_bounds, reload_profiles
from app.ocr_engine import ocr_image
from app.pdf_text_extract import extract_pdf_layout_pages
from app.profile_analyzer import analyze_account_identity_from_text
from app.statement_parser import parse_page_with_profile_fallback, is_transaction_row
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
from app.workflow_models import User, Submission, JobRecord, Transaction, Report, AuditLog
from app.workflow_service import (
    assign_submission_to_evaluator,
    compute_summary,
    create_report_record,
    create_submission_with_job,
    get_submission_for_user,
    get_transactions_for_submission,
    list_submissions_for_agent,
    list_submissions_for_evaluator,
    persist_evaluator_transactions,
    serialize_submission,
    serialize_transaction,
    set_submission_summary_generated,
    set_submission_summary_ready,
)
from sqlalchemy import select, delete, func

DATA_DIR = os.getenv("DATA_DIR", "./data")
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))

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
            "transactions": int(db.scalar(select(func.count()).select_from(Transaction)) or 0),
            "reports": int(db.scalar(select(func.count()).select_from(Report)) or 0),
            "audit_log": int(db.scalar(select(func.count()).select_from(AuditLog)) or 0),
        }
        db.execute(delete(AuditLog))
        db.execute(delete(Report))
        db.execute(delete(Transaction))
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

    if current_status in {"queued", "processing"}:
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
        if os.path.exists(status_path):
            with open(status_path) as f:
                job_payload = json.load(f)
        if os.path.exists(diagnostics_path):
            with open(diagnostics_path) as f:
                diagnostics = json.load(f)
        if os.path.exists(parsed_path):
            with open(parsed_path) as f:
                parsed = json.load(f)
        if os.path.exists(bounds_path):
            with open(bounds_path) as f:
                bounds = json.load(f)

    return {
        "submission": serialize_submission(sub),
        "job": job_payload,
        "transactions": tx_rows,
        "summary": sub.summary_snapshot_json,
        "diagnostics": diagnostics,
        "parsed": parsed,
        "bounds": bounds,
    }


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
        code = 404 if detail == "submission_not_found" else 400
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
        code = 404 if detail == "submission_not_found" else 400
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
