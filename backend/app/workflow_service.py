import datetime as dt
import json
import os
import uuid
from typing import Dict, List, Optional, Tuple

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.auth_service import AuthUser
from app.db import SessionLocal
from app.workflow_models import AuditLog, JobRecord, Report, Submission, SubmissionPage, Transaction, User


ALLOWED_STATES = {"for_review", "processing", "summary_generated", "failed"}
PARSE_STATUS_VALUES = {"pending", "processing", "done", "failed"}
REVIEW_STATUS_VALUES = {"pending", "in_review", "saved", "reviewed"}


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ".-")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_date(value: str) -> Optional[dt.date]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def compute_summary(rows: List[Dict]) -> Dict:
    tx_count = len(rows)
    debit_count = 0
    credit_count = 0
    total_debit = 0.0
    total_credit = 0.0
    daily_balances: Dict[dt.date, float] = {}
    monthly: Dict[str, Dict] = {}

    normalized = []
    for idx, row in enumerate(rows):
        date = _parse_date(row.get("date"))
        debit = _to_float(row.get("debit"))
        credit = _to_float(row.get("credit"))
        balance = _to_float(row.get("balance"))
        if debit is not None and abs(debit) > 0:
            debit_count += 1
            total_debit += abs(debit)
        if credit is not None and abs(credit) > 0:
            credit_count += 1
            total_credit += abs(credit)
        if date and balance is not None:
            daily_balances[date] = balance
        normalized.append((idx, date, debit, credit, balance))

    ending_balance = None
    for _, _, _, _, bal in normalized[::-1]:
        if bal is not None:
            ending_balance = bal
            break

    sorted_days = sorted(daily_balances.items(), key=lambda t: t[0])
    adb = None
    if sorted_days:
        weighted = 0.0
        total_days = 0
        for i, (day, bal) in enumerate(sorted_days):
            next_day = sorted_days[i + 1][0] if i < len(sorted_days) - 1 else day + dt.timedelta(days=1)
            span = max(1, (next_day - day).days)
            weighted += bal * span
            total_days += span
        if total_days > 0:
            adb = weighted / total_days

    for _, date, debit, credit, balance in normalized:
        if not date:
            continue
        key = date.strftime("%Y-%m")
        bucket = monthly.setdefault(
            key,
            {
                "month": key,
                "debit": 0.0,
                "credit": 0.0,
                "debit_count": 0,
                "credit_count": 0,
                "balance_weighted": 0.0,
                "days": 0,
            },
        )
        if debit is not None:
            bucket["debit"] += abs(debit)
            if abs(debit) > 0:
                bucket["debit_count"] += 1
        if credit is not None:
            bucket["credit"] += abs(credit)
            if abs(credit) > 0:
                bucket["credit_count"] += 1
        if balance is not None:
            bucket["balance_weighted"] += balance
            bucket["days"] += 1

    monthly_list = []
    for key in sorted(monthly.keys()):
        item = monthly[key]
        monthly_list.append(
            {
                "month": key,
                "debit": round(item["debit"], 2),
                "credit": round(item["credit"], 2),
                "avg_debit": round((item["debit"] / item["debit_count"]), 2) if item["debit_count"] else 0.0,
                "avg_credit": round((item["credit"] / item["credit_count"]), 2) if item["credit_count"] else 0.0,
                "adb": round((item["balance_weighted"] / item["days"]), 2) if item["days"] else 0.0,
            }
        )

    return {
        "total_transactions": tx_count,
        "debit_transactions": debit_count,
        "credit_transactions": credit_count,
        "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
        "ending_balance": round(ending_balance, 2) if ending_balance is not None else None,
        "adb": round(adb, 2) if adb is not None else None,
        "monthly": monthly_list,
    }


def _normalize_page_key(page_key: str | None) -> str:
    raw = str(page_key or "").strip().lower().replace(".png", "")
    if not raw:
        return ""
    if raw.startswith("page_"):
        return raw
    if raw.isdigit():
        return f"page_{int(raw):03d}"
    return raw


def _page_index_from_key(page_key: str) -> int:
    token = str(page_key or "").replace("page_", "")
    if token.isdigit():
        return int(token)
    return 0


def _serialize_submission_page(page: SubmissionPage) -> Dict:
    return {
        "page_key": page.page_key,
        "index": page.page_index,
        "parse_status": page.parse_status,
        "review_status": page.review_status,
        "saved_at": page.last_saved_at.isoformat() if page.last_saved_at else None,
        "rows_count": int(page.rows_count or 0),
        "has_unsaved": bool(page.has_unsaved),
        "parse_error": page.parse_error,
        "updated_at": page.updated_at.isoformat() if page.updated_at else None,
    }


def _compute_review_progress(pages: List[SubmissionPage]) -> Dict:
    total_pages = len(pages)
    parsed_pages = sum(1 for p in pages if p.parse_status == "done")
    reviewed_pages = sum(1 for p in pages if p.review_status in {"saved", "reviewed"})
    percent = int(round((reviewed_pages / total_pages) * 100)) if total_pages else 0
    return {
        "total_pages": total_pages,
        "parsed_pages": parsed_pages,
        "reviewed_pages": reviewed_pages,
        "percent": percent,
    }


def _all_pages_reviewed(pages: List[SubmissionPage]) -> bool:
    return bool(pages) and all(p.review_status in {"saved", "reviewed"} for p in pages)


def _refresh_submission_summary_and_state(db: Session, sub: Submission):
    rows = list(
        db.scalars(
            select(Transaction)
            .where(Transaction.submission_id == sub.id)
            .order_by(Transaction.page.asc().nulls_last(), Transaction.row_index.asc())
        )
    )
    merged = [serialize_transaction(t) for t in rows]
    sub.summary_snapshot_json = compute_summary(merged)

    pages = list(db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == sub.id)))
    if _all_pages_reviewed(pages):
        if sub.status != "summary_generated":
            sub.status = "for_review"
    elif sub.status != "failed":
        sub.status = "for_review"


def create_submission_with_job(
    agent_user: AuthUser,
    input_pdf_key: str,
    job_id: uuid.UUID,
    parse_mode: str,
    borrower_name: Optional[str],
    lead_reference: Optional[str],
    notes: Optional[str],
) -> Tuple[Submission, JobRecord]:
    db = SessionLocal()
    try:
        sub = Submission(
            agent_id=agent_user.id,
            status="for_review",
            borrower_name=borrower_name,
            lead_reference=lead_reference,
            notes=notes,
            input_pdf_key=input_pdf_key,
            current_job_id=job_id,
        )
        db.add(sub)
        db.flush()
        job = JobRecord(
            id=job_id,
            submission_id=sub.id,
            status="for_review",
            step="queued",
            progress=0,
            parse_mode=parse_mode,
        )
        db.add(job)
        db.add(
            AuditLog(
                actor_user_id=agent_user.id,
                submission_id=sub.id,
                action="agent_submission_created",
                before_json=None,
                after_json={"job_id": str(job_id), "status": "for_review"},
            )
        )
        db.commit()
        db.refresh(sub)
        db.refresh(job)
        return sub, job
    finally:
        db.close()


def _submission_for_user(db: Session, submission_id: uuid.UUID, user: AuthUser) -> Submission:
    sub = db.scalar(select(Submission).where(Submission.id == submission_id))
    if not sub:
        raise ValueError("submission_not_found")
    if user.role == "agent" and sub.agent_id != user.id:
        raise PermissionError("forbidden_submission")
    if user.role == "credit_evaluator" and sub.assigned_evaluator_id not in {None, user.id}:
        raise PermissionError("forbidden_submission")
    return sub


def list_submissions_for_agent(agent_user: AuthUser) -> List[Submission]:
    db = SessionLocal()
    try:
        return list(
            db.scalars(
                select(Submission)
                .where(Submission.agent_id == agent_user.id)
                .order_by(Submission.created_at.desc())
            )
        )
    finally:
        db.close()


def get_submission_for_user(submission_id: uuid.UUID, user: AuthUser) -> Submission:
    db = SessionLocal()
    try:
        return _submission_for_user(db, submission_id, user)
    finally:
        db.close()


def list_submissions_for_evaluator(evaluator_user: AuthUser, include_unassigned: bool = False) -> List[Submission]:
    db = SessionLocal()
    try:
        if include_unassigned:
            return list(
                db.scalars(
                    select(Submission)
                    .where((Submission.assigned_evaluator_id == evaluator_user.id) | (Submission.assigned_evaluator_id.is_(None)))
                    .order_by(Submission.updated_at.desc())
                )
            )
        return list(
            db.scalars(
                select(Submission)
                .where(Submission.assigned_evaluator_id == evaluator_user.id)
                .order_by(Submission.updated_at.desc())
            )
        )
    finally:
        db.close()


def assign_submission_to_evaluator(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Submission:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id and sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("already_assigned")
        before = {"assigned_evaluator_id": str(sub.assigned_evaluator_id) if sub.assigned_evaluator_id else None}
        sub.assigned_evaluator_id = evaluator_user.id
        if sub.status not in {"processing", "failed", "summary_generated"}:
            sub.status = "for_review"
        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=sub.id,
                action="submission_assigned",
                before_json=before,
                after_json={"assigned_evaluator_id": str(evaluator_user.id)},
            )
        )
        db.commit()
        db.refresh(sub)
        return sub
    finally:
        db.close()


def upsert_job_status(job_id: uuid.UUID | str, status_data: Dict):
    db = SessionLocal()
    try:
        jid = uuid.UUID(str(job_id))
        job = db.scalar(select(JobRecord).where(JobRecord.id == jid))
        if not job:
            return
        job.status = str(status_data.get("status") or job.status or "processing")
        job.step = str(status_data.get("step") or job.step or "")
        job.progress = int(status_data.get("progress") or 0)
        if status_data.get("parse_mode"):
            job.parse_mode = str(status_data.get("parse_mode"))
        if status_data.get("ocr_backend"):
            job.ocr_backend = str(status_data.get("ocr_backend"))
        job.diagnostics_json = dict(status_data)
        if job.status in {"done", "failed"}:
            job.completed_at = dt.datetime.utcnow()

        submission = None
        if job.submission_id:
            submission = db.scalar(select(Submission).where(Submission.id == job.submission_id))
        if submission:
            total_pages = int(status_data.get("pages") or 0)
            if total_pages > 0:
                existing_pages = {
                    p.page_key: p for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission.id))
                }
                for i in range(1, total_pages + 1):
                    page_key = f"page_{i:03d}"
                    page = existing_pages.get(page_key)
                    if page is None:
                        page = SubmissionPage(
                            submission_id=submission.id,
                            job_id=job.id,
                            page_key=page_key,
                            page_index=i,
                            parse_status="pending",
                            review_status="pending",
                            rows_count=0,
                            has_unsaved=False,
                        )
                        db.add(page)
                    elif page.job_id is None:
                        page.job_id = job.id

            if job.status == "failed":
                submission.status = "failed"
                for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission.id)):
                    if p.parse_status in {"pending", "processing"}:
                        p.parse_status = "failed"
                        p.parse_error = str(status_data.get("message") or "processing_failed")
            elif job.status == "done":
                if submission.status != "summary_generated":
                    submission.status = "for_review"
                for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission.id)):
                    if p.parse_status in {"pending", "processing"}:
                        p.parse_status = "done"
                        p.last_parsed_at = dt.datetime.utcnow()
            elif job.status in {"queued", "processing"}:
                submission.status = "processing"
                for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission.id)):
                    if p.parse_status == "pending":
                        p.parse_status = "processing"
        db.commit()
    finally:
        db.close()


def get_submission_id_for_job(job_id: uuid.UUID | str) -> Optional[uuid.UUID]:
    db = SessionLocal()
    try:
        jid = uuid.UUID(str(job_id))
        job = db.scalar(select(JobRecord).where(JobRecord.id == jid))
        if not job:
            return None
        return job.submission_id
    finally:
        db.close()


def replace_submission_transactions(
    submission_id: uuid.UUID,
    job_id: uuid.UUID | None,
    rows_by_page: Dict[str, List[Dict]],
    bounds_by_page: Dict[str, List[Dict]],
):
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            return

        now = dt.datetime.utcnow()
        db.execute(delete(Transaction).where(Transaction.submission_id == submission_id))
        existing_pages = {
            p.page_key: p for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id))
        }

        for page, rows in rows_by_page.items():
            page_key = _normalize_page_key(page)
            page_index = _page_index_from_key(page_key)
            bounds_map = {str(b.get("row_id") or ""): b for b in (bounds_by_page.get(page) or [])}
            page_obj = existing_pages.get(page_key)
            if page_obj is None:
                page_obj = SubmissionPage(
                    submission_id=submission_id,
                    job_id=job_id,
                    page_key=page_key,
                    page_index=page_index,
                    parse_status="done",
                    review_status="pending",
                    rows_count=len(rows),
                    has_unsaved=False,
                    last_parsed_at=now,
                )
                db.add(page_obj)
            else:
                page_obj.job_id = job_id
                page_obj.page_index = page_index
                page_obj.rows_count = len(rows)
                page_obj.has_unsaved = False
                page_obj.last_parsed_at = now
                if page_obj.parse_status in {"pending", "processing"}:
                    page_obj.parse_status = "done"
                if page_obj.review_status not in REVIEW_STATUS_VALUES:
                    page_obj.review_status = "pending"
            for idx, row in enumerate(rows, start=1):
                rid = str(row.get("row_id") or "")
                b = bounds_map.get(rid) or {}
                db.add(
                    Transaction(
                        submission_id=submission_id,
                        job_id=job_id,
                        page=page_key,
                        row_index=idx,
                        date=row.get("date"),
                        description=row.get("description"),
                        debit=_to_float(row.get("debit")),
                        credit=_to_float(row.get("credit")),
                        balance=_to_float(row.get("balance")),
                        x1=_to_float(b.get("x1")),
                        y1=_to_float(b.get("y1")),
                        x2=_to_float(b.get("x2")),
                        y2=_to_float(b.get("y2")),
                        is_manual_edit=False,
                    )
                )
        merged_rows = []
        for page_key in sorted(rows_by_page.keys()):
            merged_rows.extend(rows_by_page.get(page_key) or [])
        sub.summary_snapshot_json = compute_summary(merged_rows)
        if sub.status != "summary_generated":
            sub.status = "for_review"
        db.commit()
    finally:
        db.close()


def sync_job_results(job_id: uuid.UUID | str, parsed_rows: Dict[str, List[Dict]], bounds: Dict[str, List[Dict]], diagnostics: Dict):
    submission_id = None
    jid = uuid.UUID(str(job_id))
    db = SessionLocal()
    try:
        job = db.scalar(select(JobRecord).where(JobRecord.id == jid))
        if not job or not job.submission_id:
            return
        submission_id = job.submission_id
        job.diagnostics_json = diagnostics
        pages_diag = (diagnostics or {}).get("pages", {}) if isinstance(diagnostics, dict) else {}
        existing = {
            p.page_key: p for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id))
        }
        page_keys = sorted(set((parsed_rows or {}).keys()) | set((pages_diag or {}).keys()))
        now = dt.datetime.utcnow()
        for page_key_raw in page_keys:
            page_key = _normalize_page_key(page_key_raw)
            page_obj = existing.get(page_key)
            page_rows = (parsed_rows or {}).get(page_key_raw) or (parsed_rows or {}).get(page_key) or []
            diag = (pages_diag or {}).get(page_key_raw) or (pages_diag or {}).get(page_key) or {}
            parse_status = "failed" if str(diag.get("source_type") or "") == "none" else "done"
            parse_error = str(diag.get("fallback_reason") or "").strip() or None
            if page_obj is None:
                page_obj = SubmissionPage(
                    submission_id=submission_id,
                    job_id=jid,
                    page_key=page_key,
                    page_index=_page_index_from_key(page_key),
                    parse_status=parse_status,
                    review_status="pending",
                    rows_count=len(page_rows),
                    has_unsaved=False,
                    last_parsed_at=now if parse_status == "done" else None,
                    parse_error=parse_error if parse_status == "failed" else None,
                )
                db.add(page_obj)
            else:
                page_obj.job_id = jid
                page_obj.page_index = _page_index_from_key(page_key)
                page_obj.parse_status = parse_status
                page_obj.rows_count = len(page_rows)
                page_obj.parse_error = parse_error if parse_status == "failed" else None
                if parse_status == "done":
                    page_obj.last_parsed_at = now
        db.commit()
    finally:
        db.close()
    if submission_id:
        replace_submission_transactions(submission_id, jid, parsed_rows, bounds)


def sync_submission_page_result(
    job_id: uuid.UUID | str,
    page_key: str,
    rows: List[Dict],
    page_diagnostic: Dict | None = None,
):
    db = SessionLocal()
    try:
        jid = uuid.UUID(str(job_id))
        job = db.scalar(select(JobRecord).where(JobRecord.id == jid))
        if not job or not job.submission_id:
            return
        sub = db.scalar(select(Submission).where(Submission.id == job.submission_id))
        if not sub:
            return
        key = _normalize_page_key(page_key)
        page = db.scalar(
            select(SubmissionPage).where(
                SubmissionPage.submission_id == sub.id,
                SubmissionPage.page_key == key,
            )
        )
        parse_status = "done"
        parse_error = None
        if isinstance(page_diagnostic, dict):
            if str(page_diagnostic.get("source_type") or "") == "none":
                parse_status = "failed"
                parse_error = str(page_diagnostic.get("fallback_reason") or "").strip() or "parse_failed"
        if page is None:
            page = SubmissionPage(
                submission_id=sub.id,
                job_id=jid,
                page_key=key,
                page_index=_page_index_from_key(key),
                parse_status=parse_status,
                review_status="pending",
                rows_count=len(rows or []),
                has_unsaved=False,
                parse_error=parse_error,
                last_parsed_at=dt.datetime.utcnow() if parse_status == "done" else None,
            )
            db.add(page)
        else:
            page.job_id = jid
            page.page_index = _page_index_from_key(key)
            page.parse_status = parse_status
            page.rows_count = len(rows or [])
            page.parse_error = parse_error
            if parse_status == "done":
                page.last_parsed_at = dt.datetime.utcnow()
            if page.review_status == "pending":
                page.review_status = "in_review"

        if sub.status != "summary_generated":
            sub.status = "processing"
        db.commit()
    finally:
        db.close()


def get_transactions_for_submission(submission_id: uuid.UUID) -> List[Transaction]:
    db = SessionLocal()
    try:
        return list(
            db.scalars(
                select(Transaction)
                .where(Transaction.submission_id == submission_id)
                .order_by(Transaction.date.asc().nulls_last(), Transaction.page.asc().nulls_last(), Transaction.row_index.asc())
            )
        )
    finally:
        db.close()


def list_submission_pages(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        pages = list(
            db.scalars(
                select(SubmissionPage)
                .where(SubmissionPage.submission_id == submission_id)
                .order_by(SubmissionPage.page_index.asc(), SubmissionPage.page_key.asc())
            )
        )
        progress = _compute_review_progress(pages)
        return {
            "pages": [_serialize_submission_page(p) for p in pages],
            "review_progress": progress,
            "can_export": _all_pages_reviewed(pages),
        }
    finally:
        db.close()


def ensure_submission_pages(submission_id: uuid.UUID, total_pages: int, job_id: uuid.UUID | None = None) -> None:
    if total_pages <= 0:
        return
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            return
        existing = {
            p.page_key: p for p in db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id))
        }
        for i in range(1, total_pages + 1):
            key = f"page_{i:03d}"
            page = existing.get(key)
            if page is None:
                db.add(
                    SubmissionPage(
                        submission_id=submission_id,
                        job_id=job_id or sub.current_job_id,
                        page_key=key,
                        page_index=i,
                        parse_status="pending",
                        review_status="pending",
                        rows_count=0,
                        has_unsaved=False,
                    )
                )
            elif job_id and page.job_id is None:
                page.job_id = job_id
        db.commit()
    finally:
        db.close()


def get_submission_page(submission_id: uuid.UUID, page_key: str, evaluator_user: AuthUser) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        key = _normalize_page_key(page_key)
        page = db.scalar(
            select(SubmissionPage).where(
                SubmissionPage.submission_id == submission_id,
                SubmissionPage.page_key == key,
            )
        )
        if not page:
            raise ValueError("page_not_found")
        rows = list(
            db.scalars(
                select(Transaction)
                .where(Transaction.submission_id == submission_id, Transaction.page == key)
                .order_by(Transaction.row_index.asc())
            )
        )
        return {
            "page_status": _serialize_submission_page(page),
            "rows": [serialize_transaction(t) for t in rows],
        }
    finally:
        db.close()


def persist_page_transactions(
    submission_id: uuid.UUID,
    page_key: str,
    evaluator_user: AuthUser,
    rows: List[Dict],
) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        if sub.status == "failed":
            raise ValueError("submission_failed")

        key = _normalize_page_key(page_key)
        page = db.scalar(
            select(SubmissionPage).where(
                SubmissionPage.submission_id == submission_id,
                SubmissionPage.page_key == key,
            )
        )
        if not page:
            page = SubmissionPage(
                submission_id=submission_id,
                job_id=sub.current_job_id,
                page_key=key,
                page_index=_page_index_from_key(key),
                parse_status="done",
                review_status="in_review",
                rows_count=0,
                has_unsaved=True,
            )
            db.add(page)
            db.flush()

        before_rows = [
            {
                "date": t.date,
                "description": t.description,
                "debit": float(t.debit) if t.debit is not None else None,
                "credit": float(t.credit) if t.credit is not None else None,
                "balance": float(t.balance) if t.balance is not None else None,
                "page": t.page,
                "row_index": t.row_index,
            }
            for t in db.scalars(
                select(Transaction).where(
                    Transaction.submission_id == submission_id,
                    Transaction.page == key,
                )
            )
        ]

        db.execute(
            delete(Transaction).where(
                Transaction.submission_id == submission_id,
                Transaction.page == key,
            )
        )

        normalized = []
        for idx, row in enumerate(rows, start=1):
            payload = {
                "row_id": str(row.get("row_id") or f"{idx:03}"),
                "date": row.get("date"),
                "description": row.get("description"),
                "debit": _to_float(row.get("debit")),
                "credit": _to_float(row.get("credit")),
                "balance": _to_float(row.get("balance")),
                "page": key,
                "x1": _to_float(row.get("x1")),
                "y1": _to_float(row.get("y1")),
                "x2": _to_float(row.get("x2")),
                "y2": _to_float(row.get("y2")),
            }
            normalized.append(payload)
            db.add(
                Transaction(
                    submission_id=submission_id,
                    job_id=sub.current_job_id,
                    page=key,
                    row_index=idx,
                    date=payload["date"],
                    description=payload["description"],
                    debit=payload["debit"],
                    credit=payload["credit"],
                    balance=payload["balance"],
                    x1=payload["x1"],
                    y1=payload["y1"],
                    x2=payload["x2"],
                    y2=payload["y2"],
                    is_manual_edit=True,
                )
            )

        now = dt.datetime.utcnow()
        page.job_id = sub.current_job_id
        page.rows_count = len(normalized)
        page.has_unsaved = False
        page.last_saved_at = now
        page.review_status = "saved"
        if page.parse_status not in PARSE_STATUS_VALUES:
            page.parse_status = "done"
        if page.parse_status != "failed":
            page.parse_status = "done"
            page.last_parsed_at = page.last_parsed_at or now

        _refresh_submission_summary_and_state(db, sub)

        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="transactions_updated_page",
                before_json={"page": key, "rows_count": len(before_rows), "rows": before_rows[:200]},
                after_json={"page": key, "rows_count": len(normalized)},
            )
        )
        db.commit()

        pages = list(
            db.scalars(
                select(SubmissionPage).where(SubmissionPage.submission_id == submission_id)
            )
        )
        progress = _compute_review_progress(pages)
        return {
            "summary": sub.summary_snapshot_json or {},
            "page_status": _serialize_submission_page(page),
            "review_progress": progress,
            "can_export": _all_pages_reviewed(pages),
        }
    finally:
        db.close()


def mark_page_reviewed(submission_id: uuid.UUID, page_key: str, evaluator_user: AuthUser) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        key = _normalize_page_key(page_key)
        page = db.scalar(
            select(SubmissionPage).where(
                SubmissionPage.submission_id == submission_id,
                SubmissionPage.page_key == key,
            )
        )
        if not page:
            raise ValueError("page_not_found")
        page.review_status = "reviewed"
        page.has_unsaved = False
        page.last_saved_at = dt.datetime.utcnow()
        _refresh_submission_summary_and_state(db, sub)
        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="submission_page_reviewed",
                before_json={"page": key},
                after_json={"page": key, "review_status": "reviewed"},
            )
        )
        db.commit()
        pages = list(db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id)))
        return {
            "page_status": _serialize_submission_page(page),
            "review_progress": _compute_review_progress(pages),
            "can_export": _all_pages_reviewed(pages),
        }
    finally:
        db.close()


def get_submission_review_status(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        pages = list(
            db.scalars(
                select(SubmissionPage)
                .where(SubmissionPage.submission_id == submission_id)
                .order_by(SubmissionPage.page_index.asc(), SubmissionPage.page_key.asc())
            )
        )
        if not pages:
            tx_count = int(
                db.scalar(
                    select(func.count())
                    .select_from(Transaction)
                    .where(Transaction.submission_id == submission_id)
                )
                or 0
            )
            can_export = tx_count > 0
            return {
                "all_pages_reviewed": can_export,
                "missing_pages": [],
                "can_export": can_export,
                "review_progress": {
                    "total_pages": 0,
                    "parsed_pages": 0,
                    "reviewed_pages": 0,
                    "percent": 100 if can_export else 0,
                },
            }
        missing_pages = [p.page_key for p in pages if p.review_status not in {"saved", "reviewed"}]
        return {
            "all_pages_reviewed": not missing_pages and bool(pages),
            "missing_pages": missing_pages,
            "can_export": _all_pages_reviewed(pages),
            "review_progress": _compute_review_progress(pages),
        }
    finally:
        db.close()


def can_generate_exports(submission_id: uuid.UUID, evaluator_user: AuthUser) -> bool:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        pages = list(db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id)))
        if pages:
            return _all_pages_reviewed(pages)
        tx_count = int(
            db.scalar(
                select(func.count())
                .select_from(Transaction)
                .where(Transaction.submission_id == submission_id)
            )
            or 0
        )
        return tx_count > 0
    finally:
        db.close()


def finish_review_and_build_summary(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Dict:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")

        pages = list(
            db.scalars(
                select(SubmissionPage)
                .where(SubmissionPage.submission_id == submission_id)
                .order_by(SubmissionPage.page_index.asc(), SubmissionPage.page_key.asc())
            )
        )
        now = dt.datetime.utcnow()
        for page in pages:
            page.review_status = "reviewed"
            page.has_unsaved = False
            page.last_saved_at = now

        tx_rows = [serialize_transaction(t) for t in get_transactions_for_submission(submission_id)]
        summary = compute_summary(tx_rows)
        sub.summary_snapshot_json = summary
        if sub.status != "summary_generated":
            sub.status = "in_review" if pages else sub.status

        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="submission_finish_review",
                before_json={"pages": len(pages)},
                after_json={"pages_reviewed": len(pages), "tx_count": len(tx_rows)},
            )
        )
        db.commit()

        pages = list(
            db.scalars(
                select(SubmissionPage)
                .where(SubmissionPage.submission_id == submission_id)
                .order_by(SubmissionPage.page_index.asc(), SubmissionPage.page_key.asc())
            )
        )
        progress = _compute_review_progress(pages)
        return {
            "summary": summary,
            "review_progress": progress,
            "can_export": bool(tx_rows),
            "all_pages_reviewed": True,
            "missing_pages": [],
        }
    finally:
        db.close()


def set_page_parse_status(
    submission_id: uuid.UUID,
    page_key: str,
    parse_status: str,
    parse_error: Optional[str] = None,
) -> None:
    status = str(parse_status or "").strip().lower()
    if status not in PARSE_STATUS_VALUES:
        status = "pending"
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            return
        key = _normalize_page_key(page_key)
        page = db.scalar(
            select(SubmissionPage).where(
                SubmissionPage.submission_id == submission_id,
                SubmissionPage.page_key == key,
            )
        )
        if page is None:
            page = SubmissionPage(
                submission_id=submission_id,
                job_id=sub.current_job_id,
                page_key=key,
                page_index=_page_index_from_key(key),
                parse_status=status,
                review_status="pending",
                rows_count=0,
                has_unsaved=False,
            )
            db.add(page)
        else:
            page.parse_status = status
        if status == "done":
            page.parse_error = None
            page.last_parsed_at = dt.datetime.utcnow()
        elif status == "failed":
            page.parse_error = parse_error or "parse_failed"
        db.commit()
    finally:
        db.close()


def persist_evaluator_transactions(
    submission_id: uuid.UUID,
    evaluator_user: AuthUser,
    rows: List[Dict],
) -> Dict:
    grouped: Dict[str, List[Dict]] = {}
    for row in rows:
        key = _normalize_page_key(str(row.get("page") or ""))
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    # Backward-compatible behavior: rewrite pages, then return summary.
    summary = {}
    for key in sorted(grouped.keys(), key=_page_index_from_key):
        result = persist_page_transactions(submission_id, key, evaluator_user, grouped[key])
        summary = result.get("summary") or summary
    return summary


def set_submission_summary_ready(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Submission:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        if sub.status not in {"for_review", "processing"}:
            raise ValueError("invalid_state_transition")
        pages = list(db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id)))
        if pages and not _all_pages_reviewed(pages):
            raise ValueError("review_incomplete")
        before = {"status": sub.status}
        sub.status = "summary_generated"
        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="submission_summary_ready",
                before_json=before,
                after_json={"status": "summary_generated"},
            )
        )
        db.commit()
        db.refresh(sub)
        return sub
    finally:
        db.close()


def set_submission_summary_generated(submission_id: uuid.UUID, evaluator_user: AuthUser) -> Submission:
    db = SessionLocal()
    try:
        sub = db.scalar(select(Submission).where(Submission.id == submission_id))
        if not sub:
            raise ValueError("submission_not_found")
        if sub.assigned_evaluator_id != evaluator_user.id:
            raise PermissionError("forbidden_submission")
        pages = list(db.scalars(select(SubmissionPage).where(SubmissionPage.submission_id == submission_id)))
        if pages and not _all_pages_reviewed(pages):
            raise ValueError("review_incomplete")
        before = {"status": sub.status}
        sub.status = "summary_generated"
        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="submission_summary_generated",
                before_json=before,
                after_json={"status": "summary_generated"},
            )
        )
        db.commit()
        db.refresh(sub)
        return sub
    finally:
        db.close()


def create_report_record(submission_id: uuid.UUID, user: AuthUser, blob_key: str, report_type: str = "executive_summary") -> Report:
    db = SessionLocal()
    try:
        existing_versions = list(db.scalars(select(Report).where(Report.submission_id == submission_id)))
        version = len(existing_versions) + 1
        report = Report(
            submission_id=submission_id,
            generated_by=user.id,
            report_type=report_type,
            blob_key=blob_key,
            version=version,
        )
        db.add(report)
        db.add(
            AuditLog(
                actor_user_id=user.id,
                submission_id=submission_id,
                action="report_generated",
                before_json=None,
                after_json={"blob_key": blob_key, "report_type": report_type, "version": version},
            )
        )
        db.commit()
        db.refresh(report)
        return report
    finally:
        db.close()


def serialize_submission(sub: Submission) -> Dict:
    return {
        "id": str(sub.id),
        "status": sub.status,
        "agent_id": str(sub.agent_id),
        "agent_email": (sub.agent.email if getattr(sub, "agent", None) is not None else None),
        "assigned_evaluator_id": str(sub.assigned_evaluator_id) if sub.assigned_evaluator_id else None,
        "borrower_name": sub.borrower_name,
        "lead_reference": sub.lead_reference,
        "notes": sub.notes,
        "input_pdf_key": sub.input_pdf_key,
        "current_job_id": str(sub.current_job_id) if sub.current_job_id else None,
        "summary_snapshot": sub.summary_snapshot_json,
        "created_at": sub.created_at.isoformat() if sub.created_at else None,
        "updated_at": sub.updated_at.isoformat() if sub.updated_at else None,
    }


def serialize_transaction(t: Transaction) -> Dict:
    return {
        "id": str(t.id),
        "submission_id": str(t.submission_id),
        "job_id": str(t.job_id) if t.job_id else None,
        "page": t.page,
        "row_index": t.row_index,
        "date": t.date,
        "description": t.description,
        "debit": float(t.debit) if t.debit is not None else None,
        "credit": float(t.credit) if t.credit is not None else None,
        "balance": float(t.balance) if t.balance is not None else None,
        "x1": float(t.x1) if t.x1 is not None else None,
        "y1": float(t.y1) if t.y1 is not None else None,
        "x2": float(t.x2) if t.x2 is not None else None,
        "y2": float(t.y2) if t.y2 is not None else None,
        "is_manual_edit": bool(t.is_manual_edit),
    }
