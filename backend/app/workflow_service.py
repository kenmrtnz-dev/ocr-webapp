import datetime as dt
import json
import os
import uuid
from typing import Dict, List, Optional, Tuple

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.auth_service import AuthUser
from app.db import SessionLocal
from app.workflow_models import AuditLog, JobRecord, Report, Submission, Transaction, User


ALLOWED_STATES = {"for_review", "processing", "summary_generated", "failed"}


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
                "balance_weighted": 0.0,
                "days": 0,
            },
        )
        if debit is not None:
            bucket["debit"] += abs(debit)
        if credit is not None:
            bucket["credit"] += abs(credit)
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
            if job.status == "failed":
                submission.status = "failed"
            elif job.status == "done":
                if submission.status != "summary_generated":
                    submission.status = "for_review"
            elif job.status in {"queued", "processing"}:
                submission.status = "processing"
        db.commit()
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

        db.execute(delete(Transaction).where(Transaction.submission_id == submission_id))

        for page, rows in rows_by_page.items():
            bounds_map = {str(b.get("row_id") or ""): b for b in (bounds_by_page.get(page) or [])}
            for idx, row in enumerate(rows, start=1):
                rid = str(row.get("row_id") or "")
                b = bounds_map.get(rid) or {}
                db.add(
                    Transaction(
                        submission_id=submission_id,
                        job_id=job_id,
                        page=page,
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
        for page in sorted(rows_by_page.keys()):
            merged_rows.extend(rows_by_page.get(page) or [])
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
        db.commit()
    finally:
        db.close()
    if submission_id:
        replace_submission_transactions(submission_id, jid, parsed_rows, bounds)


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


def persist_evaluator_transactions(
    submission_id: uuid.UUID,
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
            for t in db.scalars(select(Transaction).where(Transaction.submission_id == submission_id))
        ]

        db.execute(delete(Transaction).where(Transaction.submission_id == submission_id))
        normalized = []
        for idx, row in enumerate(rows, start=1):
            payload = {
                "row_id": str(row.get("row_id") or f"{idx:03}"),
                "date": row.get("date"),
                "description": row.get("description"),
                "debit": _to_float(row.get("debit")),
                "credit": _to_float(row.get("credit")),
                "balance": _to_float(row.get("balance")),
                "page": row.get("page"),
            }
            normalized.append(payload)
            db.add(
                Transaction(
                    submission_id=submission_id,
                    job_id=sub.current_job_id,
                    page=payload["page"],
                    row_index=idx,
                    date=payload["date"],
                    description=payload["description"],
                    debit=payload["debit"],
                    credit=payload["credit"],
                    balance=payload["balance"],
                    is_manual_edit=True,
                )
            )

        summary = compute_summary(normalized)
        sub.summary_snapshot_json = summary
        if sub.status != "summary_generated":
            sub.status = "for_review"
        db.add(
            AuditLog(
                actor_user_id=evaluator_user.id,
                submission_id=submission_id,
                action="transactions_updated",
                before_json={"rows_count": len(before_rows), "rows": before_rows[:200]},
                after_json={"rows_count": len(normalized), "summary": summary},
            )
        )
        db.commit()
        return summary
    finally:
        db.close()


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
