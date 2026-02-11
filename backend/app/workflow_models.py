import datetime as dt
import uuid

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def _json_col():
    # JSONB works on Postgres; SQLAlchemy maps this type correctly for configured DB.
    return JSONB


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)


class Submission(Base):
    __tablename__ = "submissions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    assigned_evaluator_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="uploaded", index=True)
    borrower_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lead_reference: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_pdf_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    current_job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    summary_snapshot_json: Mapped[dict | None] = mapped_column(_json_col(), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)

    agent = relationship("User", foreign_keys=[agent_id], lazy="joined")
    assigned_evaluator = relationship("User", foreign_keys=[assigned_evaluator_id], lazy="joined")


class JobRecord(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    submission_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("submissions.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", index=True)
    step: Mapped[str | None] = mapped_column(String(64), nullable=True)
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    parse_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ocr_backend: Mapped[str | None] = mapped_column(String(64), nullable=True)
    diagnostics_json: Mapped[dict | None] = mapped_column(_json_col(), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)
    completed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    submission_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("submissions.id"), nullable=False, index=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=True, index=True)
    page: Mapped[str | None] = mapped_column(String(32), nullable=True)
    row_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    date: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    debit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    credit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    balance: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    x1: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    y1: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    x2: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    y2: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    is_manual_edit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    submission_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("submissions.id"), nullable=False, index=True)
    generated_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    report_type: Mapped[str] = mapped_column(String(64), nullable=False, default="executive_summary")
    blob_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, index=True)
    submission_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("submissions.id"), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    before_json: Mapped[dict | None] = mapped_column(_json_col(), nullable=True)
    after_json: Mapped[dict | None] = mapped_column(_json_col(), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)

