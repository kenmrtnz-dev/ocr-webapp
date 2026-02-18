import json
import os
import re
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app import workflow_service
from app.db import Base
from app.workflow_models import AuditLog, JobRecord, Submission, User



def _resolve_postgres_test_url() -> str | None:
    candidates = [
        os.getenv("TEST_DATABASE_URL"),
        os.getenv("TEST_POSTGRES_URL"),
        os.getenv("DATABASE_URL"),
        "postgresql+psycopg://ocr:ocrpass@localhost:5432/ocr",
    ]
    for candidate in candidates:
        raw = str(candidate or "").strip()
        if raw.startswith("postgresql"):
            return raw
    return None


@pytest.fixture
def postgres_session_factory():
    url = _resolve_postgres_test_url()
    if not url:
        pytest.skip("Postgres URL not configured for integration test")

    admin_engine = create_engine(url, future=True, pool_pre_ping=True)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("select 1"))
    except Exception as exc:
        admin_engine.dispose()
        pytest.skip(f"Postgres not reachable: {exc}")

    schema = f"combine_it_{uuid.uuid4().hex}"
    with admin_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    test_engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    Base.metadata.create_all(bind=test_engine)
    Session = sessionmaker(bind=test_engine, autoflush=False, autocommit=False, future=True)

    try:
        yield Session
    finally:
        test_engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()



def _seed_submission_pair(Session, evaluator_a: uuid.UUID, evaluator_b: uuid.UUID, borrower_a: str, borrower_b: str):
    db = Session()
    try:
        agent_id = uuid.uuid4()
        first_submission_id = uuid.uuid4()
        second_submission_id = uuid.uuid4()
        first_job_id = uuid.uuid4()
        second_job_id = uuid.uuid4()

        user_rows = {
            agent_id: User(
                id=agent_id,
                email=f"agent-{agent_id.hex}@example.com",
                password_hash="x",
                role="agent",
                is_active=True,
            ),
            evaluator_a: User(
                id=evaluator_a,
                email=f"eval-{evaluator_a.hex}@example.com",
                password_hash="x",
                role="credit_evaluator",
                is_active=True,
            ),
            evaluator_b: User(
                id=evaluator_b,
                email=f"eval-{evaluator_b.hex}@example.com",
                password_hash="x",
                role="credit_evaluator",
                is_active=True,
            ),
        }
        for user in user_rows.values():
            db.add(user)
        db.flush()

        db.add(
            Submission(
                id=first_submission_id,
                agent_id=agent_id,
                assigned_evaluator_id=evaluator_a,
                status="for_review",
                borrower_name=borrower_a,
                lead_reference="L-100",
                notes="n1",
                input_pdf_key="jobs/src_a/input/document.pdf",
                current_job_id=first_job_id,
            )
        )
        db.add(
            Submission(
                id=second_submission_id,
                agent_id=agent_id,
                assigned_evaluator_id=evaluator_b,
                status="for_review",
                borrower_name=borrower_b,
                lead_reference="L-100",
                notes="n2",
                input_pdf_key="jobs/src_b/input/document.pdf",
                current_job_id=second_job_id,
            )
        )
        db.flush()
        db.add(
            JobRecord(
                id=first_job_id,
                submission_id=first_submission_id,
                status="for_review",
                step="queued",
                progress=0,
                parse_mode="text",
            )
        )
        db.add(
            JobRecord(
                id=second_job_id,
                submission_id=second_submission_id,
                status="for_review",
                step="queued",
                progress=0,
                parse_mode="text",
            )
        )
        db.commit()
        return first_submission_id, second_submission_id
    finally:
        db.close()



def test_combine_success_creates_submission_job_and_audit_log(
    client_factory,
    app_with_temp_data,
    monkeypatch,
    postgres_session_factory,
):
    _app, tmp_path = app_with_temp_data
    evaluator_id = uuid.uuid4()

    sub_a, sub_b = _seed_submission_pair(
        postgres_session_factory,
        evaluator_id,
        evaluator_id,
        "  jUAN   dELA   cRUZ",
        "Juan Dela Cruz",
    )

    monkeypatch.setattr(workflow_service, "SessionLocal", postgres_session_factory)
    monkeypatch.setattr(workflow_service, "_merge_pdf_blob_keys", lambda _keys: b"%PDF-1.4\n%%EOF")
    monkeypatch.setattr(workflow_service, "write_blob", lambda *_args, **_kwargs: None)

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.post(
            "/evaluator/submissions/combine",
            json={"submission_ids": [str(sub_a), str(sub_b)]},
        )

    assert res.status_code == 200
    payload = res.json()
    new_submission_id = uuid.UUID(payload["submission_id"])
    new_job_id = uuid.UUID(payload["job_id"])
    meta_path = Path(tmp_path, "jobs", payload["job_id"], "meta.json")
    assert meta_path.exists()
    meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
    assert re.match(r"^\d{12}-JUAN-DELA-CRUZ-6MOS-BANKSTATEMENTS\.pdf$", str(meta_payload.get("original_filename") or ""))
    assert meta_payload.get("source_submission_ids") == [str(sub_a), str(sub_b)]

    db = postgres_session_factory()
    try:
        new_sub = db.scalar(select(Submission).where(Submission.id == new_submission_id))
        new_job = db.scalar(select(JobRecord).where(JobRecord.id == new_job_id))
        audit = db.scalar(
            select(AuditLog).where(
                AuditLog.submission_id == new_submission_id,
                AuditLog.action == "submission_combined",
            )
        )

        assert new_sub is not None
        assert new_sub.borrower_name == "Juan Dela Cruz"
        assert new_job is not None
        assert new_job.submission_id == new_submission_id
        assert audit is not None
        assert audit.submission_id == new_submission_id
    finally:
        db.close()



def test_combine_mixed_borrower_returns_400(client_factory, app_with_temp_data, monkeypatch, postgres_session_factory):
    _app, _tmp_path = app_with_temp_data
    evaluator_id = uuid.uuid4()

    sub_a, sub_b = _seed_submission_pair(
        postgres_session_factory,
        evaluator_id,
        evaluator_id,
        "Juan Dela Cruz",
        "Maria Dela Cruz",
    )

    monkeypatch.setattr(workflow_service, "SessionLocal", postgres_session_factory)
    monkeypatch.setattr(workflow_service, "_merge_pdf_blob_keys", lambda _keys: b"%PDF-1.4\n%%EOF")
    monkeypatch.setattr(workflow_service, "write_blob", lambda *_args, **_kwargs: None)

    db = postgres_session_factory()
    try:
        before_count = db.scalar(select(func.count()).select_from(Submission))
    finally:
        db.close()

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.post(
            "/evaluator/submissions/combine",
            json={"submission_ids": [str(sub_a), str(sub_b)]},
        )

    assert res.status_code == 400
    assert res.json().get("detail") == "borrower_name_mismatch"

    db = postgres_session_factory()
    try:
        after_count = db.scalar(select(func.count()).select_from(Submission))
        assert after_count == before_count
    finally:
        db.close()



def test_combine_forbidden_submission_returns_403(
    client_factory,
    app_with_temp_data,
    monkeypatch,
    postgres_session_factory,
):
    _app, _tmp_path = app_with_temp_data
    evaluator_id = uuid.uuid4()
    other_evaluator_id = uuid.uuid4()

    sub_a, sub_b = _seed_submission_pair(
        postgres_session_factory,
        evaluator_id,
        other_evaluator_id,
        "Juan Dela Cruz",
        "Juan Dela Cruz",
    )

    monkeypatch.setattr(workflow_service, "SessionLocal", postgres_session_factory)
    monkeypatch.setattr(workflow_service, "_merge_pdf_blob_keys", lambda _keys: b"%PDF-1.4\n%%EOF")
    monkeypatch.setattr(workflow_service, "write_blob", lambda *_args, **_kwargs: None)

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.post(
            "/evaluator/submissions/combine",
            json={"submission_ids": [str(sub_a), str(sub_b)]},
        )

    assert res.status_code == 403
    assert res.json().get("detail") == "forbidden_submission"
