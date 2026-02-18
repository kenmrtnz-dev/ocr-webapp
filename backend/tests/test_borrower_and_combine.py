import json
import datetime as dt
import re
import types
import uuid
from pathlib import Path

from app import main
from app import workflow_service
from app.auth_service import AuthUser


class _CreateSession:
    def __init__(self):
        self.added = []

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        return None

    def commit(self):
        return None

    def refresh(self, _obj):
        return None

    def close(self):
        return None


class _CombineSession:
    def __init__(self, submissions, first_job=None):
        self.submissions = list(submissions)
        self.first_job = first_job
        self.added = []

    def scalar(self, stmt):
        entity = (stmt.column_descriptions or [{}])[0].get("entity")
        if entity is workflow_service.JobRecord:
            return self.first_job
        return None

    def scalars(self, stmt):
        entity = (stmt.column_descriptions or [{}])[0].get("entity")
        if entity is workflow_service.Submission:
            return list(self.submissions)
        return []

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        return None

    def commit(self):
        return None

    def rollback(self):
        return None

    def refresh(self, _obj):
        return None

    def close(self):
        return None


def test_agent_submission_blank_borrower_returns_422(client_factory):
    with client_factory(role="agent") as client:
        res = client.post(
            "/agent/submissions",
            files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
            data={"borrower_name": "   "},
        )

    assert res.status_code == 422
    assert res.json().get("detail") == "borrower_required"


def test_create_submission_with_job_normalizes_borrower_name(monkeypatch):
    session = _CreateSession()
    monkeypatch.setattr(workflow_service, "SessionLocal", lambda: session)

    agent = AuthUser(id=uuid.uuid4(), email="agent@example.com", role="agent")
    sub, _job = workflow_service.create_submission_with_job(
        agent_user=agent,
        input_pdf_key="jobs/test/input/document.pdf",
        job_id=uuid.uuid4(),
        parse_mode="text",
        borrower_name="  jUAN   dELA   cRUZ ",
        lead_reference=None,
        notes=None,
    )

    assert sub.borrower_name == "Juan Dela Cruz"


def test_build_combined_filename_formats_business_name():
    ts = dt.datetime(2026, 2, 18, 14, 30, tzinfo=dt.timezone.utc)
    filename = workflow_service.build_combined_filename("  Acme   Trading ", ts)
    assert filename == "021820261430-ACME-TRADING-6MOS-BANKSTATEMENTS.pdf"


def test_build_combined_filename_sanitizes_invalid_filename_chars():
    ts = dt.datetime(2026, 2, 18, 14, 30, tzinfo=dt.timezone.utc)
    filename = workflow_service.build_combined_filename(' A/C\\M:E*? "Trading" <Ltd>| ', ts)
    assert filename == "021820261430-A-C-M-E-TRADING-LTD-6MOS-BANKSTATEMENTS.pdf"


def test_evaluator_combine_mixed_borrower_returns_400(client_factory, monkeypatch):
    evaluator_id = uuid.uuid4()
    submission_a = types.SimpleNamespace(
        id=uuid.uuid4(),
        assigned_evaluator_id=evaluator_id,
        borrower_name=" Juan Dela Cruz ",
        current_job_id=uuid.uuid4(),
        input_pdf_key="jobs/a/input/document.pdf",
        agent_id=uuid.uuid4(),
        lead_reference="L-001",
        notes=None,
    )
    submission_b = types.SimpleNamespace(
        id=uuid.uuid4(),
        assigned_evaluator_id=evaluator_id,
        borrower_name="Maria Dela Cruz",
        current_job_id=uuid.uuid4(),
        input_pdf_key="jobs/b/input/document.pdf",
        agent_id=submission_a.agent_id,
        lead_reference="L-001",
        notes=None,
    )

    session = _CombineSession(
        submissions=[submission_a, submission_b],
        first_job=types.SimpleNamespace(parse_mode="text"),
    )
    monkeypatch.setattr(workflow_service, "SessionLocal", lambda: session)
    monkeypatch.setattr(workflow_service, "write_blob", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(workflow_service, "_merge_pdf_blob_keys", lambda _keys: b"%PDF-1.4\n%%EOF")

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.post(
            "/evaluator/submissions/combine",
            json={"submission_ids": [str(submission_a.id), str(submission_b.id)]},
        )

    assert res.status_code == 400
    assert res.json().get("detail") == "borrower_name_mismatch"


def test_evaluator_combine_success_returns_new_submission_id(client_factory, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    evaluator_id = uuid.uuid4()
    agent_id = uuid.uuid4()
    source_a = types.SimpleNamespace(
        id=uuid.uuid4(),
        assigned_evaluator_id=evaluator_id,
        borrower_name="  juan   dela cruz",
        current_job_id=uuid.uuid4(),
        input_pdf_key="jobs/a/input/document.pdf",
        agent_id=agent_id,
        lead_reference="L-900",
        notes="source",
    )
    source_b = types.SimpleNamespace(
        id=uuid.uuid4(),
        assigned_evaluator_id=evaluator_id,
        borrower_name="JUAN DELA CRUZ",
        current_job_id=uuid.uuid4(),
        input_pdf_key="jobs/b/input/document.pdf",
        agent_id=agent_id,
        lead_reference="L-900",
        notes="source",
    )

    merged_calls = {}

    def _fake_merge(keys):
        merged_calls["keys"] = list(keys)
        return b"%PDF-1.4\n%%EOF"

    def _fake_write(blob_key, _data):
        merged_calls["blob_key"] = blob_key
        return blob_key

    session = _CombineSession(
        submissions=[source_a, source_b],
        first_job=types.SimpleNamespace(parse_mode="ocr"),
    )
    monkeypatch.setattr(workflow_service, "SessionLocal", lambda: session)
    monkeypatch.setattr(workflow_service, "_merge_pdf_blob_keys", _fake_merge)
    monkeypatch.setattr(workflow_service, "write_blob", _fake_write)

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.post(
            "/evaluator/submissions/combine",
            json={"submission_ids": [str(source_a.id), str(source_b.id)]},
        )

    assert res.status_code == 200
    body = res.json()
    assert uuid.UUID(body["submission_id"])
    assert uuid.UUID(body["job_id"])
    assert body["status"] == "for_review"

    assert merged_calls["keys"] == [source_a.input_pdf_key, source_b.input_pdf_key]
    assert merged_calls["blob_key"].startswith(f"jobs/{body['job_id']}/input/")

    status_path = Path(tmp_path, "jobs", body["job_id"], "status.json")
    assert status_path.exists()
    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload.get("status") == "for_review"

    meta_path = Path(tmp_path, "jobs", body["job_id"], "meta.json")
    assert meta_path.exists()
    meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
    assert re.match(r"^\d{12}-JUAN-DELA-CRUZ-6MOS-BANKSTATEMENTS\.pdf$", str(meta_payload.get("original_filename") or ""))
    assert meta_payload.get("source_submission_ids") == [str(source_a.id), str(source_b.id)]


def test_agent_submission_list_preserves_original_filename_for_non_combined(client_factory, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    job_id = uuid.uuid4()
    job_dir = Path(tmp_path, "jobs", str(job_id))
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "meta.json").write_text(json.dumps({"original_filename": "statement-original.pdf"}), encoding="utf-8")

    monkeypatch.setattr(main, "list_submissions_for_agent", lambda *_args, **_kwargs: [object()])
    monkeypatch.setattr(
        main,
        "serialize_submission",
        lambda _row: {
            "id": str(uuid.uuid4()),
            "current_job_id": str(job_id),
            "input_pdf_key": f"jobs/{job_id}/input/document.pdf",
        },
    )

    with client_factory(role="agent") as client:
        res = client.get("/agent/submissions")

    assert res.status_code == 200
    items = res.json().get("items") or []
    assert items and items[0].get("original_filename") == "statement-original.pdf"


def test_agent_submission_list_normalizes_legacy_combined_filename(client_factory, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    job_id = uuid.uuid4()
    job_dir = Path(tmp_path, "jobs", str(job_id))
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "meta.json").write_text(json.dumps({"original_filename": "combined.pdf"}), encoding="utf-8")

    monkeypatch.setattr(main, "list_submissions_for_agent", lambda *_args, **_kwargs: [object()])
    monkeypatch.setattr(
        main,
        "serialize_submission",
        lambda _row: {
            "id": str(uuid.uuid4()),
            "current_job_id": str(job_id),
            "input_pdf_key": f"jobs/{job_id}/input/document.pdf",
            "borrower_name": "  jUAN   dELA   cRUZ ",
            "created_at": "2026-02-18T14:30:45+00:00",
        },
    )

    with client_factory(role="agent") as client:
        res = client.get("/agent/submissions")

    assert res.status_code == 200
    items = res.json().get("items") or []
    assert items and items[0].get("original_filename") == "021820261430-JUAN-DELA-CRUZ-6MOS-BANKSTATEMENTS.pdf"
