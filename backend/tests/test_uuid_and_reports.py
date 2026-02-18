import datetime as dt
import types
import uuid
from pathlib import Path

from app import main


class _FakeDb:
    def scalar(self, _stmt):
        return None

    def close(self):
        return None


class _DownloadDb:
    def __init__(self, report, submission):
        self.report = report
        self.submission = submission

    def scalar(self, stmt):
        entity = (stmt.column_descriptions or [{}])[0].get("entity")
        if entity is main.Report:
            return self.report
        if entity is main.Submission:
            return self.submission
        return None

    def close(self):
        return None


def test_report_download_invalid_uuid_returns_422(client_factory):
    with client_factory(role="credit_evaluator") as client:
        res = client.get("/evaluator/reports/not-a-uuid/download")
    assert res.status_code == 422


def test_report_download_missing_report_returns_404(client_factory, monkeypatch):
    monkeypatch.setattr(main, "SessionLocal", lambda: _FakeDb())
    report_id = uuid.uuid4()

    with client_factory(role="credit_evaluator") as client:
        res = client.get(f"/evaluator/reports/{report_id}/download")

    assert res.status_code == 404
    assert res.json().get("detail") == "report_not_found"


def test_report_download_filename_uses_combined_contract(client_factory, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    evaluator_id = uuid.uuid4()
    report_id = uuid.uuid4()
    submission_id = uuid.uuid4()
    pdf_path = Path(tmp_path, "report.pdf")
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")

    report = types.SimpleNamespace(
        id=report_id,
        submission_id=submission_id,
        blob_key=f"reports/{submission_id}/{report_id}.pdf",
        created_at=dt.datetime(2026, 2, 18, 14, 30, tzinfo=dt.timezone.utc),
    )
    submission = types.SimpleNamespace(
        id=submission_id,
        assigned_evaluator_id=evaluator_id,
        borrower_name=" Acme  Trading ",
    )

    monkeypatch.setattr(main, "SessionLocal", lambda: _DownloadDb(report=report, submission=submission))
    monkeypatch.setattr(main, "blob_abs_path", lambda _blob_key: str(pdf_path))

    with client_factory(role="credit_evaluator", user_id=evaluator_id) as client:
        res = client.get(f"/evaluator/reports/{report_id}/download")

    assert res.status_code == 200
    disposition = str(res.headers.get("content-disposition") or "")
    assert "021820261430-ACME-TRADING-6MOS-BANKSTATEMENTS.pdf" in disposition
