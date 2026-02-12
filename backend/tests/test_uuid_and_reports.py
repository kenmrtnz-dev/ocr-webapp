import uuid

from app import main


class _FakeDb:
    def scalar(self, _stmt):
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
