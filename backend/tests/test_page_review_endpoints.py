import types
import uuid

from app import main


def _submission(submission_id: uuid.UUID, evaluator_id: uuid.UUID):
    return types.SimpleNamespace(
        id=submission_id,
        assigned_evaluator_id=evaluator_id,
        current_job_id=uuid.uuid4(),
        status="for_review",
        summary_snapshot_json=None,
    )


def test_evaluator_pages_manifest_endpoint_ok(client_factory, monkeypatch):
    user_id = uuid.uuid4()
    submission_id = uuid.uuid4()

    monkeypatch.setattr(main, "get_submission_for_user", lambda *_args, **_kwargs: _submission(submission_id, user_id))
    monkeypatch.setattr(main, "_get_submission_job_dir", lambda _sub: None)
    monkeypatch.setattr(
        main,
        "list_submission_pages",
        lambda *_args, **_kwargs: {
            "pages": [
                {
                    "page_key": "page_001",
                    "index": 1,
                    "parse_status": "done",
                    "review_status": "pending",
                    "saved_at": None,
                    "rows_count": 12,
                    "has_unsaved": False,
                    "updated_at": None,
                }
            ],
            "review_progress": {"total_pages": 1, "parsed_pages": 1, "reviewed_pages": 0, "percent": 0},
            "can_export": False,
        },
    )

    with client_factory(role="credit_evaluator", user_id=user_id) as client:
        res = client.get(f"/evaluator/submissions/{submission_id}/pages")

    assert res.status_code == 200
    data = res.json()
    assert data["pages"][0]["page_key"] == "page_001"
    assert data["can_export"] is False


def test_page_save_conflict_returns_409(client_factory, monkeypatch):
    user_id = uuid.uuid4()
    submission_id = uuid.uuid4()

    monkeypatch.setattr(
        main,
        "get_submission_page",
        lambda *_args, **_kwargs: {
            "page_status": {
                "updated_at": "2026-02-16T09:00:00+00:00",
            },
            "rows": [],
        },
    )

    with client_factory(role="credit_evaluator", user_id=user_id) as client:
        res = client.patch(
            f"/evaluator/submissions/{submission_id}/pages/page_001/transactions",
            json={
                "rows": [],
                "expected_updated_at": "2026-02-16T10:00:00+00:00",
            },
        )

    assert res.status_code == 409
    assert res.json().get("detail") == "page_conflict_reload"


def test_report_and_excel_export_blocked_when_review_incomplete(client_factory, monkeypatch):
    user_id = uuid.uuid4()
    submission_id = uuid.uuid4()

    monkeypatch.setattr(main, "get_submission_for_user", lambda *_args, **_kwargs: _submission(submission_id, user_id))
    monkeypatch.setattr(main, "can_generate_exports", lambda *_args, **_kwargs: False)

    with client_factory(role="credit_evaluator", user_id=user_id) as client:
        report_res = client.post(f"/evaluator/submissions/{submission_id}/reports")
        excel_res = client.post(f"/evaluator/submissions/{submission_id}/export-excel")

    assert report_res.status_code == 409
    assert report_res.json().get("detail") == "review_incomplete"
    assert excel_res.status_code == 409
    assert excel_res.json().get("detail") == "review_incomplete"
