import types
import uuid

import pytest

from app import main
from app.auth_service import AuthUser
from app import workflow_service


class _FakeSession:
    def __init__(self, submission, submission_pages=None, transaction_pages=None):
        self.submission = submission
        self.submission_pages = list(submission_pages or [])
        self.transaction_pages = list(transaction_pages or [])

    def scalar(self, stmt):
        entity = (stmt.column_descriptions or [{}])[0].get("entity")
        if entity is workflow_service.Submission:
            return self.submission
        return None

    def scalars(self, stmt):
        entity = (stmt.column_descriptions or [{}])[0].get("entity")
        if entity is workflow_service.SubmissionPage:
            return list(self.submission_pages)
        if entity is workflow_service.Transaction:
            return list(self.transaction_pages)
        return []

    def close(self):
        return None


def test_persist_evaluator_transactions_maps_legacy_missing_page_to_single_submission_page(monkeypatch):
    submission_id = uuid.uuid4()
    evaluator_id = uuid.uuid4()
    user = AuthUser(id=evaluator_id, email="eval@example.com", role="credit_evaluator")
    submission = types.SimpleNamespace(id=submission_id, assigned_evaluator_id=evaluator_id, status="for_review")
    session = _FakeSession(submission, submission_pages=[types.SimpleNamespace(page_key="page_001")])
    monkeypatch.setattr(workflow_service, "SessionLocal", lambda: session)

    calls = []

    def _fake_persist_page_transactions(sub_id, key, eval_user, rows):
        calls.append((sub_id, key, eval_user, rows))
        return {"summary": {"total_transactions": len(rows)}}

    monkeypatch.setattr(workflow_service, "persist_page_transactions", _fake_persist_page_transactions)

    summary = workflow_service.persist_evaluator_transactions(
        submission_id=submission_id,
        evaluator_user=user,
        rows=[{"date": "2026-02-01", "description": "legacy row"}],
    )

    assert summary == {"total_transactions": 1}
    assert len(calls) == 1
    assert calls[0][1] == "page_001"
    assert calls[0][3][0]["page"] == "page_001"


def test_persist_evaluator_transactions_rejects_legacy_missing_page_for_multi_page_submission(monkeypatch):
    submission_id = uuid.uuid4()
    evaluator_id = uuid.uuid4()
    user = AuthUser(id=evaluator_id, email="eval@example.com", role="credit_evaluator")
    submission = types.SimpleNamespace(id=submission_id, assigned_evaluator_id=evaluator_id, status="for_review")
    session = _FakeSession(
        submission,
        submission_pages=[
            types.SimpleNamespace(page_key="page_001"),
            types.SimpleNamespace(page_key="page_002"),
        ],
    )
    monkeypatch.setattr(workflow_service, "SessionLocal", lambda: session)
    monkeypatch.setattr(workflow_service, "persist_page_transactions", lambda *_args, **_kwargs: pytest.fail("unexpected call"))

    with pytest.raises(ValueError, match="missing_page_for_multi_page_payload"):
        workflow_service.persist_evaluator_transactions(
            submission_id=submission_id,
            evaluator_user=user,
            rows=[{"date": "2026-02-01", "description": "legacy row"}],
        )


def test_evaluator_transactions_endpoint_returns_400_for_ambiguous_missing_page(client_factory, monkeypatch):
    submission_id = uuid.uuid4()
    monkeypatch.setattr(
        main,
        "persist_evaluator_transactions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("missing_page_for_multi_page_payload")),
    )

    with client_factory(role="credit_evaluator") as client:
        res = client.patch(
            f"/evaluator/submissions/{submission_id}/transactions",
            json={"rows": [{"date": "2026-02-01", "description": "legacy row"}]},
        )

    assert res.status_code == 400
    assert res.json().get("detail") == "missing_page_for_multi_page_payload"
