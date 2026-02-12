import json
import types
import uuid
from pathlib import Path

import pytest
from fastapi import HTTPException

from app import main
from app.auth_service import AuthUser


@pytest.mark.parametrize(
    "path_template",
    [
        "/jobs/{id}",
        "/jobs/{id}/cleaned",
        "/jobs/{id}/parsed",
        "/jobs/{id}/bounds",
        "/jobs/{id}/diagnostics",
    ],
)
def test_jobs_endpoints_require_auth(client_factory, path_template):
    job_id = uuid.uuid4()
    with client_factory() as client:
        res = client.get(path_template.format(id=job_id))
    assert res.status_code == 401


def test_agent_denied_standalone_job(client_factory, monkeypatch):
    monkeypatch.setattr(main, "_get_job_record_or_404", lambda _jid: types.SimpleNamespace(id=_jid, submission_id=None))

    with client_factory(role="agent") as client:
        res = client.get(f"/jobs/{uuid.uuid4()}")

    assert res.status_code == 403
    assert res.json().get("detail") == "forbidden_standalone_job"


def test_evaluator_allowed_standalone_job(client_factory, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    job_id = uuid.uuid4()
    job_dir = Path(tmp_path, "jobs", str(job_id))
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "status.json").write_text(json.dumps({"status": "done", "step": "completed", "progress": 100}), encoding="utf-8")
    monkeypatch.setattr(main, "_get_job_record_or_404", lambda _jid: types.SimpleNamespace(id=_jid, submission_id=None))

    with client_factory(role="credit_evaluator") as client:
        res = client.get(f"/jobs/{job_id}")

    assert res.status_code == 200
    assert res.json().get("status") == "done"


def test_submission_access_agent_read_only_rule():
    agent_id = uuid.uuid4()
    submission = types.SimpleNamespace(agent_id=agent_id, assigned_evaluator_id=None)
    user = AuthUser(id=agent_id, email="agent@example.com", role="agent")

    main._authorize_submission_access(submission, user, write=False)

    with pytest.raises(HTTPException) as exc:
        main._authorize_submission_access(submission, user, write=True)
    assert exc.value.status_code == 403


def test_submission_access_evaluator_must_be_assigned():
    evaluator_id = uuid.uuid4()
    other_id = uuid.uuid4()
    submission = types.SimpleNamespace(agent_id=uuid.uuid4(), assigned_evaluator_id=evaluator_id)
    allowed = AuthUser(id=evaluator_id, email="eval@example.com", role="credit_evaluator")
    denied = AuthUser(id=other_id, email="other@example.com", role="credit_evaluator")

    main._authorize_submission_access(submission, allowed, write=False)

    with pytest.raises(HTTPException) as exc:
        main._authorize_submission_access(submission, denied, write=False)
    assert exc.value.status_code == 403
