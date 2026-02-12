import json
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main
from app.auth_service import AuthUser, get_current_user


@pytest.fixture
def app_with_temp_data(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path))
    Path(tmp_path, "jobs").mkdir(parents=True, exist_ok=True)

    original_startup = list(main.app.router.on_startup)
    main.app.router.on_startup.clear()

    yield main.app, tmp_path

    main.app.dependency_overrides.clear()
    main.app.router.on_startup[:] = original_startup


@pytest.fixture
def client_factory(app_with_temp_data):
    app, _tmp_path = app_with_temp_data

    @contextmanager
    def _factory(role: str | None = None, user_id: uuid.UUID | None = None):
        if role:
            uid = user_id or uuid.uuid4()

            def _fake_user():
                return AuthUser(id=uid, email=f"{role}@example.com", role=role)

            app.dependency_overrides[get_current_user] = _fake_user
        else:
            app.dependency_overrides.pop(get_current_user, None)

        with TestClient(app) as client:
            yield client

        app.dependency_overrides.clear()

    return _factory


def write_job_status(tmp_path, job_id: uuid.UUID, status: dict | None = None):
    payload = status or {"status": "done", "step": "completed", "progress": 100}
    job_dir = Path(tmp_path, "jobs", str(job_id))
    job_dir.mkdir(parents=True, exist_ok=True)
    status_path = job_dir / "status.json"
    status_path.write_text(json.dumps(payload), encoding="utf-8")
    return status_path
