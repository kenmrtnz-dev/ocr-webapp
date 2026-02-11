import os
from pathlib import Path


DATA_DIR = os.getenv("DATA_DIR", "./data")


def blob_abs_path(blob_key: str) -> str:
    key = str(blob_key or "").lstrip("/")
    return os.path.join(DATA_DIR, key)


def ensure_blob_parent(blob_key: str):
    path = Path(blob_abs_path(blob_key))
    path.parent.mkdir(parents=True, exist_ok=True)


def write_blob(blob_key: str, data: bytes) -> str:
    ensure_blob_parent(blob_key)
    abs_path = blob_abs_path(blob_key)
    with open(abs_path, "wb") as f:
        f.write(data)
    return abs_path
