import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.workflow_models import User


JWT_SECRET = os.getenv("JWT_SECRET", "dev-change-me")
JWT_ISS = os.getenv("JWT_ISS", "ocr-webapp")
JWT_EXP_SECONDS = int(os.getenv("JWT_EXP_SECONDS", "43200"))

DEFAULT_AGENT_EMAIL = os.getenv("DEFAULT_AGENT_EMAIL", "agent@example.com")
DEFAULT_AGENT_PASSWORD = os.getenv("DEFAULT_AGENT_PASSWORD", "agent123")
DEFAULT_EVALUATOR_EMAIL = os.getenv("DEFAULT_EVALUATOR_EMAIL", "evaluator@example.com")
DEFAULT_EVALUATOR_PASSWORD = os.getenv("DEFAULT_EVALUATOR_PASSWORD", "evaluator123")
DEFAULT_ADMIN_EMAIL = os.getenv("DEFAULT_ADMIN_EMAIL", "admin@example.com")
DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD", "admin123")

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class AuthUser:
    id: uuid.UUID
    email: str
    role: str


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + pad).encode("ascii"))


def hash_password(password: str, salt: Optional[str] = None) -> str:
    salt = salt or secrets.token_hex(16)
    rounds = 120_000
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), rounds)
    return f"pbkdf2_sha256${rounds}${salt}${dk.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        alg, rounds_txt, salt, digest = encoded.split("$", 3)
        if alg != "pbkdf2_sha256":
            return False
        rounds = int(rounds_txt)
    except Exception:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), rounds)
    return hmac.compare_digest(dk.hex(), digest)


def issue_token(user: User) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": JWT_ISS,
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "iat": now,
        "exp": now + JWT_EXP_SECONDS,
    }
    segments = [
        _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")),
        _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")),
    ]
    signing_input = ".".join(segments).encode("ascii")
    sig = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    segments.append(_b64url_encode(sig))
    return ".".join(segments)


def decode_token(token: str) -> dict:
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        raise HTTPException(status_code=401, detail="invalid_token")
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    expected_sig = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    got_sig = _b64url_decode(sig_b64)
    if not hmac.compare_digest(expected_sig, got_sig):
        raise HTTPException(status_code=401, detail="invalid_token_signature")
    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=401, detail="invalid_token_payload")
    if payload.get("iss") != JWT_ISS:
        raise HTTPException(status_code=401, detail="invalid_token_issuer")
    exp = int(payload.get("exp") or 0)
    if exp < int(time.time()):
        raise HTTPException(status_code=401, detail="token_expired")
    return payload


def _load_user(db: Session, user_id: str) -> Optional[User]:
    try:
        uid = uuid.UUID(str(user_id))
    except Exception:
        return None
    return db.scalar(select(User).where(User.id == uid, User.is_active.is_(True)))


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> AuthUser:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="missing_auth_token")
    payload = decode_token(credentials.credentials)
    db = SessionLocal()
    try:
        user = _load_user(db, payload.get("sub"))
        if not user:
            raise HTTPException(status_code=401, detail="user_not_found")
        return AuthUser(id=user.id, email=user.email, role=user.role)
    finally:
        db.close()


def require_role(*roles: str):
    allowed = {r.strip() for r in roles if r.strip()}

    def _guard(user: AuthUser = Depends(get_current_user)) -> AuthUser:
        if user.role not in allowed:
            raise HTTPException(status_code=403, detail="forbidden_role")
        return user

    return _guard


def ensure_default_users():
    db = SessionLocal()
    try:
        for email, password, role in [
            (DEFAULT_AGENT_EMAIL, DEFAULT_AGENT_PASSWORD, "agent"),
            (DEFAULT_EVALUATOR_EMAIL, DEFAULT_EVALUATOR_PASSWORD, "credit_evaluator"),
            (DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD, "admin"),
        ]:
            existing = db.scalar(select(User).where(User.email == email))
            if existing:
                continue
            db.add(
                User(
                    email=email,
                    password_hash=hash_password(password),
                    role=role,
                    is_active=True,
                )
            )
        db.commit()
    finally:
        db.close()
