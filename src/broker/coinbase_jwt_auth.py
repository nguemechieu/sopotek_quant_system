import secrets
import time
from typing import Optional
from urllib.parse import urlparse

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey


# =========================
# 🔍 AUTH TYPE DETECTION
# =========================
def uses_coinbase_jwt_auth(api_key: str, api_secret: str) -> bool:
    key_text = str(api_key or "").strip()
    secret_text = str(api_secret or "").strip()

    return bool(
        key_text
        and "BEGIN" in secret_text
        and "PRIVATE KEY" in secret_text
    )


# =========================
# 🌐 HOST RESOLUTION
# =========================
def resolve_coinbase_rest_host(rest_url: str) -> str:
    parsed = urlparse(str(rest_url or "").strip())
    return parsed.netloc or "api.coinbase.com"


# =========================
# 🔐 JWT BUILDER
# =========================
def build_coinbase_rest_jwt(
    request_method: str,
    request_host: str,
    request_path: str,
    api_key: str,
    api_secret: str,
    ttl_seconds: int = 120,
) -> str:

    method = str(request_method or "GET").strip().upper()
    host = str(request_host or "api.coinbase.com").strip()
    path = str(request_path or "/").strip()

    if not path.startswith("/"):
        path = f"/{path}"

    key_name = str(api_key or "").strip()
    secret_text = str(api_secret or "").strip()

    if not key_name:
        raise ValueError("Coinbase API key is missing")

    if not secret_text:
        raise ValueError("Coinbase API secret is missing")

    # =========================
    # 🔒 LOAD PRIVATE KEY
    # =========================
    try:
        private_key = serialization.load_pem_private_key(
            secret_text.encode("utf-8"),
            password=None,
        )
    except Exception as e:
        raise ValueError(f"Invalid Coinbase private key format: {e}")

    # =========================
    # 🔐 TYPE SAFETY (FIXES YOUR ERROR)
    # =========================
    if not isinstance(private_key, EllipticCurvePrivateKey):
        raise ValueError(
            "Invalid Coinbase key: expected Elliptic Curve (ES256) private key"
        )

    now = int(time.time())

    # ⚠️ MUST match request exactly
    uri = f"{method} {host}{path}"

    payload = {
        "sub": key_name,
        "iss": "cdp",
        "nbf": now,
        "exp": now + int(ttl_seconds or 120),
        "uri": uri,
        "aud": "cdp_service",  # ✅ important for Coinbase
    }

    headers = {
        "kid": key_name,
        "nonce": secrets.token_hex(),
    }

    token = jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers=headers,
    )

    return token if isinstance(token, str) else token.decode("utf-8")


# =========================
# 🔒 MASK KEY FOR LOGGING
# =========================
def masked_coinbase_key_id(api_key: Optional[str]) -> Optional[str]:
    text = str(api_key or "").strip()

    if not text:
        return None

    if len(text) <= 14:
        return text

    return f"{text[:8]}...{text[-6:]}"