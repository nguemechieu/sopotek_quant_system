import secrets
import time
from typing import Optional
from urllib.parse import urlparse

import jwt
from cryptography.hazmat.primitives import serialization


def uses_coinbase_jwt_auth(api_key, api_secret) -> bool:
    key_text = str(api_key or "").strip()
    secret_text = str(api_secret or "").strip()
    return bool(key_text and "BEGIN" in secret_text and "PRIVATE KEY" in secret_text)


def resolve_coinbase_rest_host(rest_url: str) -> str:
    parsed = urlparse(str(rest_url or "").strip())
    return parsed.netloc or "api.coinbase.com"


def build_coinbase_rest_jwt(
    request_method: str,
    request_host: str,
    request_path: str,
    api_key: str,
    api_secret: str,
    ttl_seconds: int = 120,
) -> str:
    method = str(request_method or "GET").strip().upper() or "GET"
    host = str(request_host or "api.coinbase.com").strip() or "api.coinbase.com"
    path = str(request_path or "/").strip() or "/"
    key_name = str(api_key or "").strip()
    secret_text = str(api_secret or "").strip()

    if not path.startswith("/"):
        path = f"/{path}"

    private_key = serialization.load_pem_private_key(secret_text.encode("utf-8"), password=None)
    now = int(time.time())
    uri = f"{method} {host}{path}"
    token = jwt.encode(
        {
            "sub": key_name,
            "iss": "cdp",
            "nbf": now,
            "exp": now + int(ttl_seconds or 120),
            "uri": uri,
        },
        private_key,
        algorithm="ES256",
        headers={
            "kid": key_name,
            "nonce": secrets.token_hex(),
        },
    )
    return token if isinstance(token, str) else token.decode("utf-8")


def masked_coinbase_key_id(api_key: Optional[str]) -> Optional[str]:
    text = str(api_key or "").strip()
    if not text:
        return None
    if len(text) <= 14:
        return text
    return f"{text[:8]}...{text[-6:]}"
