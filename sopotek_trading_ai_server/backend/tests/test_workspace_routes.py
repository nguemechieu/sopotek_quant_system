from __future__ import annotations


def _register_user(client):
    response = client.post(
        "/auth/register",
        json={
            "email": "config@sopotek.ai",
            "username": "configdesk",
            "password": "SuperSecure123",
            "full_name": "Config Desk",
            "role": "trader",
        },
    )
    assert response.status_code == 201
    return response.json()["access_token"]


def test_workspace_settings_require_auth(client) -> None:
    response = client.get("/workspace/settings")
    assert response.status_code == 401


def test_workspace_settings_default_and_persist(client) -> None:
    token = _register_user(client)
    headers = {"Authorization": f"Bearer {token}"}

    initial_response = client.get("/workspace/settings", headers=headers)
    assert initial_response.status_code == 200
    initial_payload = initial_response.json()
    assert initial_payload["broker_type"] == "paper"
    assert initial_payload["exchange"] == "paper"
    assert initial_payload["mode"] == "paper"
    assert initial_payload["risk_percent"] == 2

    update_response = client.put(
        "/workspace/settings",
        headers=headers,
        json={
            "language": "en",
            "broker_type": "crypto",
            "exchange": "coinbase",
            "customer_region": "us",
            "mode": "live",
            "market_type": "spot",
            "ibkr_connection_mode": "webapi",
            "ibkr_environment": "gateway",
            "schwab_environment": "sandbox",
            "api_key": "coinbase-key",
            "secret": "coinbase-secret",
            "password": "",
            "account_id": "desk-001",
            "risk_percent": 3,
            "remember_profile": True,
            "solana": {
                "wallet_address": "",
                "private_key": "",
                "rpc_url": "",
                "jupiter_api_key": "",
                "okx_api_key": "",
                "okx_secret": "",
                "okx_passphrase": "",
                "okx_project_id": "",
            },
        },
    )
    assert update_response.status_code == 200
    update_payload = update_response.json()
    assert update_payload["exchange"] == "coinbase"
    assert update_payload["mode"] == "live"
    assert update_payload["account_id"] == "desk-001"
    assert update_payload["risk_percent"] == 3

    persisted_response = client.get("/workspace/settings", headers=headers)
    assert persisted_response.status_code == 200
    persisted_payload = persisted_response.json()
    assert persisted_payload["exchange"] == "coinbase"
    assert persisted_payload["broker_type"] == "crypto"
    assert persisted_payload["account_id"] == "desk-001"

    portfolio_response = client.get("/portfolio", headers=headers)
    assert portfolio_response.status_code == 200
    portfolio_payload = portfolio_response.json()
    assert portfolio_payload["broker"] == "coinbase"
    assert portfolio_payload["account_id"] == "desk-001"
    assert portfolio_payload["risk_limits"]["risk_percent"] == 3
