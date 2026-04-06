from __future__ import annotations


def test_register_and_login_flow(client) -> None:
    response = client.post(
        "/auth/register",
        json={
            "email": "trader@sopotek.ai",
            "username": "fundtrader",
            "password": "SuperSecure123",
            "full_name": "Fund Trader",
            "role": "trader",
        },
    )
    assert response.status_code == 201
    register_payload = response.json()
    assert register_payload["access_token"]
    assert register_payload["user"]["role"] == "trader"

    login_response = client.post(
        "/auth/login",
        json={"email": "trader@sopotek.ai", "password": "SuperSecure123"},
    )
    assert login_response.status_code == 200
    login_payload = login_response.json()
    assert login_payload["user"]["email"] == "trader@sopotek.ai"


def test_forgot_and_reset_password_flow(client) -> None:
    register_response = client.post(
        "/auth/register",
        json={
            "email": "reset@sopotek.ai",
            "username": "resetdesk",
            "password": "SuperSecure123",
            "full_name": "Reset Desk",
            "role": "trader",
        },
    )
    assert register_response.status_code == 201

    forgot_response = client.post("/auth/forgot-password", json={"email": "reset@sopotek.ai"})
    assert forgot_response.status_code == 200
    forgot_payload = forgot_response.json()
    assert forgot_payload["message"]
    assert forgot_payload["reset_token"]
    assert forgot_payload["reset_url"]

    reset_response = client.post(
        "/auth/reset-password",
        json={"token": forgot_payload["reset_token"], "password": "NewSecure456"},
    )
    assert reset_response.status_code == 200
    reset_payload = reset_response.json()
    assert reset_payload["access_token"]
    assert reset_payload["user"]["email"] == "reset@sopotek.ai"

    login_response = client.post(
        "/auth/login",
        json={"email": "reset@sopotek.ai", "password": "NewSecure456"},
    )
    assert login_response.status_code == 200
