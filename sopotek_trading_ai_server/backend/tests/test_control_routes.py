from __future__ import annotations


def _register_user(client):
    response = client.post(
        "/auth/register",
        json={
            "email": "ops@sopotek.ai",
            "username": "opsdesk",
            "password": "SuperSecure123",
            "full_name": "Ops Desk",
            "role": "trader",
        },
    )
    assert response.status_code == 201
    return response.json()["access_token"]


def test_start_trading_publishes_command(client) -> None:
    token = _register_user(client)
    response = client.post(
        "/control/trading/start",
        json={"selected_symbols": ["EUR_USD", "XAU_USD"]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["trading_enabled"] is True
    assert payload["selected_symbols"] == ["EUR_USD", "XAU_USD"]

    published = client.app.state.kafka_gateway.published_messages
    assert published
    assert published[-1]["topic"] == client.app.state.settings.kafka_trading_command_topic
    assert published[-1]["payload"]["command"] == "start_trading"


def test_submit_order_persists_trade(client) -> None:
    token = _register_user(client)
    response = client.post(
        "/orders",
        json={
            "symbol": "EUR_USD",
            "side": "buy",
            "quantity": 10000,
            "order_type": "market",
            "venue": "oanda",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202
    payload = response.json()
    assert payload["symbol"] == "EUR_USD"
    assert payload["status"] == "pending"

    orders_response = client.get("/orders", headers={"Authorization": f"Bearer {token}"})
    assert orders_response.status_code == 200
    assert len(orders_response.json()) == 1
