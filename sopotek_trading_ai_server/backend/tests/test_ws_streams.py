from __future__ import annotations

import asyncio


def _register_user(client):
    response = client.post(
        "/auth/register",
        json={
            "email": "stream@sopotek.ai",
            "username": "streamdesk",
            "password": "SuperSecure123",
            "full_name": "Stream Desk",
            "role": "trader",
        },
    )
    assert response.status_code == 201
    return response.json()["access_token"]


def test_market_websocket_streams_live_events(client) -> None:
    token = _register_user(client)
    with client.websocket_connect(f"/ws/market?token={token}&symbols=EUR_USD") as websocket:
        snapshot = websocket.receive_json()
        assert snapshot["type"] == "snapshot"

        asyncio.run(
            client.app.state.platform_state.publish_market(
                "EUR_USD",
                {
                    "last": 1.1024,
                    "bid": 1.1023,
                    "ask": 1.1025,
                    "volume": 1200000,
                    "candles": [{"time": "2026-04-06T13:00:00Z", "open": 1.1, "high": 1.103, "low": 1.099, "close": 1.1024}],
                },
            )
        )

        event = websocket.receive_json()
        assert event["channel"] == "market"
        assert event["data"]["symbol"] == "EUR_USD"
        assert event["data"]["last"] == 1.1024
