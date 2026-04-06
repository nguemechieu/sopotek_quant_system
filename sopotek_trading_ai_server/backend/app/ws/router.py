from __future__ import annotations

import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from app.core.security import decode_access_token
from app.models.user import User


router = APIRouter()


def _jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


async def _authenticate_websocket(websocket: WebSocket):
    token = str(websocket.query_params.get("token") or "").strip()
    if not token:
        await websocket.close(code=4401)
        return None
    try:
        payload = decode_access_token(token, websocket.app.state.settings)
    except HTTPException:
        await websocket.close(code=4401)
        return None
    subject = str(payload.get("sub") or "").strip()
    if not subject:
        await websocket.close(code=4401)
        return None
    session_factory = websocket.app.state.session_factory
    async with session_factory() as session:
        user = await session.scalar(select(User).where(User.id == subject))
    if user is None or not user.is_active:
        await websocket.close(code=4401)
        return None
    return user


async def _stream_channel(websocket: WebSocket, *, channel: str, initial_payload, symbol_filter: set[str] | None = None) -> None:
    await websocket.accept()
    await websocket.send_json({"channel": channel, "type": "snapshot", "data": _jsonable(initial_payload)})
    store = websocket.app.state.platform_state
    queue = await store.subscribe(channel)
    try:
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=25.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"channel": channel, "type": "heartbeat"})
                continue
            if symbol_filter:
                symbol = str(((message or {}).get("data") or {}).get("symbol") or "").upper()
                if symbol and symbol not in symbol_filter:
                    continue
            await websocket.send_json(_jsonable(message))
    except WebSocketDisconnect:
        return
    finally:
        await store.unsubscribe(channel, queue)


@router.websocket("/ws/market")
async def market_stream(websocket: WebSocket) -> None:
    user = await _authenticate_websocket(websocket)
    if user is None:
        return
    raw_symbols = str(websocket.query_params.get("symbols") or "").strip()
    symbol_filter = {item.strip().upper() for item in raw_symbols.split(",") if item.strip()} or None
    initial = await websocket.app.state.platform_state.get_market_snapshot(list(symbol_filter) if symbol_filter else None)
    await _stream_channel(websocket, channel="market", initial_payload=initial, symbol_filter=symbol_filter)


@router.websocket("/ws/portfolio")
async def portfolio_stream(websocket: WebSocket) -> None:
    user = await _authenticate_websocket(websocket)
    if user is None:
        return
    store = websocket.app.state.platform_state
    initial = {
        "portfolio": await store.get_portfolio_snapshot(user.id),
        "positions": await store.get_positions_snapshot(user.id),
        "risk": await store.get_risk_snapshot(user.id),
        "control": await store.get_control_state(user.id),
    }
    await _stream_channel(websocket, channel="portfolio", initial_payload=initial)


@router.websocket("/ws/executions")
async def execution_stream(websocket: WebSocket) -> None:
    user = await _authenticate_websocket(websocket)
    if user is None:
        return
    store = websocket.app.state.platform_state
    initial = {
        "orders": await store.get_orders_snapshot(user.id),
        "alerts": await store.get_alerts(user.id),
    }
    await _stream_channel(websocket, channel="executions", initial_payload=initial)
