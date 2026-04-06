from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PlatformStateStore:
    """In-memory realtime state used by REST and WebSocket surfaces."""

    def __init__(self) -> None:
        self._market: dict[str, dict[str, Any]] = {}
        self._portfolio: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._orders: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._strategies: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._risk: dict[str, dict[str, Any]] = {}
        self._control: dict[str, dict[str, Any]] = defaultdict(lambda: {"trading_enabled": False, "selected_symbols": []})
        self._alerts: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=100))
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, channel: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=256)
        async with self._lock:
            self._subscribers[channel].add(queue)
        return queue

    async def unsubscribe(self, channel: str, queue: asyncio.Queue) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(channel)
            if subscribers is not None:
                subscribers.discard(queue)

    async def publish_market(self, symbol: str, payload: dict[str, Any]) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        market_payload = {
            "symbol": normalized_symbol,
            "last": float(payload.get("last", payload.get("price", 0.0)) or 0.0),
            "change_pct": float(payload.get("change_pct", 0.0) or 0.0),
            "bid": payload.get("bid"),
            "ask": payload.get("ask"),
            "volume": float(payload.get("volume", 0.0) or 0.0),
            "candle_timeframe": str(payload.get("candle_timeframe", "1m") or "1m"),
            "candles": list(payload.get("candles") or []),
            "order_book": deepcopy(payload.get("order_book") or {"bids": [], "asks": []}),
            "updated_at": payload.get("updated_at") or utc_now(),
        }
        async with self._lock:
            self._market[normalized_symbol] = market_payload
        await self._broadcast("market", market_payload)

    async def publish_portfolio(self, user_id: str, payload: dict[str, Any]) -> None:
        normalized_user_id = str(user_id)
        portfolio_payload = deepcopy(payload)
        portfolio_payload["updated_at"] = payload.get("updated_at") or utc_now()
        async with self._lock:
            self._portfolio[normalized_user_id] = portfolio_payload
        await self._broadcast("portfolio", {"user_id": normalized_user_id, **portfolio_payload})

    async def publish_positions(self, user_id: str, positions: list[dict[str, Any]]) -> None:
        normalized_user_id = str(user_id)
        normalized_positions = {
            str(position.get("symbol") or "").upper(): deepcopy(position) for position in list(positions or [])
        }
        async with self._lock:
            self._positions[normalized_user_id] = normalized_positions
        await self._broadcast("portfolio", {"user_id": normalized_user_id, "positions": list(normalized_positions.values())})

    async def publish_order(self, user_id: str, payload: dict[str, Any]) -> None:
        normalized_user_id = str(user_id)
        order_id = str(payload.get("order_id") or payload.get("id") or "")
        if not order_id:
            return
        async with self._lock:
            self._orders[normalized_user_id][order_id] = deepcopy(payload)
        await self._broadcast("executions", {"user_id": normalized_user_id, **payload})

    async def publish_strategy(self, user_id: str, payload: dict[str, Any]) -> None:
        normalized_user_id = str(user_id)
        strategy_id = str(payload.get("id") or payload.get("strategy_id") or "")
        if not strategy_id:
            return
        async with self._lock:
            self._strategies[normalized_user_id][strategy_id] = deepcopy(payload)
        await self._broadcast("portfolio", {"user_id": normalized_user_id, "strategy": deepcopy(payload)})

    async def publish_risk(self, user_id: str, payload: dict[str, Any]) -> None:
        normalized_user_id = str(user_id)
        risk_payload = deepcopy(payload)
        risk_payload["updated_at"] = payload.get("updated_at") or utc_now()
        async with self._lock:
            self._risk[normalized_user_id] = risk_payload
        await self._broadcast("portfolio", {"user_id": normalized_user_id, "risk": deepcopy(risk_payload)})

    async def publish_alert(self, user_id: str, payload: dict[str, Any]) -> None:
        normalized_user_id = str(user_id)
        alert_payload = deepcopy(payload)
        alert_payload["created_at"] = payload.get("created_at") or utc_now()
        async with self._lock:
            self._alerts[normalized_user_id].appendleft(alert_payload)
        await self._broadcast("executions", {"user_id": normalized_user_id, "alert": deepcopy(alert_payload)})

    async def set_control_state(
        self,
        user_id: str,
        *,
        trading_enabled: bool | None = None,
        selected_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_user_id = str(user_id)
        async with self._lock:
            current = dict(self._control[normalized_user_id])
            if trading_enabled is not None:
                current["trading_enabled"] = bool(trading_enabled)
            if selected_symbols is not None:
                current["selected_symbols"] = [str(symbol).upper() for symbol in selected_symbols]
            self._control[normalized_user_id] = current
        await self._broadcast("portfolio", {"user_id": normalized_user_id, "control": deepcopy(current)})
        return current

    async def apply_kafka_event(self, topic: str, payload: dict[str, Any]) -> None:
        normalized_topic = str(topic or "").strip().lower()
        if "market" in normalized_topic:
            await self.publish_market(str(payload.get("symbol") or payload.get("instrument") or ""), payload)
            return
        user_id = str(payload.get("user_id") or payload.get("account_id") or "shared")
        if "portfolio" in normalized_topic:
            await self.publish_portfolio(user_id, payload)
        elif "execution" in normalized_topic:
            await self.publish_order(user_id, payload)
        elif "risk" in normalized_topic:
            if payload.get("message"):
                await self.publish_alert(
                    user_id,
                    {
                        "category": payload.get("category", "risk"),
                        "severity": payload.get("severity", "warning"),
                        "message": payload.get("message"),
                        "payload": deepcopy(payload),
                    },
                )
            await self.publish_risk(user_id, payload)
        elif "strategy" in normalized_topic:
            await self.publish_strategy(user_id, payload)

    async def get_market_snapshot(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        async with self._lock:
            if not symbols:
                return deepcopy(list(self._market.values()))
            return [
                deepcopy(self._market[symbol])
                for symbol in [str(item).upper() for item in symbols]
                if symbol in self._market
            ]

    async def get_portfolio_snapshot(self, user_id: str) -> dict[str, Any]:
        async with self._lock:
            return deepcopy(self._portfolio.get(str(user_id), {}))

    async def get_positions_snapshot(self, user_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            return deepcopy(list(self._positions.get(str(user_id), {}).values()))

    async def get_orders_snapshot(self, user_id: str, *, status_filter: str | None = None) -> list[dict[str, Any]]:
        async with self._lock:
            rows = list(self._orders.get(str(user_id), {}).values())
        if status_filter:
            normalized = str(status_filter).strip().lower()
            rows = [row for row in rows if str(row.get("status") or "").strip().lower() == normalized]
        rows.sort(key=lambda row: str(row.get("updated_at") or row.get("created_at") or ""), reverse=True)
        return deepcopy(rows)

    async def get_strategies_snapshot(self, user_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            return deepcopy(list(self._strategies.get(str(user_id), {}).values()))

    async def get_risk_snapshot(self, user_id: str) -> dict[str, Any]:
        async with self._lock:
            return deepcopy(self._risk.get(str(user_id), {}))

    async def get_control_state(self, user_id: str) -> dict[str, Any]:
        async with self._lock:
            return deepcopy(self._control.get(str(user_id), {"trading_enabled": False, "selected_symbols": []}))

    async def get_alerts(self, user_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            return deepcopy(list(self._alerts.get(str(user_id), deque())))

    async def _broadcast(self, channel: str, payload: dict[str, Any]) -> None:
        message = {"channel": channel, "timestamp": utc_now().isoformat(), "data": deepcopy(payload)}
        async with self._lock:
            subscribers = list(self._subscribers.get(channel, set()))
        stale: list[asyncio.Queue] = []
        for queue in subscribers:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            await self.unsubscribe(channel, queue)
