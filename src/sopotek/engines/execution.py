from __future__ import annotations

import asyncio
import time
from enum import Enum
from uuid import uuid4

from sopotek.broker.base import BaseBroker
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import ExecutionReport, OrderIntent, TradeReview


class OrderState(str, Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    FILLED = "filled"
    FAILED = "failed"


class ExecutionEngine:
    def __init__(
        self,
        broker: BaseBroker,
        event_bus: AsyncEventBus,
        *,
        max_retries: int = 2,
    ) -> None:
        self.broker = broker
        self.bus = event_bus
        self.max_retries = max(1, int(max_retries))
        self.order_states: dict[str, OrderState] = {}
        self.bus.subscribe(EventType.RISK_APPROVED, self._on_risk_approved)

    async def _on_risk_approved(self, event) -> None:
        review = getattr(event, "data", None)
        if review is None:
            return
        if not isinstance(review, TradeReview):
            review = TradeReview(**dict(review))
        report = await self.execute(review)
        await self.bus.publish(EventType.ORDER_FILLED, report, priority=80, source="execution_engine")
        await self.bus.publish(EventType.EXECUTION_REPORT, report, priority=85, source="execution_engine")

    async def execute(self, review: TradeReview) -> ExecutionReport:
        order = OrderIntent(
            symbol=review.symbol,
            side=review.side,
            quantity=review.quantity,
            price=review.price,
            order_type="market",
            stop_price=review.stop_price,
            take_profit=review.take_profit,
            strategy_name=review.strategy_name,
            metadata=dict(review.metadata),
        )
        order_id = str(review.metadata.get("order_id") or uuid4().hex)
        self.order_states[order_id] = OrderState.NEW

        last_error = None
        start = time.perf_counter()
        for attempt in range(1, self.max_retries + 1):
            try:
                self.order_states[order_id] = OrderState.SUBMITTED
                await self.bus.publish(
                    EventType.ORDER_SUBMITTED,
                    {"order_id": order_id, "symbol": order.symbol, "attempt": attempt},
                    priority=75,
                    source="execution_engine",
                )
                raw = await self.broker.place_order(order)
                latency_ms = (time.perf_counter() - start) * 1000.0
                fill_price = self._extract_fill_price(raw, fallback=order.price)
                report = ExecutionReport(
                    order_id=str((raw or {}).get("id") or order_id),
                    symbol=order.symbol,
                    side=order.side,
                    quantity=float(order.quantity),
                    requested_price=order.price,
                    fill_price=fill_price,
                    status=str((raw or {}).get("status") or "filled"),
                    latency_ms=latency_ms,
                    slippage_bps=self._slippage_bps(order.price, fill_price, order.side),
                    strategy_name=order.strategy_name,
                    metadata={"attempt": attempt, "raw": raw or {}},
                )
                self.order_states[order_id] = OrderState.FILLED
                return report
            except Exception as exc:
                last_error = exc
                self.order_states[order_id] = OrderState.FAILED
                if attempt >= self.max_retries:
                    break
                await asyncio.sleep(0)

        latency_ms = (time.perf_counter() - start) * 1000.0
        return ExecutionReport(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=float(order.quantity),
            requested_price=order.price,
            fill_price=None,
            status="failed",
            latency_ms=latency_ms,
            strategy_name=order.strategy_name,
            metadata={"error": str(last_error) if last_error is not None else "Unknown execution error"},
        )

    def _extract_fill_price(self, payload, *, fallback):
        if not isinstance(payload, dict):
            return fallback
        for key in ("fill_price", "average", "price", "avgPrice", "last"):
            value = payload.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                continue
        return fallback

    def _slippage_bps(self, requested_price, fill_price, side: str) -> float:
        try:
            requested = float(requested_price)
            filled = float(fill_price)
        except Exception:
            return 0.0
        if requested <= 0:
            return 0.0
        raw_bps = ((filled - requested) / requested) * 10000.0
        return raw_bps if str(side).lower() == "buy" else -raw_bps
