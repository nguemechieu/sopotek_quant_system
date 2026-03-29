from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class Candle:
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    start: datetime = field(default_factory=utcnow)
    end: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class OrderBookSnapshot:
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class Signal:
    symbol: str
    side: str
    quantity: float
    price: float
    confidence: float = 0.0
    strategy_name: str = "unknown"
    reason: str = ""
    stop_price: float | None = None
    take_profit: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class TradeReview:
    approved: bool
    symbol: str
    side: str
    quantity: float
    price: float
    reason: str
    risk_score: float = 0.0
    stop_price: float | None = None
    take_profit: float | None = None
    strategy_name: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class OrderIntent:
    symbol: str
    side: str
    quantity: float
    price: float | None = None
    order_type: str = "market"
    stop_price: float | None = None
    take_profit: float | None = None
    strategy_name: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionReport:
    order_id: str
    symbol: str
    side: str
    quantity: float
    requested_price: float | None
    fill_price: float | None
    status: str
    latency_ms: float
    slippage_bps: float = 0.0
    strategy_name: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class Position:
    symbol: str
    quantity: float = 0.0
    average_price: float = 0.0
    last_price: float = 0.0
    realized_pnl: float = 0.0

    @property
    def market_value(self) -> float:
        return self.quantity * self.last_price

    @property
    def unrealized_pnl(self) -> float:
        if self.quantity == 0:
            return 0.0
        return (self.last_price - self.average_price) * self.quantity


@dataclass(slots=True)
class PortfolioSnapshot:
    cash: float
    equity: float
    positions: dict[str, Position] = field(default_factory=dict)
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    drawdown_pct: float = 0.0
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class AnalystInsight:
    symbol: str
    regime: str
    momentum: float = 0.0
    volatility: float = 1.0
    preferred_strategy: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utcnow)
