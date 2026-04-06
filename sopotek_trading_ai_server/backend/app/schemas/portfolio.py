from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class PositionSnapshot(BaseModel):
    symbol: str
    quantity: float
    side: str
    avg_price: float
    mark_price: float | None = None
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    market_value: float = 0.0
    notional_exposure: float = 0.0
    asset_class: str = "unknown"


class PortfolioHistoryPoint(BaseModel):
    timestamp: datetime
    total_equity: float


class PortfolioResponse(BaseModel):
    account_id: str
    broker: str
    total_equity: float
    cash: float
    buying_power: float
    daily_pnl: float
    weekly_pnl: float
    monthly_pnl: float
    gross_exposure: float
    net_exposure: float
    max_drawdown: float
    var_95: float
    margin_usage: float
    active_positions: int
    selected_symbols: list[str] = Field(default_factory=list)
    risk_limits: dict = Field(default_factory=dict)
    positions: list[PositionSnapshot] = Field(default_factory=list)
    history: list[PortfolioHistoryPoint] = Field(default_factory=list)
