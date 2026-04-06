from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.enums import OrderStatus


class OrderCreateRequest(BaseModel):
    symbol: str
    side: str
    quantity: float = Field(gt=0)
    order_type: str = "market"
    limit_price: float | None = None
    strategy_id: str | None = None
    venue: str | None = None
    reason: str | None = None
    metadata: dict = Field(default_factory=dict)


class TradeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    order_id: str
    symbol: str
    side: str
    order_type: str
    status: OrderStatus
    quantity: float
    requested_price: float | None
    average_price: float | None
    filled_quantity: float
    pnl: float
    venue: str | None
    reason: str | None
    details: dict
    created_at: datetime
    updated_at: datetime
