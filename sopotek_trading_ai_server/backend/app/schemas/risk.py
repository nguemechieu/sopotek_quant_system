from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class AlertMessage(BaseModel):
    category: str
    severity: str
    message: str
    created_at: datetime
    payload: dict = Field(default_factory=dict)


class RiskResponse(BaseModel):
    drawdown: float = 0.0
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    exposure_by_asset: dict[str, float] = Field(default_factory=dict)
    risk_limits: dict = Field(default_factory=dict)
    trading_enabled: bool = False
    selected_symbols: list[str] = Field(default_factory=list)
    alerts: list[AlertMessage] = Field(default_factory=list)
    updated_at: datetime


class RiskUpdateRequest(BaseModel):
    risk_limits: dict = Field(default_factory=dict)
    selected_symbols: list[str] | None = None
