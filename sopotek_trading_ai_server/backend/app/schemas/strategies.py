from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.enums import StrategyStatus


class StrategyCreateRequest(BaseModel):
    name: str
    code: str
    description: str | None = None
    status: StrategyStatus = StrategyStatus.ENABLED
    parameters: dict = Field(default_factory=dict)
    performance: dict = Field(default_factory=dict)
    assigned_symbols: list[str] = Field(default_factory=list)


class StrategyUpdateRequest(BaseModel):
    status: StrategyStatus | None = None
    parameters: dict | None = None
    performance: dict | None = None
    assigned_symbols: list[str] | None = None


class StrategyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    code: str
    description: str | None
    status: StrategyStatus
    parameters: dict
    performance: dict
    assigned_symbols: list[str]
    created_at: datetime
    updated_at: datetime
