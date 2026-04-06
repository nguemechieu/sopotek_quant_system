from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class SolanaWorkspaceSettings(BaseModel):
    wallet_address: str = ""
    private_key: str = ""
    rpc_url: str = ""
    jupiter_api_key: str = ""
    okx_api_key: str = ""
    okx_secret: str = ""
    okx_passphrase: str = ""
    okx_project_id: str = ""


class WorkspaceSettings(BaseModel):
    language: str = "en"
    broker_type: Literal["crypto", "forex", "stocks", "options", "futures", "derivatives", "paper"] = "paper"
    exchange: str = "paper"
    customer_region: Literal["us", "global"] = "us"
    mode: Literal["live", "paper"] = "paper"
    market_type: Literal["auto", "spot", "derivative", "option", "otc"] = "auto"
    ibkr_connection_mode: Literal["webapi", "tws"] = "webapi"
    ibkr_environment: Literal["gateway", "hosted"] = "gateway"
    schwab_environment: Literal["sandbox", "production"] = "sandbox"
    api_key: str = ""
    secret: str = ""
    password: str = ""
    account_id: str = ""
    risk_percent: int = Field(default=2, ge=1, le=100)
    remember_profile: bool = True
    solana: SolanaWorkspaceSettings = Field(default_factory=SolanaWorkspaceSettings)

    @model_validator(mode="after")
    def normalize_fields(self) -> "WorkspaceSettings":
        self.language = str(self.language or "en").strip() or "en"
        self.exchange = str(self.exchange or "paper").strip().lower() or "paper"
        self.api_key = str(self.api_key or "").strip()
        self.secret = str(self.secret or "").strip()
        self.password = str(self.password or "").strip()
        self.account_id = str(self.account_id or "").strip()
        if self.broker_type == "paper" or self.exchange == "paper":
            self.mode = "paper"
            self.exchange = "paper"
            self.broker_type = "paper"
        return self


class WorkspaceSettingsResponse(WorkspaceSettings):
    created_at: datetime | None = None
    updated_at: datetime | None = None
