from pydantic import BaseModel, Field
from typing import Optional


# ==========================================
# Broker Configuration
# ==========================================

class BrokerConfig(BaseModel):

    type: str = Field(..., description="crypto / forex / stocks / paper")



    exchange: Optional[str] = Field(
        default=None,
        description="Exchange name (binance, coinbase, stellar, alpaca, oanda)"
    )

    customer_region: Optional[str] = Field(
        default=None,
        description="Customer jurisdiction hint such as us or global"
    )

    mode: str = Field(
        default="paper",
        description="paper or live trading"
    )

    api_key: Optional[str] = None
    secret: Optional[str] = None
    password: Optional[str] = None
    passphrase: Optional[str] = None
    uid: Optional[str] = None
    account_id: Optional[str] = None
    sandbox: bool = False
    timeout: int = 30000
    options: dict = Field(default_factory=dict)
    params: dict = Field(default_factory=dict)
    close: float = Field( default=None,
        description="Close broker"
    )


# ==========================================
# Risk Configuration
# ==========================================

class RiskConfig(BaseModel):

    risk_percent: float = Field(
        default=2,
        description="% risk per trade"
    )

    max_portfolio_risk: float = 1000
    max_daily_drawdown: float = 10

    max_position_size_pct: float = 5
    max_gross_exposure_pct: float = 100


# ==========================================
# System Configuration
# ==========================================

class SystemConfig(BaseModel):

    limit: int = Field(
        default=50000,
        description="Max candles stored in memory"
    )

    equity_refresh: int = Field(
        default=60,
        description="Equity refresh interval seconds"
    )

    rate_limit: int = Field(
        default=3,
        description="API request rate limit"
    )

    timeframe: str = "1h"


# ==========================================
# Main Application Config
# ==========================================

class AppConfig(BaseModel):

    broker: BrokerConfig
    risk: RiskConfig
    system: SystemConfig

    strategy: str = "LSTM"


# ==========================================
# Example Configuration Instance
# ==========================================

config = AppConfig(

    broker=BrokerConfig(
        type="crypto",
        exchange="coinbase",
        mode="live",
        api_key="wtwey",
        secret="qtyqi"
    ),

    risk=RiskConfig(
        risk_percent=2
    ),

    system=SystemConfig(
        limit=50000,
        rate_limit=30
    ),

    strategy="LSTM",

    time_frame="1h"
)
