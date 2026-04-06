from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _split_csv(value: str | None, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    raw = str(value or "").strip()
    if not raw:
        return default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(slots=True)
class Settings:
    app_name: str = "Sopotek Trading AI Platform API"
    environment: str = field(default_factory=lambda: os.getenv("SOPOTEK_PLATFORM_ENV", "development"))
    secret_key: str = field(default_factory=lambda: os.getenv("SOPOTEK_PLATFORM_SECRET_KEY", "change-me-in-production"))
    access_token_expire_minutes: int = field(
        default_factory=lambda: int(os.getenv("SOPOTEK_PLATFORM_ACCESS_TOKEN_MINUTES", "720"))
    )
    password_reset_token_expire_minutes: int = field(
        default_factory=lambda: int(os.getenv("SOPOTEK_PLATFORM_PASSWORD_RESET_MINUTES", "30"))
    )
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "SOPOTEK_PLATFORM_DATABASE_URL",
            "sqlite+aiosqlite:///./backend/platform.db",
        )
    )
    cors_origins: tuple[str, ...] = field(
        default_factory=lambda: _split_csv(
            os.getenv("SOPOTEK_PLATFORM_CORS_ORIGINS"),
            default=("http://localhost:3000", "http://127.0.0.1:3000"),
        )
    )
    frontend_base_url: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_PLATFORM_FRONTEND_BASE_URL", "http://localhost:3000")
    )
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_KAFKA_BOOTSTRAP_SERVERS", "memory")
    )
    kafka_client_id: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_CLIENT_ID", "sopotek-platform-api"))
    kafka_group_id: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_GROUP_ID", "sopotek-platform-web"))
    kafka_market_topic: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_MARKET_TOPIC", "market.data"))
    kafka_execution_topic: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_EXECUTION_TOPIC", "execution.events"))
    kafka_portfolio_topic: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_PORTFOLIO_TOPIC", "portfolio.updates"))
    kafka_risk_topic: str = field(default_factory=lambda: os.getenv("SOPOTEK_KAFKA_RISK_TOPIC", "risk.alerts"))
    kafka_strategy_state_topic: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_KAFKA_STRATEGY_STATE_TOPIC", "strategy.state")
    )
    kafka_strategy_command_topic: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_KAFKA_STRATEGY_COMMAND_TOPIC", "strategy.commands")
    )
    kafka_trading_command_topic: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_KAFKA_TRADING_COMMAND_TOPIC", "trading.commands")
    )
    kafka_risk_command_topic: str = field(
        default_factory=lambda: os.getenv("SOPOTEK_KAFKA_RISK_COMMAND_TOPIC", "risk.commands")
    )
    bootstrap_admin_email: str | None = field(
        default_factory=lambda: os.getenv("SOPOTEK_PLATFORM_BOOTSTRAP_ADMIN_EMAIL") or None
    )
    bootstrap_admin_password: str | None = field(
        default_factory=lambda: os.getenv("SOPOTEK_PLATFORM_BOOTSTRAP_ADMIN_PASSWORD") or None
    )

    @property
    def is_memory_kafka(self) -> bool:
        return str(self.kafka_bootstrap_servers or "").strip().lower() in {"", "memory", "inmemory"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
