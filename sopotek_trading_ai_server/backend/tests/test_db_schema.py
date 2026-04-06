from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime

from app.db.base import coerce_utc_datetime
from app.db.session import normalize_timestamped_model
from app.models.log import LogEntry
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy
from app.models.trade import Trade
from app.models.user import User


def test_timestamp_columns_are_timezone_aware() -> None:
    for model in (User, Portfolio, Strategy, Trade, LogEntry):
        created_at_type = model.__table__.c.created_at.type
        updated_at_type = model.__table__.c.updated_at.type

        assert isinstance(created_at_type, DateTime)
        assert created_at_type.timezone is True
        assert isinstance(updated_at_type, DateTime)
        assert updated_at_type.timezone is True


def test_coerce_utc_datetime_handles_naive_and_aware_inputs() -> None:
    aware_value = datetime(2026, 4, 6, 20, 49, 53, tzinfo=timezone.utc)
    naive_value = datetime(2026, 4, 6, 20, 49, 53)

    coerced_aware = coerce_utc_datetime(aware_value, timezone_aware=True)
    coerced_naive = coerce_utc_datetime(aware_value, timezone_aware=False)

    assert coerced_aware.tzinfo == timezone.utc
    assert coerced_naive.tzinfo is None
    assert coerce_utc_datetime(naive_value, timezone_aware=True).tzinfo == timezone.utc


def test_normalize_timestamped_model_stamps_timezone_aware_columns() -> None:
    user = User(
        email="trader@sopotek.ai",
        username="fundtrader",
        full_name="Fund Trader",
        password_hash="hash",
    )
    normalize_timestamped_model(user)

    assert user.created_at.tzinfo == timezone.utc
    assert user.updated_at.tzinfo == timezone.utc


def test_normalize_timestamped_model_can_follow_live_naive_schema_override() -> None:
    user = User(
        email="legacy@sopotek.ai",
        username="legacytrader",
        full_name="Legacy Trader",
        password_hash="hash",
    )
    normalize_timestamped_model(
        user,
        timezone_overrides={"users": {"created_at": False, "updated_at": False}},
    )

    assert user.created_at.tzinfo is None
    assert user.updated_at.tzinfo is None
