from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def coerce_utc_datetime(value: datetime | None, *, timezone_aware: bool) -> datetime:
    normalized = value or utc_now()
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    else:
        normalized = normalized.astimezone(timezone.utc)
    if timezone_aware:
        return normalized
    return normalized.replace(tzinfo=None)


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for the web platform."""


class TimestampedMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
