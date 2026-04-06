from __future__ import annotations

from uuid import uuid4

from sqlalchemy import Enum, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampedMixin
from app.models.enums import LogLevel


class LogEntry(TimestampedMixin, Base):
    __tablename__ = "logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    category: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(64), default="platform")
    level: Mapped[LogLevel] = mapped_column(Enum(LogLevel), default=LogLevel.INFO, index=True)
    message: Mapped[str] = mapped_column(String(1024))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)

    user = relationship("User", back_populates="logs")
