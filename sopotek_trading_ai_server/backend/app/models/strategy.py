from __future__ import annotations

from uuid import uuid4

from sqlalchemy import Enum, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampedMixin
from app.models.enums import StrategyStatus


class Strategy(TimestampedMixin, Base):
    __tablename__ = "strategies"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    code: Mapped[str] = mapped_column(String(128), index=True)
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    status: Mapped[StrategyStatus] = mapped_column(Enum(StrategyStatus), default=StrategyStatus.ENABLED, index=True)
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    performance: Mapped[dict] = mapped_column(JSON, default=dict)
    assigned_symbols: Mapped[list] = mapped_column(JSON, default=list)

    user = relationship("User", back_populates="strategies")
    trades = relationship("Trade", back_populates="strategy")
