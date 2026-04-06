from __future__ import annotations

from uuid import uuid4

from sqlalchemy import Float, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampedMixin


class Portfolio(TimestampedMixin, Base):
    __tablename__ = "portfolios"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    account_id: Mapped[str] = mapped_column(String(128), default="primary", index=True)
    broker: Mapped[str] = mapped_column(String(64), default="paper")
    total_equity: Mapped[float] = mapped_column(Float, default=100000.0)
    cash: Mapped[float] = mapped_column(Float, default=100000.0)
    buying_power: Mapped[float] = mapped_column(Float, default=100000.0)
    daily_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    weekly_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    monthly_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    gross_exposure: Mapped[float] = mapped_column(Float, default=0.0)
    net_exposure: Mapped[float] = mapped_column(Float, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    var_95: Mapped[float] = mapped_column(Float, default=0.0)
    margin_usage: Mapped[float] = mapped_column(Float, default=0.0)
    positions_json: Mapped[dict] = mapped_column(JSON, default=dict)
    risk_limits: Mapped[dict] = mapped_column(JSON, default=dict)

    user = relationship("User", back_populates="portfolios")
