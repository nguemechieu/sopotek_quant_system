from __future__ import annotations

from uuid import uuid4

from sqlalchemy import Boolean, Enum, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampedMixin
from app.models.enums import UserRole


class User(TimestampedMixin, Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), default=UserRole.TRADER, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    portfolios = relationship("Portfolio", back_populates="user", cascade="all, delete-orphan")
    trades = relationship("Trade", back_populates="user", cascade="all, delete-orphan")
    strategies = relationship("Strategy", back_populates="user", cascade="all, delete-orphan")
    logs = relationship("LogEntry", back_populates="user", cascade="all, delete-orphan")
    workspace_config = relationship("WorkspaceConfig", back_populates="user", cascade="all, delete-orphan", uselist=False)
