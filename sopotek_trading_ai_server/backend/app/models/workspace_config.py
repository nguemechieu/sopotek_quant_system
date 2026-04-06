from __future__ import annotations

from uuid import uuid4

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampedMixin


class WorkspaceConfig(TimestampedMixin, Base):
    __tablename__ = "workspace_configs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True, index=True)
    settings_json: Mapped[dict] = mapped_column(JSON, default=dict)

    user = relationship("User", back_populates="workspace_config")
