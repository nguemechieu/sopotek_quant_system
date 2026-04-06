from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import get_current_user, get_db, require_roles
from app.models.enums import UserRole
from app.models.portfolio import Portfolio
from app.models.user import User
from app.models.workspace_config import WorkspaceConfig
from app.schemas.workspace import WorkspaceSettings, WorkspaceSettingsResponse


router = APIRouter()


def _serialize_workspace_config(config: WorkspaceConfig | None) -> WorkspaceSettingsResponse:
    settings = WorkspaceSettings.model_validate((config.settings_json if config is not None else {}) or {})
    return WorkspaceSettingsResponse(
        **settings.model_dump(),
        created_at=getattr(config, "created_at", None),
        updated_at=getattr(config, "updated_at", None),
    )


@router.get("/settings", response_model=WorkspaceSettingsResponse)
async def get_workspace_settings(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> WorkspaceSettingsResponse:
    config = await db.scalar(select(WorkspaceConfig).where(WorkspaceConfig.user_id == current_user.id))
    return _serialize_workspace_config(config)


@router.put("/settings", response_model=WorkspaceSettingsResponse)
async def update_workspace_settings(
    payload: WorkspaceSettings,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
) -> WorkspaceSettingsResponse:
    config = await db.scalar(select(WorkspaceConfig).where(WorkspaceConfig.user_id == current_user.id))
    if config is None:
        config = WorkspaceConfig(user_id=current_user.id, settings_json={})
        db.add(config)

    normalized = WorkspaceSettings.model_validate(payload.model_dump())
    config.settings_json = normalized.model_dump(mode="json")

    portfolio = await db.scalar(
        select(Portfolio).where(Portfolio.user_id == current_user.id).order_by(Portfolio.updated_at.desc())
    )
    if portfolio is None:
        portfolio = Portfolio(user_id=current_user.id, account_id="primary", broker="paper", risk_limits={})
        db.add(portfolio)

    merged_risk_limits = dict(portfolio.risk_limits or {})
    merged_risk_limits.update(
        {
            "risk_percent": normalized.risk_percent,
            "broker_type": normalized.broker_type,
            "exchange": normalized.exchange,
            "mode": normalized.mode,
            "market_type": normalized.market_type,
            "remember_profile": normalized.remember_profile,
        }
    )
    portfolio.account_id = normalized.account_id or "primary"
    portfolio.broker = normalized.exchange or normalized.broker_type or "paper"
    portfolio.risk_limits = merged_risk_limits

    await db.flush()
    await db.commit()
    await db.refresh(config)
    return _serialize_workspace_config(config)
