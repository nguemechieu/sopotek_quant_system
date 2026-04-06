from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_control_service, get_state_store
from app.core.security import get_current_user, get_db, require_roles
from app.models.enums import UserRole
from app.models.portfolio import Portfolio
from app.models.user import User
from app.schemas.risk import AlertMessage, RiskResponse, RiskUpdateRequest


router = APIRouter()


@router.get("", response_model=RiskResponse)
async def get_risk(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    state_store=Depends(get_state_store),
) -> RiskResponse:
    portfolio = await db.scalar(
        select(Portfolio).where(Portfolio.user_id == current_user.id).order_by(Portfolio.updated_at.desc())
    )
    risk_state = await state_store.get_risk_snapshot(current_user.id)
    control_state = await state_store.get_control_state(current_user.id)
    alerts = await state_store.get_alerts(current_user.id)
    positions = await state_store.get_positions_snapshot(current_user.id)

    exposure_by_asset: dict[str, float] = {}
    for position in positions:
        asset_class = str(position.get("asset_class") or "unknown")
        exposure_by_asset[asset_class] = exposure_by_asset.get(asset_class, 0.0) + float(
            position.get("notional_exposure", 0.0) or 0.0
        )

    risk_limits = dict((portfolio.risk_limits if portfolio is not None else {}) or {})
    risk_limits.update(dict(risk_state.get("risk_limits") or {}))

    return RiskResponse(
        drawdown=float(risk_state.get("drawdown", portfolio.max_drawdown if portfolio is not None else 0.0) or 0.0),
        gross_exposure=float(
            risk_state.get("gross_exposure", portfolio.gross_exposure if portfolio is not None else 0.0) or 0.0
        ),
        net_exposure=float(
            risk_state.get("net_exposure", portfolio.net_exposure if portfolio is not None else 0.0) or 0.0
        ),
        exposure_by_asset=exposure_by_asset,
        risk_limits=risk_limits,
        trading_enabled=bool(control_state.get("trading_enabled")),
        selected_symbols=list(control_state.get("selected_symbols") or []),
        alerts=[AlertMessage.model_validate(alert) for alert in alerts],
        updated_at=risk_state.get("updated_at") or datetime.now(timezone.utc),
    )


@router.patch("", response_model=RiskResponse)
async def update_risk(
    payload: RiskUpdateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
    state_store=Depends(get_state_store),
) -> RiskResponse:
    risk_payload = await control_service.update_risk(db, user=current_user, payload=payload)
    await db.commit()
    alerts = await state_store.get_alerts(current_user.id)
    return RiskResponse(
        drawdown=float(risk_payload.get("drawdown", 0.0) or 0.0),
        gross_exposure=float(risk_payload.get("gross_exposure", 0.0) or 0.0),
        net_exposure=float(risk_payload.get("net_exposure", 0.0) or 0.0),
        exposure_by_asset=dict(risk_payload.get("exposure_by_asset") or {}),
        risk_limits=dict(risk_payload.get("risk_limits") or {}),
        trading_enabled=bool(risk_payload.get("trading_enabled")),
        selected_symbols=list(risk_payload.get("selected_symbols") or []),
        alerts=[AlertMessage.model_validate(alert) for alert in alerts],
        updated_at=risk_payload.get("updated_at"),
    )
