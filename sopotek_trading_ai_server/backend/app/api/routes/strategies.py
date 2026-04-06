from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_control_service
from app.core.security import get_current_user, get_db, require_roles
from app.models.enums import UserRole
from app.models.strategy import Strategy
from app.models.user import User
from app.schemas.strategies import StrategyCreateRequest, StrategyResponse, StrategyUpdateRequest


router = APIRouter()


@router.get("", response_model=list[StrategyResponse])
async def list_strategies(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[StrategyResponse]:
    rows = await db.scalars(
        select(Strategy).where(Strategy.user_id == current_user.id).order_by(Strategy.created_at.asc())
    )
    return [StrategyResponse.model_validate(strategy) for strategy in list(rows)]


@router.post("", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    payload: StrategyCreateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
) -> StrategyResponse:
    strategy = await control_service.create_strategy(db, user=current_user, payload=payload)
    await db.commit()
    await db.refresh(strategy)
    return StrategyResponse.model_validate(strategy)


@router.patch("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    strategy_id: str,
    payload: StrategyUpdateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
) -> StrategyResponse:
    try:
        strategy = await control_service.update_strategy(
            db,
            user=current_user,
            strategy_id=strategy_id,
            payload=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    await db.commit()
    await db.refresh(strategy)
    return StrategyResponse.model_validate(strategy)
