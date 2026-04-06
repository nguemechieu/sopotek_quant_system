from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_control_service, get_state_store
from app.core.security import get_current_user, get_db, require_roles
from app.models.enums import UserRole
from app.models.trade import Trade
from app.models.user import User
from app.schemas.orders import OrderCreateRequest, TradeResponse


router = APIRouter()


@router.get("", response_model=list[TradeResponse])
async def get_orders(
    status_filter: str | None = Query(default=None, alias="status"),
    current_user: User = Depends(get_current_user),
    state_store=Depends(get_state_store),
    db: AsyncSession = Depends(get_db),
) -> list[TradeResponse]:
    live_orders = await state_store.get_orders_snapshot(current_user.id, status_filter=status_filter)
    if live_orders:
        rows = await db.scalars(select(Trade).where(Trade.user_id == current_user.id).order_by(Trade.created_at.desc()).limit(200))
        persisted = {trade.order_id: trade for trade in list(rows)}
        response: list[TradeResponse] = []
        for live_order in live_orders:
            trade = persisted.get(str(live_order.get("order_id") or ""))
            if trade is not None:
                response.append(TradeResponse.model_validate(trade))
        if response:
            return response
    trades = await db.scalars(
        select(Trade).where(Trade.user_id == current_user.id).order_by(Trade.created_at.desc()).limit(200)
    )
    results = list(trades)
    if status_filter:
        normalized = status_filter.strip().lower()
        results = [row for row in results if row.status.value == normalized]
    return [TradeResponse.model_validate(trade) for trade in results]


@router.get("/trades", response_model=list[TradeResponse])
async def get_trade_history(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[TradeResponse]:
    trades = await db.scalars(
        select(Trade).where(Trade.user_id == current_user.id).order_by(Trade.created_at.desc()).limit(200)
    )
    return [TradeResponse.model_validate(trade) for trade in list(trades)]


@router.post("", response_model=TradeResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_order(
    payload: OrderCreateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
) -> TradeResponse:
    trade = await control_service.submit_order(db, user=current_user, payload=payload)
    await db.commit()
    await db.refresh(trade)
    return TradeResponse.model_validate(trade)
