from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_control_service
from app.core.security import get_db, require_roles
from app.models.enums import UserRole
from app.models.user import User


router = APIRouter()


class TradingControlRequest(BaseModel):
    selected_symbols: list[str] = Field(default_factory=list)


@router.post("/trading/start")
async def start_trading(
    payload: TradingControlRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
) -> dict:
    control_state = await control_service.set_trading_state(
        db,
        user=current_user,
        enabled=True,
        selected_symbols=payload.selected_symbols,
    )
    await db.commit()
    return {"status": "accepted", "trading_enabled": True, **control_state}


@router.post("/trading/stop")
async def stop_trading(
    payload: TradingControlRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.TRADER)),
    db: AsyncSession = Depends(get_db),
    control_service=Depends(get_control_service),
) -> dict:
    control_state = await control_service.set_trading_state(
        db,
        user=current_user,
        enabled=False,
        selected_symbols=payload.selected_symbols or None,
    )
    await db.commit()
    return {"status": "accepted", "trading_enabled": False, **control_state}
