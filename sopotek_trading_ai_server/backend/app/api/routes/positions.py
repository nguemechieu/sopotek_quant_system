from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.dependencies import get_state_store
from app.core.security import get_current_user
from app.models.user import User
from app.schemas.portfolio import PositionSnapshot


router = APIRouter()


@router.get("", response_model=list[PositionSnapshot])
async def get_positions(
    current_user: User = Depends(get_current_user),
    state_store=Depends(get_state_store),
) -> list[PositionSnapshot]:
    rows = await state_store.get_positions_snapshot(current_user.id)
    return [PositionSnapshot.model_validate(row) for row in rows]
