from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_state_store
from app.core.security import get_current_user, get_db
from app.models.portfolio import Portfolio
from app.models.user import User
from app.schemas.portfolio import PortfolioHistoryPoint, PortfolioResponse, PositionSnapshot


router = APIRouter()


@router.get("", response_model=PortfolioResponse)
async def get_portfolio(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    state_store=Depends(get_state_store),
) -> PortfolioResponse:
    latest = await db.scalar(
        select(Portfolio).where(Portfolio.user_id == current_user.id).order_by(Portfolio.updated_at.desc())
    )
    history_rows = await db.scalars(
        select(Portfolio).where(Portfolio.user_id == current_user.id).order_by(Portfolio.updated_at.desc()).limit(20)
    )
    positions = await state_store.get_positions_snapshot(current_user.id)
    portfolio_snapshot = await state_store.get_portfolio_snapshot(current_user.id)
    control_state = await state_store.get_control_state(current_user.id)

    if latest is None:
        account_id = str(portfolio_snapshot.get("account_id") or "primary")
        broker = str(portfolio_snapshot.get("broker") or "paper")
        total_equity = float(portfolio_snapshot.get("total_equity", 100000.0) or 100000.0)
        cash = float(portfolio_snapshot.get("cash", total_equity) or total_equity)
        buying_power = float(portfolio_snapshot.get("buying_power", total_equity) or total_equity)
        daily_pnl = float(portfolio_snapshot.get("daily_pnl", 0.0) or 0.0)
        weekly_pnl = float(portfolio_snapshot.get("weekly_pnl", 0.0) or 0.0)
        monthly_pnl = float(portfolio_snapshot.get("monthly_pnl", 0.0) or 0.0)
        gross_exposure = float(portfolio_snapshot.get("gross_exposure", 0.0) or 0.0)
        net_exposure = float(portfolio_snapshot.get("net_exposure", 0.0) or 0.0)
        max_drawdown = float(portfolio_snapshot.get("max_drawdown", 0.0) or 0.0)
        var_95 = float(portfolio_snapshot.get("var_95", 0.0) or 0.0)
        margin_usage = float(portfolio_snapshot.get("margin_usage", 0.0) or 0.0)
        risk_limits = dict(portfolio_snapshot.get("risk_limits") or {})
    else:
        account_id = latest.account_id
        broker = latest.broker
        total_equity = latest.total_equity
        cash = latest.cash
        buying_power = latest.buying_power
        daily_pnl = latest.daily_pnl
        weekly_pnl = latest.weekly_pnl
        monthly_pnl = latest.monthly_pnl
        gross_exposure = latest.gross_exposure
        net_exposure = latest.net_exposure
        max_drawdown = latest.max_drawdown
        var_95 = latest.var_95
        margin_usage = latest.margin_usage
        risk_limits = dict(latest.risk_limits or {})

    return PortfolioResponse(
        account_id=account_id,
        broker=broker,
        total_equity=total_equity,
        cash=cash,
        buying_power=buying_power,
        daily_pnl=daily_pnl,
        weekly_pnl=weekly_pnl,
        monthly_pnl=monthly_pnl,
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        max_drawdown=max_drawdown,
        var_95=var_95,
        margin_usage=margin_usage,
        active_positions=len(positions),
        selected_symbols=list(control_state.get("selected_symbols") or []),
        risk_limits=risk_limits,
        positions=[PositionSnapshot.model_validate(position) for position in positions],
        history=[
            PortfolioHistoryPoint(timestamp=row.updated_at, total_equity=row.total_equity)
            for row in list(history_rows)
        ][::-1],
    )
