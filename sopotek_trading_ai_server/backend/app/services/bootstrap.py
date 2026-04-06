from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import hash_password
from app.models.enums import StrategyStatus, UserRole
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy
from app.models.user import User


DEFAULT_RISK_LIMITS = {
    "max_position_pct": 0.08,
    "max_gross_exposure_pct": 1.8,
    "max_drawdown_pct": 0.12,
    "daily_loss_limit_pct": 0.03,
    "var_limit_pct": 0.02,
}

DEFAULT_STRATEGIES = [
    {
        "name": "Adaptive Trend",
        "code": "adaptive_trend",
        "description": "Trend-following engine with volatility-aware sizing and execution throttles.",
        "parameters": {"timeframe": "5m", "lookback": 55, "risk_budget_bps": 30},
        "performance": {"win_rate": 0.58, "sharpe": 1.82, "max_drawdown": 0.071},
        "assigned_symbols": ["EUR_USD", "XAU_USD", "BTC_USDT"],
    },
    {
        "name": "Mean Reversion FX",
        "code": "mean_reversion_fx",
        "description": "Session-aware short-term reversion model for liquid FX pairs.",
        "parameters": {"timeframe": "1m", "zscore_entry": 2.1, "zscore_exit": 0.5},
        "performance": {"win_rate": 0.63, "sharpe": 1.37, "max_drawdown": 0.048},
        "assigned_symbols": ["EUR_USD", "GBP_USD", "USD_JPY"],
    },
    {
        "name": "Event Breakout",
        "code": "event_breakout",
        "description": "Breakout strategy tuned for macro releases and sudden liquidity expansion.",
        "parameters": {"timeframe": "15m", "atr_multiple": 1.6, "confirmation_bars": 2},
        "performance": {"win_rate": 0.47, "sharpe": 1.94, "max_drawdown": 0.082},
        "assigned_symbols": ["NAS100_USD", "SPX500_USD", "XAU_USD"],
    },
]


async def provision_user_defaults(session: AsyncSession, user: User) -> None:
    portfolio_count = await session.scalar(select(func.count(Portfolio.id)).where(Portfolio.user_id == user.id))
    if not portfolio_count:
        session.add(
            Portfolio(
                user_id=user.id,
                account_id="primary",
                broker="paper",
                total_equity=100000.0,
                cash=100000.0,
                buying_power=100000.0,
                risk_limits=dict(DEFAULT_RISK_LIMITS),
            )
        )

    strategy_count = await session.scalar(select(func.count(Strategy.id)).where(Strategy.user_id == user.id))
    if not strategy_count:
        for template in DEFAULT_STRATEGIES:
            session.add(
                Strategy(
                    user_id=user.id,
                    name=template["name"],
                    code=template["code"],
                    description=template["description"],
                    status=StrategyStatus.ENABLED,
                    parameters=dict(template["parameters"]),
                    performance=dict(template["performance"]),
                    assigned_symbols=list(template["assigned_symbols"]),
                )
            )

    await session.flush()


async def ensure_bootstrap_admin(session: AsyncSession, settings) -> None:
    if not settings.bootstrap_admin_email or not settings.bootstrap_admin_password:
        return
    existing_user = await session.scalar(select(User).where(User.email == settings.bootstrap_admin_email))
    if existing_user is not None:
        return
    user = User(
        email=settings.bootstrap_admin_email,
        username=settings.bootstrap_admin_email.split("@", 1)[0],
        full_name="Platform Admin",
        password_hash=hash_password(settings.bootstrap_admin_password),
        role=UserRole.ADMIN,
        is_active=True,
    )
    session.add(user)
    await session.flush()
    await provision_user_defaults(session, user)
