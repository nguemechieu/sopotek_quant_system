from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import LogLevel, OrderStatus
from app.models.log import LogEntry
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy
from app.models.trade import Trade
from app.schemas.orders import OrderCreateRequest
from app.schemas.risk import RiskUpdateRequest
from app.schemas.strategies import StrategyCreateRequest, StrategyUpdateRequest


class TradingControlService:
    def __init__(self, *, settings, state_store, kafka_gateway) -> None:
        self.settings = settings
        self.state_store = state_store
        self.kafka_gateway = kafka_gateway

    async def create_log(
        self,
        session: AsyncSession,
        *,
        user_id: str | None,
        category: str,
        level: LogLevel,
        message: str,
        payload: dict | None = None,
        source: str = "platform",
    ) -> LogEntry:
        entry = LogEntry(
            user_id=user_id,
            category=category,
            level=level,
            message=message,
            payload=dict(payload or {}),
            source=source,
        )
        session.add(entry)
        await session.flush()
        return entry

    async def submit_order(self, session: AsyncSession, *, user, payload: OrderCreateRequest) -> Trade:
        order_id = f"web-{uuid4()}"
        trade = Trade(
            user_id=user.id,
            strategy_id=payload.strategy_id,
            order_id=order_id,
            symbol=payload.symbol.upper(),
            side=payload.side.lower(),
            order_type=payload.order_type.lower(),
            status=OrderStatus.PENDING,
            quantity=float(payload.quantity),
            requested_price=payload.limit_price,
            filled_quantity=0.0,
            venue=payload.venue,
            reason=payload.reason,
            details=dict(payload.metadata or {}),
        )
        session.add(trade)
        await self.create_log(
            session,
            user_id=user.id,
            category="orders",
            level=LogLevel.INFO,
            message=f"Submitted {payload.side.upper()} order for {payload.symbol.upper()}",
            payload={"order_id": order_id, "symbol": payload.symbol.upper(), "quantity": payload.quantity},
        )
        await session.flush()
        await self.kafka_gateway.publish(
            self.settings.kafka_trading_command_topic,
            {
                "command": "submit_order",
                "user_id": user.id,
                "order_id": order_id,
                "symbol": payload.symbol.upper(),
                "side": payload.side.lower(),
                "quantity": float(payload.quantity),
                "order_type": payload.order_type.lower(),
                "limit_price": payload.limit_price,
                "venue": payload.venue,
                "strategy_id": payload.strategy_id,
                "metadata": dict(payload.metadata or {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            key=user.id,
        )
        await self.state_store.publish_order(
            user.id,
            {
                "id": trade.id,
                "order_id": order_id,
                "symbol": trade.symbol,
                "side": trade.side,
                "status": trade.status.value,
                "quantity": trade.quantity,
                "filled_quantity": trade.filled_quantity,
                "average_price": trade.average_price,
                "requested_price": trade.requested_price,
                "venue": trade.venue,
                "created_at": trade.created_at.isoformat() if trade.created_at else None,
                "updated_at": trade.updated_at.isoformat() if trade.updated_at else None,
                "reason": trade.reason,
            },
        )
        return trade

    async def create_strategy(self, session: AsyncSession, *, user, payload: StrategyCreateRequest):
        strategy = Strategy(
            user_id=user.id,
            name=payload.name,
            code=payload.code,
            description=payload.description,
            status=payload.status,
            parameters=dict(payload.parameters or {}),
            performance=dict(payload.performance or {}),
            assigned_symbols=list(payload.assigned_symbols or []),
        )
        session.add(strategy)
        await session.flush()
        await self.kafka_gateway.publish(
            self.settings.kafka_strategy_command_topic,
            {
                "command": "create_strategy",
                "user_id": user.id,
                "strategy_id": strategy.id,
                "name": strategy.name,
                "code": strategy.code,
                "status": strategy.status.value,
                "parameters": strategy.parameters,
                "assigned_symbols": strategy.assigned_symbols,
            },
            key=user.id,
        )
        await self.state_store.publish_strategy(
            user.id,
            {
                "id": strategy.id,
                "name": strategy.name,
                "code": strategy.code,
                "status": strategy.status.value,
                "parameters": strategy.parameters,
                "performance": strategy.performance,
                "assigned_symbols": strategy.assigned_symbols,
            },
        )
        return strategy

    async def update_strategy(self, session: AsyncSession, *, user, strategy_id: str, payload: StrategyUpdateRequest):
        strategy = await session.scalar(
            select(Strategy).where(Strategy.id == strategy_id, Strategy.user_id == user.id)
        )
        if strategy is None:
            raise ValueError("Strategy not found")
        if payload.status is not None:
            strategy.status = payload.status
        if payload.parameters is not None:
            strategy.parameters = dict(payload.parameters)
        if payload.performance is not None:
            strategy.performance = dict(payload.performance)
        if payload.assigned_symbols is not None:
            strategy.assigned_symbols = list(payload.assigned_symbols)
        await session.flush()
        await self.kafka_gateway.publish(
            self.settings.kafka_strategy_command_topic,
            {
                "command": "update_strategy",
                "user_id": user.id,
                "strategy_id": strategy.id,
                "status": strategy.status.value,
                "parameters": strategy.parameters,
                "performance": strategy.performance,
                "assigned_symbols": strategy.assigned_symbols,
            },
            key=user.id,
        )
        await self.state_store.publish_strategy(
            user.id,
            {
                "id": strategy.id,
                "name": strategy.name,
                "code": strategy.code,
                "status": strategy.status.value,
                "parameters": strategy.parameters,
                "performance": strategy.performance,
                "assigned_symbols": strategy.assigned_symbols,
            },
        )
        return strategy

    async def update_risk(self, session: AsyncSession, *, user, payload: RiskUpdateRequest) -> dict:
        portfolio = await session.scalar(
            select(Portfolio).where(Portfolio.user_id == user.id).order_by(Portfolio.updated_at.desc())
        )
        if portfolio is None:
            portfolio = Portfolio(user_id=user.id, account_id="primary", broker="paper", risk_limits={})
            session.add(portfolio)
            await session.flush()

        merged_limits = dict(portfolio.risk_limits or {})
        merged_limits.update(dict(payload.risk_limits or {}))
        portfolio.risk_limits = merged_limits
        await session.flush()

        control_state = await self.state_store.set_control_state(
            user.id,
            selected_symbols=payload.selected_symbols,
        ) if payload.selected_symbols is not None else await self.state_store.get_control_state(user.id)

        risk_payload = {
            "drawdown": portfolio.max_drawdown,
            "gross_exposure": portfolio.gross_exposure,
            "net_exposure": portfolio.net_exposure,
            "risk_limits": dict(portfolio.risk_limits or {}),
            "selected_symbols": list(control_state.get("selected_symbols") or []),
            "trading_enabled": bool(control_state.get("trading_enabled")),
            "updated_at": datetime.now(timezone.utc),
        }
        await self.kafka_gateway.publish(
            self.settings.kafka_risk_command_topic,
            {
                "command": "update_risk",
                "user_id": user.id,
                "risk_limits": dict(portfolio.risk_limits or {}),
                "selected_symbols": list(control_state.get("selected_symbols") or []),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            key=user.id,
        )
        await self.state_store.publish_risk(user.id, risk_payload)
        return risk_payload

    async def set_trading_state(
        self,
        session: AsyncSession,
        *,
        user,
        enabled: bool,
        selected_symbols: list[str] | None = None,
    ) -> dict:
        control_state = await self.state_store.set_control_state(
            user.id,
            trading_enabled=enabled,
            selected_symbols=selected_symbols,
        )
        await self.create_log(
            session,
            user_id=user.id,
            category="controls",
            level=LogLevel.INFO,
            message="Trading enabled" if enabled else "Trading stopped",
            payload={"selected_symbols": list(control_state.get("selected_symbols") or [])},
        )
        await self.kafka_gateway.publish(
            self.settings.kafka_trading_command_topic,
            {
                "command": "start_trading" if enabled else "stop_trading",
                "user_id": user.id,
                "selected_symbols": list(control_state.get("selected_symbols") or []),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            key=user.id,
        )
        return control_state
