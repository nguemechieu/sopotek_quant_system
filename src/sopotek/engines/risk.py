from __future__ import annotations

from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import AnalystInsight, PortfolioSnapshot, Signal, TradeReview


class RiskEngine:
    def __init__(
        self,
        event_bus: AsyncEventBus,
        *,
        starting_equity: float = 100000.0,
        max_risk_per_trade: float = 0.01,
        max_portfolio_exposure: float = 1.5,
        daily_drawdown_limit: float = 0.05,
    ) -> None:
        self.bus = event_bus
        self.starting_equity = max(1.0, float(starting_equity))
        self.max_risk_per_trade = max(0.0001, float(max_risk_per_trade))
        self.max_portfolio_exposure = max(0.01, float(max_portfolio_exposure))
        self.daily_drawdown_limit = max(0.001, float(daily_drawdown_limit))
        self.latest_snapshot = PortfolioSnapshot(cash=self.starting_equity, equity=self.starting_equity)
        self.insights: dict[str, AnalystInsight] = {}
        self.kill_switch_reason: str | None = None

        self.bus.subscribe(EventType.SIGNAL, self._on_signal)
        self.bus.subscribe(EventType.PORTFOLIO_SNAPSHOT, self._on_portfolio_snapshot)
        self.bus.subscribe(EventType.ANALYST_INSIGHT, self._on_analyst_insight)

    @property
    def kill_switch_active(self) -> bool:
        return bool(self.kill_switch_reason)

    def activate_kill_switch(self, reason: str) -> None:
        self.kill_switch_reason = str(reason or "Kill switch activated").strip()

    def reset_kill_switch(self) -> None:
        self.kill_switch_reason = None

    async def _on_portfolio_snapshot(self, event) -> None:
        snapshot = getattr(event, "data", None)
        if snapshot is None:
            return
        if not isinstance(snapshot, PortfolioSnapshot):
            snapshot = PortfolioSnapshot(**dict(snapshot))
        self.latest_snapshot = snapshot
        if snapshot.drawdown_pct >= self.daily_drawdown_limit:
            self.activate_kill_switch(
                f"Daily drawdown limit breached: {snapshot.drawdown_pct:.2%} >= {self.daily_drawdown_limit:.2%}"
            )

    async def _on_analyst_insight(self, event) -> None:
        insight = getattr(event, "data", None)
        if insight is None:
            return
        if not isinstance(insight, AnalystInsight):
            insight = AnalystInsight(**dict(insight))
        self.insights[insight.symbol] = insight

    async def _on_signal(self, event) -> None:
        signal = getattr(event, "data", None)
        if signal is None:
            return
        if not isinstance(signal, Signal):
            signal = Signal(**dict(signal))

        review = self.review_signal(signal)
        event_type = EventType.RISK_APPROVED if review.approved else EventType.RISK_REJECTED
        priority = 70 if review.approved else 10
        await self.bus.publish(event_type, review, priority=priority, source="risk_engine")
        if not review.approved:
            await self.bus.publish(
                EventType.RISK_ALERT,
                {"symbol": review.symbol, "reason": review.reason, "kill_switch_active": self.kill_switch_active},
                priority=5,
                source="risk_engine",
            )

    def review_signal(self, signal: Signal) -> TradeReview:
        if self.kill_switch_active:
            return TradeReview(
                approved=False,
                symbol=signal.symbol,
                side=signal.side,
                quantity=0.0,
                price=signal.price,
                reason=self.kill_switch_reason or "Kill switch active",
                strategy_name=signal.strategy_name,
            )

        if self.latest_snapshot.drawdown_pct >= self.daily_drawdown_limit:
            self.activate_kill_switch(
                f"Daily drawdown limit breached: {self.latest_snapshot.drawdown_pct:.2%} >= {self.daily_drawdown_limit:.2%}"
            )
            return TradeReview(
                approved=False,
                symbol=signal.symbol,
                side=signal.side,
                quantity=0.0,
                price=signal.price,
                reason=self.kill_switch_reason or "Daily drawdown limit breached",
                strategy_name=signal.strategy_name,
            )

        equity = max(1.0, float(self.latest_snapshot.equity or self.starting_equity))
        max_notional = equity * self.max_risk_per_trade
        insight = self.insights.get(signal.symbol)
        volatility = float(getattr(insight, "volatility", 1.0) or 1.0)
        volatility_multiplier = max(1.0, volatility)
        adjusted_quantity = min(float(signal.quantity), max_notional / max(signal.price, 1e-9))
        adjusted_quantity = adjusted_quantity / volatility_multiplier
        adjusted_quantity = max(0.0, adjusted_quantity)
        requested_notional = adjusted_quantity * max(signal.price, 0.0)
        projected_exposure = self.latest_snapshot.gross_exposure + requested_notional
        exposure_limit = equity * self.max_portfolio_exposure

        if adjusted_quantity <= 0:
            return TradeReview(
                approved=False,
                symbol=signal.symbol,
                side=signal.side,
                quantity=0.0,
                price=signal.price,
                reason="Signal size reduced to zero by risk constraints",
                strategy_name=signal.strategy_name,
            )

        if projected_exposure > exposure_limit:
            return TradeReview(
                approved=False,
                symbol=signal.symbol,
                side=signal.side,
                quantity=0.0,
                price=signal.price,
                reason="Portfolio exposure limit breached",
                strategy_name=signal.strategy_name,
                metadata={"projected_exposure": projected_exposure, "exposure_limit": exposure_limit},
            )

        return TradeReview(
            approved=True,
            symbol=signal.symbol,
            side=signal.side,
            quantity=adjusted_quantity,
            price=signal.price,
            reason=f"Approved with volatility multiplier {volatility_multiplier:.2f}",
            risk_score=min(1.0, requested_notional / max(equity, 1.0)),
            stop_price=signal.stop_price,
            take_profit=signal.take_profit,
            strategy_name=signal.strategy_name,
            metadata={
                "requested_quantity": signal.quantity,
                "volatility_multiplier": volatility_multiplier,
                "confidence": signal.confidence,
            },
        )
