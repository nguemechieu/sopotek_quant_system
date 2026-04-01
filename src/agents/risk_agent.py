from event_bus.event import Event
from event_bus.event_types import EventType

from agents.base_agent import BaseAgent


class RiskAgent(BaseAgent):
    def __init__(self, reviewer, memory=None, event_bus=None):
        super().__init__("RiskAgent", memory=memory, event_bus=event_bus)
        self.reviewer = reviewer

    async def process(self, context):
        working = dict(context or {})
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        signal = working.get("signal")
        if not isinstance(signal, dict):
            working["halt_pipeline"] = True
            self.remember(
                "skipped",
                {"reason": working.get("news_bias_reason") or "No active signal.", "timeframe": working.get("timeframe")},
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        signal = dict(signal)
        signal.setdefault("decision_id", decision_id)
        working["signal"] = signal
        review = await self.reviewer(
            symbol=symbol,
            signal=signal,
            dataset=working.get("dataset"),
            timeframe=working.get("timeframe"),
            regime_snapshot=working.get("regime_snapshot"),
            portfolio_snapshot=working.get("portfolio_snapshot"),
        )
        working["trade_review"] = review
        approved = bool((review or {}).get("approved"))
        if not approved:
            working["halt_pipeline"] = True
            if self.event_bus is not None:
                await self.event_bus.publish(
                    Event(
                        EventType.RISK_ALERT,
                        {
                            "symbol": symbol,
                            "decision_id": decision_id,
                            "stage": (review or {}).get("stage"),
                            "reason": (review or {}).get("reason"),
                            "strategy_name": signal.get("strategy_name"),
                            "timeframe": (review or {}).get("timeframe") or working.get("timeframe"),
                            "side": signal.get("side"),
                        },
                    )
                )
            self.remember(
                "rejected",
                {
                    "stage": (review or {}).get("stage"),
                    "reason": (review or {}).get("reason"),
                    "strategy_name": signal.get("strategy_name"),
                    "timeframe": review.get("timeframe") if isinstance(review, dict) else working.get("timeframe"),
                    "approved": False,
                },
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        self.remember(
            "approved",
            {
                "amount": review.get("amount"),
                "price": review.get("price"),
                "strategy_name": review.get("strategy_name"),
                "timeframe": review.get("timeframe"),
                "side": review.get("side"),
                "execution_strategy": review.get("execution_strategy"),
                "approved": True,
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
