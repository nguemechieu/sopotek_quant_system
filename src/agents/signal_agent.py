from agents.base_agent import BaseAgent


class SignalAgent(BaseAgent):
    def __init__(
        self,
        selector,
        display_builder=None,
        publisher=None,
        news_bias_applier=None,
        memory=None,
        event_bus=None,
    ):
        super().__init__("SignalAgent", memory=memory, event_bus=event_bus)
        self.selector = selector
        self.display_builder = display_builder
        self.publisher = publisher
        self.news_bias_applier = news_bias_applier

    async def process(self, context):
        working = dict(context or {})
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        candles = working.get("candles") or []
        dataset = working.get("dataset")

        signal, assigned_strategies = self.selector(symbol, candles, dataset)
        working["assigned_strategies"] = list(assigned_strategies or [])

        if signal is not None and callable(self.news_bias_applier):
            signal = await self.news_bias_applier(symbol, signal)
            if not signal:
                working["blocked_by_news_bias"] = True
                working["news_bias_reason"] = "Signal was neutralized by news bias controls."

        working["signal"] = signal
        display_signal = self.display_builder(working, signal, working["assigned_strategies"]) if callable(self.display_builder) else signal
        working["display_signal"] = display_signal

        if callable(self.publisher):
            self.publisher(working, display_signal)

        if signal is None:
            stage = "blocked" if working.get("blocked_by_news_bias") else "hold"
            reason = ""
            if isinstance(display_signal, dict):
                reason = str(display_signal.get("reason") or "").strip()
            if not reason:
                reason = str(working.get("news_bias_reason") or "No entry signal on the latest scan.").strip()
            self.remember(
                stage,
                {
                    "reason": reason,
                    "assigned_count": len(working["assigned_strategies"]),
                    "timeframe": working.get("timeframe"),
                },
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        self.remember(
            "selected",
            {
                "strategy_name": signal.get("strategy_name"),
                "timeframe": working.get("timeframe"),
                "side": signal.get("side"),
                "confidence": signal.get("confidence"),
                "reason": signal.get("reason"),
                "assigned_count": len(working["assigned_strategies"]),
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
