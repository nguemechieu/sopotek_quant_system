class RiskService:

    def __init__(self, event_bus, risk_engine):

        self.event_bus = event_bus
        self.risk_engine = risk_engine

        event_bus.subscribe("signal.generated", self.on_signal)

    async def on_signal(self, signal):

        approved = self.risk_engine.validate_trade(signal)

        if approved:

            await self.event_bus.publish(
                "signal.approved",
                signal
            )