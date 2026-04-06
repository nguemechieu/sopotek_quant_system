from __future__ import annotations


class TradingCoreBridge:
    """Bridges Kafka topics from the trading core into the API state store."""

    def __init__(self, *, settings, state_store, kafka_gateway) -> None:
        self.settings = settings
        self.state_store = state_store
        self.kafka_gateway = kafka_gateway

    def bind(self) -> None:
        for topic in (
            self.settings.kafka_market_topic,
            self.settings.kafka_execution_topic,
            self.settings.kafka_portfolio_topic,
            self.settings.kafka_risk_topic,
            self.settings.kafka_strategy_state_topic,
        ):
            self.kafka_gateway.subscribe(topic, self._handle_event)

    async def _handle_event(self, topic: str, payload: dict) -> None:
        await self.state_store.apply_kafka_event(topic, payload)
