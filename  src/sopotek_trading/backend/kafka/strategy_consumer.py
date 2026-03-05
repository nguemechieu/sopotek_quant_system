import json



class StrategyConsumer:

    def __init__(self, kafka, strategy):

        self.kafka = kafka
        self.strategy = strategy

    async def start(self):

        await self.kafka.start_consumer(
            topic="market.candles",
            group_id="strategy-engine"
        )

        async for msg in self.kafka.consumer:

            data = json.loads(msg.value)

            symbol = data["symbol"]
            candle = data["candle"]
            timestamp = data["timestamp"]
            ticker = data["ticker"]

            signal = await self.strategy.generate_signal(symbol, candle)

            if signal:

                await self.kafka.publish("signals", signal)

            if timestamp:
                await self.kafka.publish("timestamp", timestamp)

            if symbol:
                await self.kafka.publish("symbol", symbol)

            if candle:
                await self.kafka.publish("candle", candle)

            if ticker:
                await self.kafka.publish("ticker", ticker)

