class MarketFeedService:

    def __init__(self, event_bus, websocket_manager):

        self.event_bus = event_bus
        self.websocket_manager = websocket_manager

    async def on_candle(self, symbol, candle):

        await self.event_bus.publish(
            "market.candle",
            {
                "symbol": symbol,
                "candle": candle
            }
        )

    async def start(self):
        await self.websocket_manager.start()