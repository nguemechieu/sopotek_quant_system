import numpy as np

from strategy.base_strategy import BaseStrategy
from event_bus.event_types import EventType


class MeanReversionStrategy(BaseStrategy):
    

    def __init__(self, event_bus):

        super().__init__(event_bus)
        

        self.prices = []

        self.bus.subscribe(EventType.MARKET_TICK, self.on_tick)

    async def on_tick(self, event):

        tick = event.data

        price = tick["price"]
        symbol = tick["symbol"]

        self.prices.append(price)

        if len(self.prices) < 20:
            return

        mean_price = np.mean(self.prices[-20:])

        deviation = (price - mean_price) / mean_price

        if deviation < -0.02:

            await self.signal(symbol, "BUY", 0.01)

        elif deviation > 0.02:

            await self.signal(symbol, "SELL", 0.01)
