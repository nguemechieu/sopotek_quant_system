import asyncio
import json
import websockets

from event_bus.event import Event
from event_bus.event_types import EventType


class CoinbaseWebSocket:

    def __init__(self, symbols, event_bus):

        self.symbols = symbols
        self.bus = event_bus

        self.url = "wss://ws-feed.exchange.coinbase.com"

    # ==========================================
    # CONNECT
    # ==========================================

    async def connect(self):

        async with websockets.connect(self.url) as ws:

            subscribe_msg = {
                "type": "subscribe",
                "channels": [
                    {
                        "name": "ticker",
                        "product_ids": self.symbols
                    }
                ]
            }

            await ws.send(json.dumps(subscribe_msg))

            while True:

                message = await ws.recv()

                data = json.loads(message)

                if data.get("type") != "ticker":
                    continue

                ticker = {
                    "symbol": data.get("product_id"),
                    "price": float(data.get("price", 0)),
                    "bid": float(data.get("best_bid", 0)),
                    "ask": float(data.get("best_ask", 0)),
                    "volume": float(data.get("volume_24h", 0)),
                    "timestamp": data.get("time")
                }

                event = Event(
                    type=EventType.MARKET_TICK,
                    data=ticker
                )

                await self.bus.publish(event)
