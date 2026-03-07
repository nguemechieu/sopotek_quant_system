import asyncio
import json
import websockets

from sopotek_trading.event_bus.event import Event
from sopotek_trading.event_bus.event_types import EventType


class AlpacaWebSocket:

    def __init__(self, api_key, secret_key, symbols, event_bus):

        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.bus = event_bus

        self.url = "wss://stream.data.alpaca.markets/v2/sip"

    # ==========================================
    # CONNECT
    # ==========================================

    async def connect(self):

        async with websockets.connect(self.url) as ws:

            # Authenticate
            auth = {
                "action": "auth",
                "key": self.api_key,
                "secret": self.secret_key
            }

            await ws.send(json.dumps(auth))

            # Subscribe
            subscribe = {
                "action": "subscribe",
                "trades": self.symbols,
                "quotes": self.symbols
            }

            await ws.send(json.dumps(subscribe))

            while True:

                msg = await ws.recv()

                data = json.loads(msg)

                for tick in data:

                    if tick.get("T") != "q":
                        continue

                    ticker = {
                        "exchange": "alpaca",
                        "symbol": tick.get("S"),
                        "bid": tick.get("bp"),
                        "ask": tick.get("ap"),
                        "timestamp": tick.get("t")
                    }

                    event = Event(
                        type=EventType.MARKET_TICK,
                        data=ticker
                    )

                    await self.bus.publish(event)