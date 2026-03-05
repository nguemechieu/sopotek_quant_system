
import json
import websockets


class AlpacaWebSocket:

    def __init__(
            self,
            controller,
            symbols,
            candle_callback=None,
            ticker_callback=None
    ):

        self.controller = controller
        self.logger = controller.logger

        self.api_key = controller.api_key
        self.secret = controller.secret

        self.symbols = symbols

        self.candle_callback = candle_callback
        self.ticker_callback = ticker_callback

        self.ws = None
        self.running = False

        # Alpaca market data websocket
        self.url = "wss://stream.data.alpaca.markets/v2/sip"

    # ====================================
    # START
    # ====================================

    async def start(self):

        self.running = True

        async with websockets.connect(self.url) as ws:

            self.ws = ws

            await self._authenticate()

            await self._subscribe()

            if self.logger:
                self.logger.info("Alpaca websocket started")

            while self.running:

                msg = await ws.recv()

                data = json.loads(msg)

                await self._handle_message(data)

    # ====================================
    # AUTH
    # ====================================

    async def _authenticate(self):

        payload = {
            "action": "auth",
            "key": self.api_key,
            "secret": self.secret
        }

        await self.ws.send(json.dumps(payload))

    # ====================================
    # SUBSCRIBE
    # ====================================

    async def _subscribe(self):

        payload = {
            "action": "subscribe",
            "quotes": self.symbols,
            "bars": self.symbols
        }

        await self.ws.send(json.dumps(payload))

    # ====================================
    # MESSAGE HANDLER
    # ====================================

    async def _handle_message(self, data):

        for msg in data:

            msg_type = msg.get("T")

            # =========================
            # QUOTE (ticker)
            # =========================

            if msg_type == "q":

                symbol = msg.get("S")
                bid = msg.get("bp")
                ask = msg.get("ap")

                if self.ticker_callback:

                    await self.ticker_callback(
                        symbol,
                        bid,
                        ask
                    )

            # =========================
            # BAR (candle)
            # =========================

            elif msg_type == "b":

                symbol = msg.get("S")

                candle = {
                    "timestamp": msg.get("t"),
                    "open": msg.get("o"),
                    "high": msg.get("h"),
                    "low": msg.get("l"),
                    "close": msg.get("c"),
                    "volume": msg.get("v")
                }

                if self.candle_callback:

                    await self.candle_callback(
                        symbol,
                        candle
                    )

    # ====================================
    # STOP
    # ====================================

    async def stop(self):

        self.running = False

        if self.ws:
            await self.ws.close()

        if self.logger:
            self.logger.info("Alpaca websocket stopped")