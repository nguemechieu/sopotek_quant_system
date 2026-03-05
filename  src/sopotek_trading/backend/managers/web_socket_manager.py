from sopotek_trading.backend.websocket.alphaca_web_socket import AlpacaWebSocket
from sopotek_trading.backend.websocket.binance_web_socket import BinanceWebSocket
from sopotek_trading.backend.websocket.oanda_web_socket import OandaWebSocket


class WebSocketManager:

    def __init__(
            self,
            controller,
            symbols,
            timeframe,
            candle_callback,
            ticker_callback
    ):

        self.controller = controller
        self.logger = controller.logger

        self.symbols = symbols
        self.timeframe = timeframe

        # 🔥 Proper dependency injection
        self.candle_callback = candle_callback
        self.ticker_callback = ticker_callback

        self.ws = None
        self.running = False

    # ======================================================
    # START
    # ======================================================

    async def start(self):

        broker_type = self.controller.type

        if broker_type == "crypto":

            self.ws = BinanceWebSocket(
                controller=self.controller,
                symbols=self.symbols,
                timeframe=self.timeframe,
                candle_callback=self.candle_callback,
                ticker_callback=self.ticker_callback
            )

        elif broker_type == "forex":

            self.ws = OandaWebSocket(
                controller=self.controller,
                symbols=self.symbols,
                timeframe=self.timeframe,
                candle_callback=self.candle_callback,
                ticker_callback=self.ticker_callback
            )

        elif broker_type == "stocks":

            self.ws = AlpacaWebSocket(
                controller=self.controller,
                symbols=self.symbols,
                candle_callback=self.candle_callback,
                ticker_callback=self.ticker_callback
            )

        else:
            raise ValueError(f"Unsupported broker type {broker_type}")

        await self.ws.start()

        self.running = True

    # ======================================================
    # STOP
    # ======================================================

    async def stop(self):

        self.running = False

        if self.ws:
            await self.ws.stop()