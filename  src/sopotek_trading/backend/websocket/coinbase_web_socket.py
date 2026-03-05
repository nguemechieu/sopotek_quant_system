import asyncio
import json
import time
import traceback

import websockets


class CoinbaseWebSocket:

    def __init__(
            self,controller,
            symbols,
            timeframe,
            candle_callback,
            ticker_callback
    ):

        # Convert BTC/USDT → BTC-USDT
        self.symbols = [
            s.replace("/", "-") for s in symbols
        ]

        self.timeframe = self._parse_timeframe(timeframe)
        self.on_candle_callback = candle_callback
        self.on_ticker_callback = ticker_callback
        self.logger = controller.logger

        self.base_url = "wss://ws-feed.exchange.coinbase.com"

        self.running = False
        self._ws = None

        # Candle builder storage
        self.current_candles = {}

    # ======================================================
    # START
    # ======================================================

    async def start(self):

        self.running = True

        while self.running:

            try:
                async with websockets.connect(
                        self.base_url,
                        ping_interval=20,
                ) as ws:

                    self._ws = ws

                    if self.logger:
                        self.logger.info("Coinbase WebSocket connected.")

                    # Subscribe to channels
                    subscribe_msg = {
                        "type": "subscribe",
                        "product_ids": self.symbols,
                        "channels": ["matches", "ticker"],
                    }

                    await ws.send(json.dumps(subscribe_msg))

                    while self.running:

                        message = await ws.recv()
                        data = json.loads(message)

                        await self._handle_message(data)

            except asyncio.CancelledError:
                break

            except Exception as e:
                if self.logger:
                    self.logger.error(f"Coinbase WS error: {e}")
                    traceback.print_exc()

                await asyncio.sleep(5)

        if self.logger:
            self.logger.info("Coinbase WebSocket stopped.")

    # ======================================================
    # HANDLE MESSAGE
    # ======================================================

    async def _handle_message(self, data):

        msg_type = data.get("type")

        # --------------------------
        # TRADE (match)
        # --------------------------
        if msg_type == "match":

            symbol = data["product_id"]
            price = float(data["price"])
            size = float(data["size"])
            timestamp = data["time"]

            await self._build_candle(
                symbol,
                price,
                size,
                timestamp,
            )

        # --------------------------
        # TICKER
        # --------------------------
        elif msg_type == "ticker":

            symbol = data["product_id"]

            bid = float(data.get("best_bid", 0))
            ask = float(data.get("best_ask", 0))

            if self.on_ticker_callback:
                await self.on_ticker_callback(
                    symbol.replace("-", "/"),
                    bid,
                    ask,
                )

    # ======================================================
    # CANDLE BUILDER
    # ======================================================

    async def _build_candle(
            self,
            symbol,
            price,
            volume,
            timestamp,
    ):

        ts = self._to_epoch(timestamp)
        bucket = ts - (ts % self.timeframe)

        candle = self.current_candles.get(symbol)

        if candle is None or candle["timestamp"] != bucket:

            # Emit previous candle
            if candle and self.on_candle_callback:
                await self.on_candle_callback(
                    symbol.replace("-", "/"),
                    candle,
                )

            # Create new candle
            candle = {
                "timestamp": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
            }

            self.current_candles[symbol] = candle

        else:
            # Update existing candle
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += volume

    # ======================================================
    # UTILS
    # ======================================================

    def _parse_timeframe(self, tf):

        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }

        return mapping.get(tf, 60)

    def _to_epoch(self, iso_time):
        return int(time.strptime(
            iso_time.split(".")[0],
            "%Y-%m-%dT%H:%M:%S"
        ).tm_sec)

    # ======================================================
    # STOP
    # ======================================================

    def stop(self):

        self.running = False

        if self._ws:
            asyncio.create_task(self._ws.close())