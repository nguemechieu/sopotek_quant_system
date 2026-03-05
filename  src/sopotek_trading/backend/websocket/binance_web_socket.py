import asyncio
import json
import traceback

import websockets

from sopotek_trading.backend.websocket.candle_buffer import CandleBuffer


class BinanceWebSocket:

    def __init__(
            self,controller,
            symbols,
            timeframe,
            candle_callback,
            ticker_callback

    ):
        self.controller = controller
        self.logger=controller.logger
        if symbols is None or len(symbols) == 0:
            raise ValueError("symbols cannot be empty")
        if timeframe is None or len(timeframe) == 0:
            raise ValueError("timeframe cannot be empty")


        self.timeframe = timeframe

        self.base_url = "wss://stream.binance.us:9443/stream"
        self.symbols = symbols

        self.on_candle_callback = candle_callback
        self.on_ticker_callback = ticker_callback
        self.running = False
        self._ws = None

    # ======================================================
    # START
    # ======================================================

    async def start(self):

        self.running = True

        streams = []

        for symbol in self.symbols:
            streams.append(
                f"{symbol}@kline_{self.timeframe}"
            )
            streams.append(
                f"{symbol}@bookTicker"
            )

        url = f"{self.base_url}?streams={'/'.join(streams)}"

        while self.running:

            try:

                async with websockets.connect(
                        url,
                        ping_interval=20,
                        ping_timeout=20,
                ) as ws:

                    self._ws = ws

                    if self.controller.logger:
                        self.controller.logger.info("WebSocket connected.")

                    while self.running:

                        message = await ws.recv()
                        data = json.loads(message)
                        self.logger.debug(data)

                        if "data" not in data:
                            continue

                        payload = data["data"]

                        await self._handle_payload(payload)

            except asyncio.CancelledError:
                break

            except Exception as e:

                if self.logger:
                    self.logger.error(f"WebSocket error: {e}")
                    traceback.print_exc()

                # Reconnect delay
                await asyncio.sleep(5)

        if self.logger:
            self.logger.info("WebSocket stopped.")

    # ======================================================
    # HANDLE PAYLOAD
    # ======================================================

    async def _handle_payload(self, payload):

        try:

            # -----------------------------
            # Candle
            # -----------------------------
            if payload.get("e") == "kline":

                k = payload["k"]

                # Only closed candle
                if not k["x"]:
                    return

                symbol = payload["s"]

                if symbol.endswith("USDT"):
                    symbol = symbol[:-4] + "/USDT"

                candle = {
                    "timestamp": k["t"],
                    "open": float(k["o"]),
                    "high": float(k["h"]),
                    "low": float(k["l"]),
                    "close": float(k["c"]),
                    "volume": float(k["v"]),
                }
                candle_buffer= CandleBuffer(candle.__sizeof__())
                candle_buffer.update(symbol, candle)

                if self.on_candle_callback:
                    await self.on_candle_callback(symbol, candle_buffer)

            # -----------------------------
            # BookTicker
            # -----------------------------
            elif payload.get("e") == "bookTicker":

                symbol = payload["s"]

                if symbol.endswith("USDT"):
                    symbol = symbol[:-4] + "/USDT"

                bid = float(payload["b"])
                ask = float(payload["a"])

                if self.on_ticker_callback:
                    await self.on_ticker_callback(symbol, bid, ask)

        except Exception as e:
            if self.logger:
                self.logger.error(f"Payload processing error: {e}")
                traceback.print_exc()

    # ======================================================
    # STOP
    # ======================================================

    def stop(self):

        self.running = False

        if self._ws:
            asyncio.create_task(self._ws.close())