import asyncio
import aiohttp
import json
import traceback
from datetime import datetime


class OandaWebSocket:

    def __init__(
            self,controller,
            symbols,
            timeframe,
            candle_callback,
            ticker_callback,
            practice=True,
            logger=None,
    ):



        self.account_id = controller.account_id
        self.api_key = controller.api_key
        self.instruments = symbols  # ["EUR_USD", "GBP_USD"]
        self.timeframe_seconds = self._parse_timeframe(timeframe)

        self.on_candle_callback = candle_callback
        self.on_ticker_callback = ticker_callback
        self.logger = logger

        self.running = False

        self.base_url = (
            "https://stream-fxpractice.oanda.com"
            if practice
            else "https://stream-fxtrade.oanda.com"
        )

        self.current_candles = {}

    # ======================================================
    # START STREAM
    # ======================================================

    async def start(self):

        self.running = True

        url = (
            f"{self.base_url}/v3/accounts/"
            f"{self.account_id}/pricing/stream"
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

        params = {
            "instruments": ",".join(self.instruments)
        }

        async with aiohttp.ClientSession() as session:

            while self.running:

                try:
                    async with session.get(
                            url,
                            headers=headers,
                            params=params,
                    ) as resp:

                        async for line in resp.content:

                            if not self.running:
                                break

                            if not line:
                                continue

                            data = json.loads(line.decode())

                            if data.get("type") != "PRICE":
                                continue

                            await self._handle_price(data)

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"OANDA stream error: {e}")
                        traceback.print_exc()

                    await asyncio.sleep(5)

    # ======================================================
    # HANDLE PRICE
    # ======================================================

    async def _handle_price(self, data):

        instrument = data["instrument"]

        bid = float(data["bids"][0]["price"])
        ask = float(data["asks"][0]["price"])

        if self.on_ticker_callback:
            await self.on_ticker_callback(
                instrument.replace("_", "/"),
                bid,
                ask,
            )

        mid_price = (bid + ask) / 2

        timestamp = self._to_epoch(data["time"])

        await self._build_candle(
            instrument,
            mid_price,
            timestamp,
        )

    # ======================================================
    # CANDLE BUILDER
    # ======================================================

    async def _build_candle(
            self,
            instrument,
            price,
            timestamp,
    ):

        bucket = timestamp - (timestamp % self.timeframe_seconds)

        candle = self.current_candles.get(instrument)

        if candle is None or candle["timestamp"] != bucket:

            if candle and self.on_candle_callback:
                await self.on_candle_callback(
                    instrument.replace("_", "/"),
                    candle,
                )

            candle = {
                "timestamp": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,  # tick-based
            }

            self.current_candles[instrument] = candle

        else:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += 1

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
            "2h": 7200,
            "4h": 14400,
            "1d": 86400,
            "1w": 604800,
            "2w": 2592000,
            "1mn": (3600*24*7*4)
        }

        return mapping.get(tf, 60)

    def _to_epoch(self, iso_time):

        dt = datetime.fromisoformat(
            iso_time.replace("Z", "")
        )
        return int(dt.timestamp())

    # ======================================================
    # STOP
    # ======================================================

    def stop(self):
        self.running = False