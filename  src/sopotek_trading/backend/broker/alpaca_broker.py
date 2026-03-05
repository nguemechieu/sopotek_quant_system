import aiohttp
import pandas as pd
from abc import ABC
from typing import Dict, Optional

from sopotek_trading.backend.broker.base_broker import BaseBroker


class AlpacaBroker(BaseBroker, ABC):

    def __init__(self, controller):

        super().__init__()

        self.controller = controller
        self.logger = getattr(controller, "logger", None)

        self.api_key = getattr(controller, "api_key", None)
        self.secret = getattr(controller, "secret", None)

        if not self.api_key or not self.secret:
            raise ValueError("Alpaca api_key and secret required")

        self.base_url = getattr(
            controller,
            "base_url",
            "https://paper-api.alpaca.markets"
        )

        self.data_url = getattr(
            controller,
            "data_url",
            "https://data.alpaca.markets"
        )

        self.mode = getattr(controller, "mode", "paper")
        self.rate_limiter = getattr(controller, "rate_limiter", None)

        self.session: Optional[aiohttp.ClientSession] = None
        self._connected = False

    # =====================================================
    # INTERNAL
    # =====================================================

    def _headers(self):

        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret
        }

    def _check_connection(self):

        if not self._connected:
            raise RuntimeError("Alpaca broker not connected")

    # =====================================================
    # CONNECT
    # =====================================================

    async def connect(self):

        if self._connected:
            return True

        self.session = aiohttp.ClientSession()
        self._connected = True

        if self.logger:
            self.logger.info("Alpaca connected")

        return True

    async def close(self):

        if self.session:
            await self.session.close()

        self._connected = False

        if self.logger:
            self.logger.info("Alpaca connection closed")

    async def ping(self):

        self._check_connection()

        url = f"{self.base_url}/v2/account"

        async with self.session.get(url, headers=self._headers()) as resp:
            return resp.status == 200

    # =====================================================
    # ACCOUNT
    # =====================================================

    async def fetch_balance(self) -> Dict:

        self._check_connection()

        url = f"{self.base_url}/v2/account"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            equity = float(data.get("equity", 0))
            cash = float(data.get("cash", 0))

            return {
                "equity": equity,
                "free": cash,
                "used": equity - cash,
                "currency": "USD"
            }

    async def fetch_positions(self):

        self._check_connection()

        url = f"{self.base_url}/v2/positions"

        async with self.session.get(url, headers=self._headers()) as resp:

            return await resp.json()

    async def fetch_open_orders(self):

        self._check_connection()

        url = f"{self.base_url}/v2/orders?status=open"

        async with self.session.get(url, headers=self._headers()) as resp:

            return await resp.json()

    async def fetch_order(self, order_id):

        self._check_connection()

        url = f"{self.base_url}/v2/orders/{order_id}"

        async with self.session.get(url, headers=self._headers()) as resp:

            return await resp.json()

    # =====================================================
    # MARKET DATA
    # =====================================================

    async def fetch_symbols(self):

        self._check_connection()

        url = f"{self.base_url}/v2/assets"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            return [
                asset["symbol"]
                for asset in data
                if asset.get("tradable")
            ]

    async def fetch_ticker(self, symbol):

        self._check_connection()

        url = f"{self.data_url}/v2/stocks/{symbol}/quotes/latest"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            quote = data.get("quote", {})

            return {
                "bid": quote.get("bp"),
                "ask": quote.get("ap"),
                "last": quote.get("ap")
            }

    async def fetch_price(self, symbol):

        ticker = await self.fetch_ticker(symbol)

        return ticker["last"]

    async def fetch_ohlcv(self, symbol, timeframe="1Min", limit=500):

        self._check_connection()

        url = f"{self.data_url}/v2/stocks/{symbol}/bars"

        params = {
            "timeframe": timeframe,
            "limit": limit
        }

        async with self.session.get(url, headers=self._headers(), params=params) as resp:

            data = await resp.json()

            bars = data.get("bars", [])

            rows = []

            for b in bars:

                rows.append({
                    "timestamp": b["t"],
                    "open": b["o"],
                    "high": b["h"],
                    "low": b["l"],
                    "close": b["c"],
                    "volume": b["v"]
                })

            return pd.DataFrame(rows)

    # =====================================================
    # TRADING
    # =====================================================

    async def create_order(
            self,
            symbol,
            side,
            order_type="market",
            amount=1,
            price=None,
            stop_loss=None,
            take_profit=None,
            slippage=None
    ):

        self._check_connection()

        url = f"{self.base_url}/v2/orders"

        payload = {
            "symbol": symbol,
            "qty": amount,
            "side": side,
            "type": order_type,
            "time_in_force": "gtc"
        }

        if order_type == "limit" and price:
            payload["limit_price"] = price

        # bracket order
        if stop_loss or take_profit:

            payload["order_class"] = "bracket"

            payload["stop_loss"] = {"stop_price": stop_loss} if stop_loss else 100
            payload["take_profit"] = {"limit_price": take_profit} if take_profit else 100

        async with self.session.post(
                url,
                json=payload,
                headers=self._headers()
        ) as resp:

            return await resp.json()

    async def cancel_order(self, order_id):

        self._check_connection()

        url = f"{self.base_url}/v2/orders/{order_id}"

        async with self.session.delete(url, headers=self._headers()) as resp:

            return await resp.json()

    async def cancel_all_orders(self):

        self._check_connection()

        url = f"{self.base_url}/v2/orders"

        async with self.session.delete(url, headers=self._headers()) as resp:

            return await resp.json()