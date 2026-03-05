from abc import ABC
from typing import Dict, List, Optional

import aiohttp

from sopotek_trading.backend.broker.base_broker import BaseBroker


class OandaBroker(BaseBroker, ABC):

    def __init__(self, controller):

        super().__init__()

        self.controller = controller
        self.logger = getattr(controller, "logger", None)

        self.api_key = getattr(controller, "api_key", None)
        self.account_id = getattr(controller, "account_id", None)

        if not self.api_key or not self.account_id:
            raise ValueError("OANDA api_key and account_id are required")

        self.base_url = "https://api-fxtrade.oanda.com/v3"

        self.session: Optional[aiohttp.ClientSession] = None
        self._connected = False

    # ======================================================
    # INTERNAL
    # ======================================================

    def _headers(self):

        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _check_connection(self):

        if not self._connected:
            raise RuntimeError("OANDA not connected")

    # ======================================================
    # CONNECT
    # ======================================================

    async def connect(self):

        if self._connected:
            return True

        self.session = aiohttp.ClientSession()

        self._connected = True

        if self.logger:
            self.logger.info("OANDA connected.")

        return True

    async def close(self):

        self._connected = False

        if self.session:
            await self.session.close()
            self.session = None

        if self.logger:
            self.logger.info("OANDA closed.")

    async def ping(self):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/summary"

        async with self.session.get(url, headers=self._headers()) as resp:
            return resp.status == 200

    # ======================================================
    # ACCOUNT
    # ======================================================

    async def fetch_balance(self) -> Dict:

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/summary"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            account = data["account"]

            equity = float(account["NAV"])
            margin_used = float(account.get("marginUsed", 0))
            margin_available = float(account.get("marginAvailable", equity))

            return {
                "equity": equity,
                "free": margin_available,
                "used": margin_used,
                "currency": account["currency"]
            }

    async def fetch_positions(self) -> List[Dict]:

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/positions"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            return data.get("positions", [])

    async def fetch_open_orders(self):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/orders"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            return data.get("orders", [])

    async def fetch_order(self, order_id):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/orders/{order_id}"

        async with self.session.get(url, headers=self._headers()) as resp:

            return await resp.json()

    # ======================================================
    # MARKET DATA
    # ======================================================

    async def fetch_ticker(self, symbol):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/pricing"

        params = {"instruments": symbol}

        async with self.session.get(url, headers=self._headers(), params=params) as resp:

            data = await resp.json()

            price = float(data["prices"][0]["closeoutBid"])

            return {"last": price}

    async def fetch_price(self, symbol):

        ticker = await self.fetch_ticker(symbol)

        return ticker["last"]

    async def fetch_symbols(self):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/instruments"

        async with self.session.get(url, headers=self._headers()) as resp:

            data = await resp.json()

            return [i["name"] for i in data.get("instruments", [])]

    async def fetch_ohlcv(self, symbol, timeframe="H1", limit=500):

        self._check_connection()

        url = f"{self.base_url}/instruments/{symbol}/candles"

        params = {
            "count": limit,
            "granularity": timeframe,
            "price": "M"
        }

        async with self.session.get(url, headers=self._headers(), params=params) as resp:

            data = await resp.json()

            candles = []

            for c in data.get("candles", []):

                if not c["complete"]:
                    continue

                candles.append([
                    c["time"],
                    float(c["mid"]["o"]),
                    float(c["mid"]["h"]),
                    float(c["mid"]["l"]),
                    float(c["mid"]["c"]),
                    float(c["volume"])
                ])

            return candles

    # ======================================================
    # TRADING
    # ======================================================

    async def create_order(
            self,
            symbol,
            side,
            order_type,
            amount,
            price=None,
            stop_loss=None,
            take_profit=None,
            slippage=None
    ):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/orders"

        units = str(amount if side.lower() == "buy" else -amount)

        order = {
            "instrument": symbol,
            "units": units,
            "type": "MARKET",
            "timeInForce": "FOK",
            "positionFill": "DEFAULT"
        }

        if stop_loss:
            order["stopLossOnFill"] = {"price": str(stop_loss)}

        if take_profit:
            order["takeProfitOnFill"] = {"price": str(take_profit)}

        payload = {"order": order}

        async with self.session.post(
                url,
                headers=self._headers(),
                json=payload
        ) as resp:

            return await resp.json()

    async def cancel_order(self, order_id):

        self._check_connection()

        url = f"{self.base_url}/accounts/{self.account_id}/orders/{order_id}"

        async with self.session.delete(url, headers=self._headers()) as resp:

            return await resp.json()

    async def cancel_all_orders(self):

        orders = await self.fetch_open_orders()

        for order in orders:
            await self.cancel_order(order["id"])

        return {"status": "all_cancelled"}