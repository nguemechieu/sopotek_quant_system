import socket
from abc import ABC
from typing import Optional

import aiohttp
import ccxt.async_support as ccxt

from sopotek_trading.backend.broker.base_broker import BaseBroker


class CoinbaseBroker(BaseBroker, ABC):

    def __init__(self, controller):

        super().__init__()

        self.controller = controller
        self.logger = getattr(controller, "logger", None)

        # Coinbase credentials
        self.api_key = getattr(controller, "api_key", "dje")
        self.secret = getattr(controller, "secret", "hd")
        self.password = getattr(controller, "password", "msj")

        if not self.api_key or not self.secret:
            raise ValueError("Coinbase api_key and secret required")

        self.exchange = None
        self.session = None
        self._connected = False

    # ==========================================
    # CONNECT
    # ==========================================

    async def connect(self):

        if self._connected:
            return True

        try:

            self.exchange = ccxt.coinbase({
                "apiKey": self.api_key,
                "secret": self.secret,
                "password": self.password,
                "enableRateLimit": True,
            })

            connector = aiohttp.TCPConnector(family=socket.AF_INET)

            self.session = aiohttp.ClientSession(connector=connector)

            self.exchange.session = self.session

            await self.exchange.load_time_difference()
            await self.exchange.load_markets()

            self._connected = True

            if self.logger:
                self.logger.info("Coinbase connected.")

            return True

        except Exception as e:

            if self.logger:
                self.logger.error(f"Coinbase connection failed: {e}")

            raise

    async def close(self):

        self._connected = False

        if self.exchange:
            await self.exchange.close()

        if self.session:
            await self.session.close()

    # ==========================================
    # ACCOUNT
    # ==========================================

    async def fetch_balance(self, currency="USD"):

        balance = await self.exchange.fetch_balance()

        total = balance.get("total", {})
        free = balance.get("free", {})
        used = balance.get("used", {})

        return {
            "equity": float(total.get(currency, 0)),
            "free": float(free.get(currency, 0)),
            "used": float(used.get(currency, 0)),
            "currency": currency
        }

    async def fetch_positions(self):

        # Coinbase spot has no positions
        return []

    async def fetch_open_orders(self):

        return await self.exchange.fetch_open_orders()

    async def fetch_order(self, order_id):

        return await self.exchange.fetch_order(order_id)

    # ==========================================
    # MARKET DATA
    # ==========================================

    async def fetch_symbols(self):

        if not self.exchange.markets:
            await self.exchange.load_markets()

        return list(self.exchange.symbols)

    async def fetch_ticker(self, symbol):

        return await self.exchange.fetch_ticker(symbol)

    async def fetch_price(self, symbol):

        ticker = await self.fetch_ticker(symbol)

        return ticker["last"]

    async def fetch_ohlcv(self, symbol, timeframe="1m", limit=500):

        return await self.exchange.fetch_ohlcv(
            symbol,
            timeframe,
            limit=limit
        )

    async def fetch_order_book(self, symbol):

        return await self.exchange.fetch_order_book(symbol)

    async def ping(self):

        return await self.exchange.fetch_time()

    # ==========================================
    # TRADING
    # ==========================================

    async def create_order(
            self,
            symbol: str,
            side: str,
            order_type: str,
            amount: float,
            price: Optional[float] = None,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            slippage: Optional[float] = None,
    ):

        if amount <= 0:
            raise ValueError("Invalid order amount")

        return await self.exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price
        )

    async def cancel_order(self, order_id):

        return await self.exchange.cancel_order(order_id)

    async def cancel_all_orders(self):

        orders = await self.fetch_open_orders()

        for order in orders:
            await self.cancel_order(order["id"])

        return {"status": "all_cancelled"}

    async def fetch_fees(self):

        return await self.exchange.fetch_trading_fees()