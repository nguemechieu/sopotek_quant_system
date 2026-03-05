import asyncio
import socket
from abc import ABC
from typing import Optional, Dict

import aiohttp
import ccxt.async_support as ccxt

from sopotek_trading.backend.broker.base_broker import BaseBroker


class CCXTBroker(BaseBroker, ABC):

    def __init__(self, controller):

        super().__init__()

        self.controller = controller
        self.logger = getattr(controller, "logger", None)

        # Credentials
        self.api_key = getattr(controller, "api_key", None)
        self.secret = getattr(controller, "secret", None)

        # Exchange configuration
        self.exchange_name = getattr(controller, "exchange_name", "binance").lower()
        self.mode = getattr(controller, "mode", "paper")
        self.exchange_options = getattr(controller, "exchange_options", "spot")

        self.rate_limiter = getattr(controller, "rate_limiter", None)

        # Exchange objects
        self.exchange = None
        self.session = None

        self._connected = False
        self._health_task = None
        self._reconnect_lock = asyncio.Lock()

        # Paper trading
        self.paper_balance = getattr(controller, "paper_balance", 10000.0)
        self.paper_positions = getattr(controller, "paper_positions", {})
        self.paper_order_id = getattr(controller, "paper_order_id", 0)

    # ======================================================
    # CONNECT
    # ======================================================

    async def connect(self):

        if self._connected:
            return True

        try:

            exchange_class = getattr(ccxt, self.exchange_name)

            self.exchange = exchange_class({
                "apiKey": self.api_key,
                "secret": self.secret,
                "enableRateLimit": True,
                "options": {
                    "adjustForTimeDifference": True,
                    "defaultType": self.exchange_options,
                }
            })

            # Force IPv4 (important for BinanceUS)
            connector = aiohttp.TCPConnector(family=socket.AF_INET)

            self.session = aiohttp.ClientSession(connector=connector)
            self.exchange.session = self.session

            # Sync time
            await self.exchange.load_time_difference()

            # Load markets
            await self.exchange.load_markets()

            self._connected = True

            if self.logger:
                self.logger.info(f"{self.exchange_name} connected.")

            self._health_task = asyncio.create_task(self._health_monitor())

            return True

        except Exception as e:

            if self.exchange:
                await self.exchange.close()

            if self.session:
                await self.session.close()

            if self.logger:
                self.logger.error(f"Broker connect failed: {e}")

            raise

    # ======================================================
    # HEALTH MONITOR
    # ======================================================

    async def _health_monitor(self):

        while self._connected:

            await asyncio.sleep(20)

            try:

                await self.exchange.fetch_time()

            except Exception as e:

                if self.logger:
                    self.logger.warning(f"Exchange heartbeat failed: {e}")

                asyncio.create_task(self._reconnect())

    # ======================================================
    # RECONNECT
    # ======================================================

    async def _reconnect(self):

        async with self._reconnect_lock:

            if not self._connected:
                return

            if self.logger:
                self.logger.warning("Reconnecting exchange...")

            try:

                await self.close()

                await asyncio.sleep(5)

                await self.connect()

            except Exception as e:

                if self.logger:
                    self.logger.error(f"Reconnect failed: {e}")

                await asyncio.sleep(10)

    # ======================================================
    # ACCOUNT
    # ======================================================

    async def fetch_balance(self, currency: str = "USDT") -> Dict:

        if self.mode == "paper":

            return {
                "equity": self.paper_balance,
                "free": self.paper_balance,
                "used": 0,
                "currency": currency
            }

        raw = await self.exchange.fetch_balance()

        total = raw.get("total", {})
        free = raw.get("free", {})
        used = raw.get("used", {})

        return {
            "equity": float(total.get(currency, 0)),
            "free": float(free.get(currency, 0)),
            "used": float(used.get(currency, 0)),
            "currency": currency
        }

    # ======================================================
    # MARKET DATA
    # ======================================================

    async def fetch_symbols(self):

     self._ensure_exchange()

     if not self.exchange.markets:
        await self.exchange.load_markets()

     return list(self.exchange.symbols)
    def _ensure_exchange(self):

     if not self.exchange:
        raise RuntimeError("Exchange not connected")



    async def fetch_ticker(self, symbol: str):

        if not self._connected:
            raise RuntimeError("Exchange not connected")

        return await self.exchange.fetch_ticker(symbol)

    async def fetch_price(self, symbol: str):

        ticker = await self.fetch_ticker(symbol)

        return ticker["last"]

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500):

        return await self.exchange.fetch_ohlcv(
            symbol,
            timeframe,
            limit=limit
        )

    async def fetch_order_book(self, symbol: str):

        return await self.exchange.fetch_order_book(symbol)

    async def ping(self):

        return await self.exchange.fetch_time()

    # ======================================================
    # TRADING
    # ======================================================

    async def create_order(
            self,
            symbol: str,
            side: str,
            order_type: str,
            amount: float,
            price: Optional[float] = None,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            slippage: Optional[float] = None
    ):

        if amount <= 0:
            raise ValueError("Order amount must be greater than zero")

        if self.mode == "paper":
            return await self._simulate_order(symbol, side, amount, price)

        if self.rate_limiter:
            await self.rate_limiter.wait()

        params = {}

        if stop_loss:
            params["stopLoss"] = stop_loss

        if take_profit:
            params["takeProfit"] = take_profit

        return await self.exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price,
            params=params
        )

    async def cancel_order(self, order_id: str):

        return await self.exchange.cancel_order(order_id)

    async def fetch_open_orders(self):

        return await self.exchange.fetch_open_orders()

    async def cancel_all_orders(self):

        return await self.exchange.cancel_all_orders()

    async def fetch_order(self, order_id: str):

        return await self.exchange.fetch_order(order_id)

    async def fetch_positions(self):

        if hasattr(self.exchange, "fetch_positions"):
            return await self.exchange.fetch_positions()

        return []

    async def fetch_fees(self):

        return await self.exchange.fetch_trading_fees()

    # ======================================================
    # PAPER TRADING
    # ======================================================

    async def _simulate_order(self, symbol, side, amount, price):

        if price is None:
            price = await self.fetch_price(symbol)

        self.paper_order_id += 1

        order_id = f"paper_{self.paper_order_id}"

        cost = amount * price

        position = self.paper_positions.get(symbol)

        if side.lower() == "buy":

            if self.paper_balance < cost:
                raise ValueError("Insufficient paper balance")

            self.paper_balance -= cost

            if position:

                total_amount = position["amount"] + amount

                avg_price = (
                                    position["amount"] * position["entry_price"]
                                    + amount * price
                            ) / total_amount

                position["amount"] = total_amount
                position["entry_price"] = avg_price

            else:

                self.paper_positions[symbol] = {
                    "amount": amount,
                    "entry_price": price
                }

        elif side.lower() == "sell":

            if not position or position["amount"] < amount:
                raise ValueError("No position to sell")

            pnl = (price - position["entry_price"]) * amount

            self.paper_balance += cost + pnl

            position["amount"] -= amount

            if position["amount"] == 0:
                del self.paper_positions[symbol]

        return {
            "id": order_id,
            "symbol": symbol,
            "side": side,
            "price": price,
            "amount": amount,
            "status": "filled",
            "paper_balance": self.paper_balance
        }

    # ======================================================
    # CLOSE
    # ======================================================

    async def close(self):

        self._connected = False

        if self._health_task:
            self._health_task.cancel()

        try:

            if self.exchange:
                await self.exchange.close()
                self.exchange = None

            if self.session:
                await self.session.close()
                self.session = None

            if self.logger:
                self.logger.info(f"{self.exchange_name} closed.")

        except Exception as e:

            if self.logger:
                self.logger.warning(f"Shutdown error: {e}")