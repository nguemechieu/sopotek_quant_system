import asyncio
from typing import Dict, List, Optional
from abc import ABC

from sopotek_trading.backend.broker.base_broker import BaseBroker


class PaperBroker(BaseBroker, ABC):

    def __init__(self, controller):

        super().__init__()

        self.controller = controller
        self.logger = getattr(controller, "logger", None)

        self.balance = getattr(controller, "paper_balance", 10000.0)

        self.positions: Dict = {}
        self.orders: Dict = {}

        self.order_id = 0
        self._connected = False

    # ======================================================
    # CONNECT
    # ======================================================

    async def connect(self):

        self._connected = True

        if self.logger:
            self.logger.info("PaperBroker connected.")

        return True

    # ======================================================
    # ACCOUNT
    # ======================================================

    async def fetch_balance(self, currency="USDT"):

        used = sum(
            p["amount"] * p["entry_price"]
            for p in self.positions.values()
        )

        return {
            "equity": self.balance + self._unrealized_pnl(),
            "free": self.balance,
            "used": used,
            "currency": currency
        }

    async def fetch_positions(self):

        return list(self.positions.values())

    # ======================================================
    # MARKET DATA (Delegated to Controller Price Feed)
    # ======================================================

    async def fetch_price(self, symbol):

        if hasattr(self.controller, "get_price"):
            return await self.controller.get_price(symbol)

        raise RuntimeError("Controller must provide price feed")

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
            raise ValueError("Invalid order amount")

        if price is None:
            price = await self.fetch_price(symbol)

        self.order_id += 1

        order_id = f"paper_{self.order_id}"

        cost = amount * price

        position = self.positions.get(symbol)

        if side.lower() == "buy":

            if self.balance < cost:
                raise ValueError("Insufficient paper balance")

            self.balance -= cost

            if position:

                total_amount = position["amount"] + amount

                avg_price = (
                                    position["amount"] * position["entry_price"]
                                    + amount * price
                            ) / total_amount

                position["amount"] = total_amount
                position["entry_price"] = avg_price

            else:

                self.positions[symbol] = {
                    "symbol": symbol,
                    "amount": amount,
                    "entry_price": price,
                    "side": "long"
                }

        elif side.lower() == "sell":

            if not position or position["amount"] < amount:
                raise ValueError("No position to sell")

            pnl = (price - position["entry_price"]) * amount

            self.balance += amount * price
            self.balance += pnl

            position["amount"] -= amount

            if position["amount"] == 0:
                del self.positions[symbol]

        order = {
            "id": order_id,
            "symbol": symbol,
            "side": side,
            "price": price,
            "amount": amount,
            "status": "filled"
        }

        self.orders[order_id] = order

        return order

    # ======================================================
    # ORDER MANAGEMENT
    # ======================================================

    async def fetch_open_orders(self):

        return [
            o for o in self.orders.values()
            if o["status"] == "open"
        ]

    async def fetch_order(self, order_id):

        return self.orders.get(order_id)

    async def cancel_order(self, order_id):

        order = self.orders.get(order_id)

        if order and order["status"] == "open":
            order["status"] = "canceled"

        return order

    async def cancel_all_orders(self):

        for order in self.orders.values():

            if order["status"] == "open":
                order["status"] = "canceled"

        return True

    # ======================================================
    # PNL
    # ======================================================

    def _unrealized_pnl(self):

        pnl = 0

        if not hasattr(self.controller, "price_cache"):
            return 0

        for symbol, position in self.positions.items():

            price = self.controller.price_cache.get(symbol)

            if price:

                pnl += (
                               price - position["entry_price"]
                       ) * position["amount"]

        return pnl

    # ======================================================
    # SYMBOLS
    # ======================================================

    async def fetch_symbols(self):

        if hasattr(self.controller, "symbols"):
            return self.controller.symbols

        return []

    # ======================================================
    # CLOSE
    # ======================================================

    async def close(self):

        self._connected = False

        if self.logger:
            self.logger.info("PaperBroker closed.")