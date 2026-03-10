from abc import ABC, abstractmethod


class BaseBroker(ABC):

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def close(self):
        pass

    # ===============================
    # MARKET DATA
    # ===============================

    @abstractmethod
    async def fetch_ticker(self, symbol):
        pass

    async def fetch_tickers(self, symbols=None):
        raise NotImplementedError("fetch_tickers is not implemented for this broker")

    async def fetch_orderbook(self, symbol, limit=50):
        raise NotImplementedError("fetch_orderbook is not implemented for this broker")

    async def fetch_order_book(self, symbol, limit=50):
        return await self.fetch_orderbook(symbol, limit=limit)

    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
        raise NotImplementedError("fetch_ohlcv is not implemented for this broker")

    async def fetch_trades(self, symbol, limit=None):
        raise NotImplementedError("fetch_trades is not implemented for this broker")

    async def fetch_my_trades(self, symbol=None, limit=None):
        raise NotImplementedError("fetch_my_trades is not implemented for this broker")

    async def fetch_markets(self):
        raise NotImplementedError("fetch_markets is not implemented for this broker")

    async def fetch_currencies(self):
        raise NotImplementedError("fetch_currencies is not implemented for this broker")

    async def fetch_status(self):
        raise NotImplementedError("fetch_status is not implemented for this broker")

    # ===============================
    # TRADING
    # ===============================

    @abstractmethod
    async def create_order(
        self,
        symbol,
        side,
        amount,
        type="market",
        price=None,
        params=None,
        stop_loss=None,
        take_profit=None,
    ):
        pass

    @abstractmethod
    async def cancel_order(self, order_id, symbol=None):
        pass

    async def cancel_all_orders(self, symbol=None):
        raise NotImplementedError("cancel_all_orders is not implemented for this broker")

    # ===============================
    # ACCOUNT
    # ===============================

    @abstractmethod
    async def fetch_balance(self):
        pass

    async def fetch_positions(self, symbols=None):
        raise NotImplementedError("fetch_positions is not implemented for this broker")

    async def fetch_position(self, symbol):
        positions = await self.fetch_positions(symbols=[symbol])
        if isinstance(positions, list):
            for position in positions:
                if isinstance(position, dict) and position.get("symbol") == symbol:
                    return position
        return None

    def _position_amount(self, position):
        if not isinstance(position, dict):
            return 0.0
        for key in ("amount", "qty", "quantity", "size", "contracts"):
            value = position.get(key)
            if value is None:
                continue
            try:
                amount = abs(float(value))
            except Exception:
                continue
            if amount > 0:
                return amount
        return 0.0

    def _position_side(self, position):
        if not isinstance(position, dict):
            return None
        side = position.get("side")
        if side is not None:
            return str(side).lower()
        amount = position.get("amount")
        try:
            numeric = float(amount)
        except Exception:
            return None
        if numeric < 0:
            return "short"
        if numeric > 0:
            return "long"
        return None

    async def close_position(self, symbol, amount=None, params=None, order_type="market"):
        position = await self.fetch_position(symbol)
        if not isinstance(position, dict):
            return None

        close_amount = self._position_amount(position) if amount is None else abs(float(amount))
        if close_amount <= 0:
            return None

        side = self._position_side(position)
        # Closing reverses the current exposure regardless of how the broker labels it.
        if side in {"short", "sell"}:
            close_side = "buy"
        else:
            close_side = "sell"

        return await self.create_order(
            symbol=symbol,
            side=close_side,
            amount=close_amount,
            type=order_type,
            params=params,
        )

    async def close_all_positions(self, symbols=None, params=None, order_type="market"):
        try:
            positions = await self.fetch_positions(symbols=symbols)
        except TypeError:
            positions = await self.fetch_positions()

        closed = []
        for position in positions or []:
            if not isinstance(position, dict):
                continue
            symbol = position.get("symbol")
            if not symbol:
                continue
            result = await self.close_position(
                symbol=symbol,
                amount=self._position_amount(position),
                params=params,
                order_type=order_type,
            )
            if result is not None:
                closed.append(result)
        return closed

    async def fetch_order(self, order_id, symbol=None):
        raise NotImplementedError("fetch_order is not implemented for this broker")

    async def fetch_orders(self, symbol=None, limit=None):
        raise NotImplementedError("fetch_orders is not implemented for this broker")

    async def fetch_open_orders(self, symbol=None, limit=None):
        raise NotImplementedError("fetch_open_orders is not implemented for this broker")

    async def fetch_closed_orders(self, symbol=None, limit=None):
        raise NotImplementedError("fetch_closed_orders is not implemented for this broker")

    async def fetch_symbol(self):
        raise NotImplementedError("fetch_symbol is not implemented for this broker")

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    async def withdraw(self, code, amount, address, tag=None, params=None):
        raise NotImplementedError("withdraw is not implemented for this broker")

    async def fetch_deposit_address(self, code, params=None):
        raise NotImplementedError("fetch_deposit_address is not implemented for this broker")
