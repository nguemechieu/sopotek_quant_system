from abc import ABC, abstractmethod

from broker.market_venues import supported_market_venues_for_profile


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
        stop_price=None,
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

    async def close_position(
        self,
        symbol,
        amount=None,
        params=None,
        order_type="market",
        position=None,
        position_side=None,
        position_id=None,
    ):
        target_position = position if isinstance(position, dict) else None
        if not isinstance(target_position, dict):
            try:
                positions = await self.fetch_positions(symbols=[symbol])
            except TypeError:
                positions = await self.fetch_positions()
            except Exception:
                positions = []

            candidates = [
                item
                for item in (positions or [])
                if isinstance(item, dict) and item.get("symbol") == symbol
            ]
            if position_id:
                normalized_id = str(position_id).strip().lower()
                candidates = [
                    item
                    for item in candidates
                    if str(
                        item.get("position_id")
                        or item.get("id")
                        or item.get("trade_id")
                        or ""
                    ).strip().lower() == normalized_id
                ]
            if position_side:
                normalized_side = str(position_side).strip().lower()
                candidates = [
                    item
                    for item in candidates
                    if str(item.get("position_side") or item.get("side") or "").strip().lower() == normalized_side
                ]
            if len(candidates) > 1 and self.supports_hedging():
                raise ValueError(
                    f"Multiple hedge legs are open for {symbol}. Specify the long or short position to close."
                )
            target_position = candidates[0] if candidates else await self.fetch_position(symbol)

        if not isinstance(target_position, dict):
            return None

        close_amount = self._position_amount(target_position) if amount is None else abs(float(amount))
        if close_amount <= 0:
            return None

        side = self._position_side(target_position)
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
                position=position,
                position_side=position.get("position_side") or position.get("side"),
                position_id=position.get("position_id") or position.get("id"),
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

    async def fetch_open_orders_snapshot(self, symbols=None, limit=None):
        if not symbols:
            return await self.fetch_open_orders(limit=limit)

        snapshot = []
        seen = set()
        for symbol in dict.fromkeys(str(item).strip() for item in (symbols or []) if str(item).strip()):
            try:
                orders = await self.fetch_open_orders(symbol=symbol, limit=limit)
            except TypeError:
                orders = await self.fetch_open_orders(symbol)

            for order in orders or []:
                if isinstance(order, dict):
                    key = (
                        str(order.get("id") or ""),
                        str(order.get("clientOrderId") or ""),
                        str(order.get("symbol") or symbol),
                        str(order.get("status") or ""),
                    )
                else:
                    key = (str(order), "", symbol, "")
                if key in seen:
                    continue
                seen.add(key)
                snapshot.append(order)

        return snapshot

    async def fetch_closed_orders(self, symbol=None, limit=None):
        raise NotImplementedError("fetch_closed_orders is not implemented for this broker")

    async def fetch_symbol(self):
        raise NotImplementedError("fetch_symbol is not implemented for this broker")

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    def supported_market_venues(self):
        config = getattr(self, "config", None)
        return supported_market_venues_for_profile(
            getattr(config, "type", None),
            getattr(config, "exchange", None),
        )

    def apply_market_preference(self, preference=None):
        return []

    def supports_hedging(self):
        return bool(getattr(self, "hedging_supported", False))

    async def withdraw(self, code, amount, address, tag=None, params=None):
        raise NotImplementedError("withdraw is not implemented for this broker")

    async def fetch_deposit_address(self, code, params=None):
        raise NotImplementedError("fetch_deposit_address is not implemented for this broker")
