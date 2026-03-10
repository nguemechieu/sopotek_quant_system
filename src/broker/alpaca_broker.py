import logging

from broker.base_broker import BaseBroker

try:
    import alpaca_trade_api as tradeapi
except Exception:  # pragma: no cover - optional dependency at runtime
    tradeapi = None


class AlpacaBroker(BaseBroker):
    TIMEFRAME_MAP = {
        "1m": "1Min",
        "5m": "5Min",
        "15m": "15Min",
        "30m": "30Min",
        "1h": "1Hour",
        "4h": "4Hour",
        "1d": "1Day",
        "1w": "1Week",
    }

    def __init__(self, config):
        super().__init__()

        self.logger = logging.getLogger("AlpacaBroker")
        self.config = config

        self.api_key = getattr(config, "api_key", None)
        self.secret = getattr(config, "secret", None)
        self.mode = (getattr(config, "mode", "paper") or "paper").lower()
        self.paper = bool(getattr(config, "sandbox", False) or self.mode in {"paper", "sandbox", "testnet"})
        self.base_url = "https://paper-api.alpaca.markets" if self.paper else "https://api.alpaca.markets"

        self.api = None
        self._connected = False

        if not self.api_key:
            raise ValueError("Alpaca API key is required")
        if not self.secret:
            raise ValueError("Alpaca secret is required")

    # =================================
    # INTERNALS
    # =================================

    def _ensure_api(self):
        if self.api is not None:
            return

        if tradeapi is None:
            raise RuntimeError("alpaca_trade_api is not installed")

        self.api = tradeapi.REST(
            self.api_key,
            self.secret,
            self.base_url,
            api_version="v2",
        )

    async def _ensure_connected(self):
        if not self._connected:
            await self.connect()

    def _normalize_order(self, order):
        if order is None:
            return None
        return {
            "id": getattr(order, "id", None),
            "symbol": getattr(order, "symbol", None),
            "side": getattr(order, "side", None),
            "type": getattr(order, "type", None),
            "status": getattr(order, "status", None),
            "amount": float(getattr(order, "qty", 0) or 0),
            "filled": float(getattr(order, "filled_qty", 0) or 0),
            "price": float(getattr(order, "limit_price", 0) or getattr(order, "filled_avg_price", 0) or 0),
            "raw": order,
        }

    def _normalize_timeframe(self, timeframe):
        return self.TIMEFRAME_MAP.get(str(timeframe or "1h").lower(), timeframe or "1Hour")

    # =================================
    # CONNECT
    # =================================

    async def connect(self):
        self._ensure_api()
        account = self.api.get_account()
        self._connected = True
        self.logger.info("Connected to Alpaca (%s)", getattr(account, "status", "unknown"))
        return True

    async def close(self):
        self._connected = False
        if self.api is not None and hasattr(self.api, "close"):
            self.api.close()

    # =================================
    # MARKET DATA
    # =================================

    async def fetch_ticker(self, symbol):
        await self._ensure_connected()

        trade = self.api.get_latest_trade(symbol)
        quote = self.api.get_latest_quote(symbol)

        return {
            "symbol": symbol,
            "bid": float(getattr(quote, "bid_price", 0) or 0),
            "ask": float(getattr(quote, "ask_price", 0) or 0),
            "last": float(getattr(trade, "price", 0) or 0),
        }

    async def fetch_orderbook(self, symbol, limit=10):
        ticker = await self.fetch_ticker(symbol)
        return {
            "symbol": symbol,
            "bids": [[ticker["bid"], 0.0]] if ticker["bid"] else [],
            "asks": [[ticker["ask"], 0.0]] if ticker["ask"] else [],
        }

    async def fetch_ohlcv(self, symbol, timeframe="1Hour", limit=100):
        await self._ensure_connected()

        bars = self.api.get_bars(symbol, self._normalize_timeframe(timeframe), limit=limit)
        data = []
        for bar in bars:
            data.append(
                [
                    getattr(bar, "t", None),
                    float(getattr(bar, "o", 0) or 0),
                    float(getattr(bar, "h", 0) or 0),
                    float(getattr(bar, "l", 0) or 0),
                    float(getattr(bar, "c", 0) or 0),
                    float(getattr(bar, "v", 0) or 0),
                ]
            )
        return data

    async def fetch_symbol(self):
        await self._ensure_connected()
        assets = self.api.list_assets(status="active")
        return [asset.symbol for asset in assets if getattr(asset, "tradable", True)]

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    async def fetch_status(self):
        await self._ensure_connected()
        account = self.api.get_account()
        return {"status": getattr(account, "status", "unknown"), "broker": "alpaca"}

    # =================================
    # ORDERS
    # =================================

    async def create_order(self, symbol, side, amount, type="market", price=None, params=None, stop_loss=None, take_profit=None):
        await self._ensure_connected()

        params = dict(params or {})
        time_in_force = params.pop("time_in_force", "gtc")

        order_kwargs = {
            "symbol": symbol,
            "qty": amount,
            "side": str(side).lower(),
            "type": type,
            "time_in_force": time_in_force,
        }
        if price is not None and type != "market":
            order_kwargs["limit_price"] = price
        if stop_loss is not None or take_profit is not None:
            order_kwargs["order_class"] = params.pop("order_class", "bracket")
            if take_profit is not None:
                order_kwargs["take_profit"] = {"limit_price": float(take_profit)}
            if stop_loss is not None:
                order_kwargs["stop_loss"] = {"stop_price": float(stop_loss)}
        order_kwargs.update(params)

        order = self.api.submit_order(**order_kwargs)
        return self._normalize_order(order)

    async def cancel_order(self, order_id, symbol=None):
        await self._ensure_connected()
        return self.api.cancel_order(order_id)

    async def cancel_all_orders(self, symbol=None):
        await self._ensure_connected()
        if hasattr(self.api, "cancel_all_orders"):
            return self.api.cancel_all_orders()
        return []

    async def fetch_order(self, order_id, symbol=None):
        await self._ensure_connected()
        order = self.api.get_order(order_id)
        normalized = self._normalize_order(order)
        if symbol is None or normalized["symbol"] == symbol:
            return normalized
        return None

    async def fetch_orders(self, symbol=None, limit=None):
        await self._ensure_connected()
        orders = self.api.list_orders(status="all", limit=limit)
        normalized = [self._normalize_order(order) for order in orders]
        if symbol is None:
            return normalized
        return [order for order in normalized if order["symbol"] == symbol]

    async def fetch_open_orders(self, symbol=None, limit=None):
        await self._ensure_connected()
        orders = self.api.list_orders(status="open", limit=limit)
        normalized = [self._normalize_order(order) for order in orders]
        if symbol is None:
            return normalized
        return [order for order in normalized if order["symbol"] == symbol]

    async def fetch_closed_orders(self, symbol=None, limit=None):
        orders = await self.fetch_orders(symbol=symbol, limit=limit)
        return [
            order for order in orders
            if order.get("status") not in {"new", "accepted", "pending_new", "partially_filled"}
        ]

    # =================================
    # ACCOUNT
    # =================================

    async def fetch_balance(self):
        await self._ensure_connected()
        account = self.api.get_account()

        cash = float(getattr(account, "cash", 0) or 0)
        equity = float(getattr(account, "equity", cash) or cash)
        buying_power = float(getattr(account, "buying_power", cash) or cash)
        used = max(buying_power - cash, 0.0)

        return {
            "equity": equity,
            "cash": cash,
            "free": {"USD": cash},
            "used": {"USD": used},
            "total": {"USD": equity},
            "raw": account,
        }

    async def fetch_positions(self, symbols=None):
        await self._ensure_connected()
        positions = self.api.list_positions()
        target = set(symbols or [])
        normalized = []
        for position in positions:
            symbol = getattr(position, "symbol", None)
            if target and symbol not in target:
                continue
            qty = float(getattr(position, "qty", 0) or 0)
            normalized.append(
                {
                    "symbol": symbol,
                    "amount": abs(qty),
                    "side": "long" if qty >= 0 else "short",
                    "entry_price": float(getattr(position, "avg_entry_price", 0) or 0),
                    "market_value": float(getattr(position, "market_value", 0) or 0),
                    "raw": position,
                }
            )
        return normalized
