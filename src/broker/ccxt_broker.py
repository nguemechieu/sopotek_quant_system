import logging
import socket

import aiohttp
import ccxt.async_support as ccxt

from broker.base_broker import BaseBroker


class CCXTBroker(BaseBroker):
    DEFAULT_TIMEOUT_MS = 30000
    CAPABILITY_MAP = {
        "fetch_ticker": "fetchTicker",
        "fetch_tickers": "fetchTickers",
        "fetch_order_book": "fetchOrderBook",
        "fetch_ohlcv": "fetchOHLCV",
        "fetch_trades": "fetchTrades",
        "fetch_my_trades": "fetchMyTrades",
        "fetch_markets": "fetchMarkets",
        "fetch_currencies": "fetchCurrencies",
        "fetch_status": "fetchStatus",
        "create_order": "createOrder",
        "cancel_order": "cancelOrder",
        "cancel_all_orders": "cancelAllOrders",
        "fetch_balance": "fetchBalance",
        "fetch_positions": "fetchPositions",
        "fetch_order": "fetchOrder",
        "fetch_orders": "fetchOrders",
        "fetch_open_orders": "fetchOpenOrders",
        "fetch_closed_orders": "fetchClosedOrders",
        "withdraw": "withdraw",
        "fetch_deposit_address": "fetchDepositAddress",
    }

    def __init__(self, config):
        super().__init__()

        self.logger = logging.getLogger("CCXTBroker")

        self.config = config
        self.exchange_name = getattr(config, "exchange", None)
        self.api_key = getattr(config, "api_key", None)
        self.secret = getattr(config, "secret", None)
        self.password = getattr(config, "password", None) or getattr(config, "passphrase", None)
        self.uid = getattr(config, "uid", None)
        self.account_id = getattr(config, "account_id", None)
        self.wallet = getattr(config, "wallet", None)
        self.mode = (getattr(config, "mode", "live") or "live").lower()
        self.sandbox = bool(getattr(config, "sandbox", False) or self.mode in {"paper", "sandbox", "testnet"})
        self.timeout = int(getattr(config, "timeout", self.DEFAULT_TIMEOUT_MS) or self.DEFAULT_TIMEOUT_MS)
        self.extra_options = dict(getattr(config, "options", None) or {})
        self.extra_params = dict(getattr(config, "params", None) or {})

        self.exchange = None
        self.session = None
        self.symbols = []
        self._connected = False

        if not self.exchange_name:
            raise ValueError("CCXT exchange name is required")

        self.logger.info("Initializing broker %s", self.exchange_name)

    # ==========================================================
    # INTERNALS
    # ==========================================================

    def _exchange_class(self):
        try:
            return getattr(ccxt, self.exchange_name)
        except AttributeError as exc:
            raise ValueError(f"Unsupported CCXT exchange: {self.exchange_name}") from exc

    def _build_exchange_options(self):
        options = {"adjustForTimeDifference": True}
        options.update(self.extra_options)
        return options

    def _build_exchange_config(self):
        cfg = {
            "enableRateLimit": True,
            "timeout": self.timeout,
            "options": self._build_exchange_options(),
        }

        if self.session is not None:
            cfg["session"] = self.session

        if self.api_key:
            cfg["apiKey"] = self.api_key
        if self.secret:
            cfg["secret"] = self.secret
        if self.password:
            cfg["password"] = self.password
        if self.uid:
            cfg["uid"] = self.uid
        if self.wallet:
            cfg["walletAddress"] = self.wallet

        if self.exchange_name.startswith("binance"):
            cfg["recvWindow"] = int(self.extra_options.get("recvWindow", 10000))

        return cfg

    async def _ensure_connected(self):
        if not self._connected:
            await self.connect()

    def _exchange_has(self, capability):
        exchange = self.exchange
        if exchange is None:
            return False

        has_key = self.CAPABILITY_MAP.get(capability, capability)
        has_map = getattr(exchange, "has", None)
        if isinstance(has_map, dict):
            supported = has_map.get(has_key)
            if supported in (True, "emulated"):
                return True
            if supported is False:
                return False

        return callable(getattr(exchange, capability, None))

    def _maybe_precision_amount(self, symbol, amount):
        if self.exchange is None or amount is None:
            return amount

        converter = getattr(self.exchange, "amount_to_precision", None)
        if callable(converter):
            try:
                return float(converter(symbol, amount))
            except Exception:
                return amount
        return amount

    def _maybe_precision_price(self, symbol, price):
        if self.exchange is None or price is None:
            return price

        converter = getattr(self.exchange, "price_to_precision", None)
        if callable(converter):
            try:
                return float(converter(symbol, price))
            except Exception:
                return price
        return price

    async def _call_unified(self, method_name, *args, default=None, **kwargs):
        await self._ensure_connected()

        method = getattr(self.exchange, method_name, None)
        if not callable(method):
            if default is not None:
                return default
            raise NotImplementedError(
                f"{self.exchange_name} does not expose {method_name}"
            )

        if not self._exchange_has(method_name):
            if default is not None:
                return default
            raise NotImplementedError(
                f"{self.exchange_name} does not support {method_name}"
            )

        return await method(*args, **kwargs)

    # ==========================================================
    # CONNECT
    # ==========================================================

    async def connect(self):
        if self._connected:
            return

        exchange_class = self._exchange_class()

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        self.session = aiohttp.ClientSession(connector=connector)
        self.exchange = exchange_class(self._build_exchange_config())

        try:
            if hasattr(self.exchange, "set_sandbox_mode"):
                self.exchange.set_sandbox_mode(self.sandbox)

            if callable(getattr(self.exchange, "load_time_difference", None)):
                await self.exchange.load_time_difference()

            await self.exchange.load_markets()
            self.symbols = sorted((getattr(self.exchange, "markets", {}) or {}).keys())
            self._connected = True
        except Exception:
            await self.close()
            raise

    async def close(self):
        errors = []

        if self.exchange is not None:
            try:
                await self.exchange.close()
            except Exception as exc:
                errors.append(exc)

        if self.session is not None:
            try:
                await self.session.close()
            except Exception as exc:
                errors.append(exc)

        self.exchange = None
        self.session = None
        self.symbols = []
        self._connected = False

        if errors:
            self.logger.warning("Broker close encountered %s issue(s)", len(errors))

    # ==========================================================
    # DISCOVERY
    # ==========================================================

    async def fetch_symbol(self):
        await self._ensure_connected()
        return list(self.symbols)

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    async def fetch_markets(self):
        await self._ensure_connected()
        markets = getattr(self.exchange, "markets", None)
        if isinstance(markets, dict) and markets:
            return markets
        return await self._call_unified("fetch_markets", default={})

    async def fetch_currencies(self):
        await self._ensure_connected()
        currencies = getattr(self.exchange, "currencies", None)
        if isinstance(currencies, dict) and currencies:
            return currencies
        return await self._call_unified("fetch_currencies", default={})

    async def fetch_status(self):
        if not self._connected:
            return {"status": "disconnected"}

        if self._exchange_has("fetchStatus"):
            return await self._call_unified("fetch_status")

        return {"status": "ok", "exchange": self.exchange_name}

    # ==========================================================
    # MARKET DATA
    # ==========================================================

    async def fetch_ticker(self, symbol):
        return await self._call_unified("fetch_ticker", symbol)

    async def fetch_tickers(self, symbols=None):
        return await self._call_unified("fetch_tickers", symbols, default={})

    async def fetch_orderbook(self, symbol, limit=100):
        return await self._call_unified("fetch_order_book", symbol, limit)

    async def fetch_order_book(self, symbol, limit=100):
        return await self.fetch_orderbook(symbol, limit=limit)

    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
        self.logger.info("Fetching OHLCV for %s", symbol)
        return await self._call_unified(
            "fetch_ohlcv",
            symbol,
            timeframe=timeframe,
            limit=limit,
            default=[],
        )

    async def fetch_trades(self, symbol, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_trades", symbol, default=[], **kwargs)

    async def fetch_my_trades(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_my_trades", symbol, default=[], **kwargs)

    # ==========================================================
    # TRADING
    # ==========================================================

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
        await self._ensure_connected()

        normalized_amount = self._maybe_precision_amount(symbol, float(amount))
        normalized_price = self._maybe_precision_price(symbol, price)
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        if stop_loss is not None:
            order_params.setdefault("stopLossPrice", stop_loss)
        if take_profit is not None:
            order_params.setdefault("takeProfitPrice", take_profit)

        if not self._exchange_has("create_order"):
            raise NotImplementedError(f"{self.exchange_name} does not support create_order")

        return await self.exchange.create_order(
            symbol,
            type,
            str(side).lower(),
            normalized_amount,
            normalized_price,
            order_params,
        )

    async def cancel_order(self, order_id, symbol=None):
        if symbol is None:
            return await self._call_unified("cancel_order", order_id)
        return await self._call_unified("cancel_order", order_id, symbol)

    async def cancel_all_orders(self, symbol=None):
        if symbol is None:
            return await self._call_unified("cancel_all_orders", default=[])
        return await self._call_unified("cancel_all_orders", symbol, default=[])

    # ==========================================================
    # ACCOUNT
    # ==========================================================

    async def fetch_balance(self):
        return await self._call_unified("fetch_balance")

    async def fetch_positions(self, symbols=None):
        return await self._call_unified("fetch_positions", symbols, default=[])

    async def fetch_order(self, order_id, symbol=None):
        if symbol is None:
            return await self._call_unified("fetch_order", order_id)
        return await self._call_unified("fetch_order", order_id, symbol)

    async def fetch_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_orders", symbol, default=[], **kwargs)

    async def fetch_open_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_open_orders", symbol, default=[], **kwargs)

    async def fetch_closed_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_closed_orders", symbol, default=[], **kwargs)

    async def withdraw(self, code, amount, address, tag=None, params=None):
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        if tag is not None:
            order_params.setdefault("tag", tag)
        return await self._call_unified(
            "withdraw",
            code,
            amount,
            address,
            tag,
            order_params,
        )

    async def fetch_deposit_address(self, code, params=None):
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        return await self._call_unified(
            "fetch_deposit_address",
            code,
            order_params,
        )
