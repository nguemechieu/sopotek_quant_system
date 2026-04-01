import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import ccxt.async_support as ccxt
import jwt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from broker.ccxt_broker import CCXTBroker
from event_bus.event_bus import EventBus
from market_data.websocket.coinbase_web_socket import CoinbaseWebSocket


class FakeSession:
    def __init__(self, connector=None):
        self.connector = connector
        self.closed = False

    async def close(self):
        self.closed = True


class FakeCoinbaseExchange:
    def __init__(self, cfg):
        self.cfg = cfg
        self.closed = False
        self.sandbox_mode = None
        self.fetch_ticker_calls = []
        self.fetch_balance_calls = 0
        self.fetch_open_orders_calls = []
        self.has = {
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchOrderBook": True,
            "fetchOHLCV": True,
            "fetchTrades": True,
            "fetchMyTrades": True,
            "fetchStatus": True,
            "fetchOrders": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchOrder": True,
            "fetchBalance": True,
            "cancelOrder": True,
            "cancelAllOrders": True,
            "createOrder": True,
        }
        self.markets = {}
        self.currencies = {"USD": {"code": "USD"}}

    def set_sandbox_mode(self, enabled):
        self.sandbox_mode = enabled

    async def load_time_difference(self):
        return 0

    async def load_markets(self):
        self.markets = {
            "BTC/USD": {"symbol": "BTC/USD", "active": True},
            "ETH/USD": {"symbol": "ETH/USD", "active": True},
        }
        return self.markets

    async def fetch_ticker(self, symbol):
        self.fetch_ticker_calls.append(symbol)
        return {"symbol": symbol, "last": 65000.0, "bid": 64999.0, "ask": 65001.0}

    async def fetch_tickers(self, symbols=None):
        return {symbol: {"symbol": symbol, "last": 1 + idx} for idx, symbol in enumerate(symbols or [])}

    async def fetch_order_book(self, symbol, limit=100):
        return {"symbol": symbol, "bids": [[64999.0, 1.0]], "asks": [[65001.0, 1.5]], "limit": limit}

    async def fetch_ohlcv(self, symbol, timeframe="1h", since=None, limit=100, params=None):
        return [[1710000000000 + i, 1, 2, 0.5, 1.5, 10] for i in range(limit)]

    async def fetch_trades(self, symbol, limit=None):
        return [{"symbol": symbol, "limit": limit}]

    async def fetch_my_trades(self, symbol=None, limit=None):
        return [{"symbol": symbol, "limit": limit, "private": True}]

    async def fetch_status(self):
        return {"status": "ok"}

    async def create_order(self, symbol, order_type, side, amount, price, params):
        return {
            "id": "cb-1",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "status": "open",
            "params": params,
        }

    async def cancel_order(self, order_id, symbol=None):
        return {"id": order_id, "symbol": symbol, "status": "canceled"}

    async def cancel_all_orders(self, symbol=None):
        return [{"symbol": symbol, "status": "canceled"}]

    async def fetch_balance(self):
        self.fetch_balance_calls += 1
        return {"free": {"USD": 500.0, "BTC": 0.25}}

    async def fetch_order(self, order_id, symbol=None):
        return {"id": order_id, "symbol": symbol, "status": "filled", "filled": 0.01, "price": 65000.0}

    async def fetch_orders(self, symbol=None, limit=None):
        return [{"id": "cb-1", "symbol": symbol, "limit": limit}]

    async def fetch_open_orders(self, symbol=None, limit=None):
        self.fetch_open_orders_calls.append({"symbol": symbol, "limit": limit})
        return [{"id": "cb-1", "symbol": symbol, "limit": limit, "status": "open"}]

    async def fetch_closed_orders(self, symbol=None, limit=None):
        return [{"id": "cb-2", "symbol": symbol, "limit": limit, "status": "closed"}]

    def amount_to_precision(self, symbol, amount):
        return f"{amount:.8f}"

    def price_to_precision(self, symbol, price):
        return f"{price:.2f}"

    async def close(self):
        self.closed = True


class FakeSocket:
    def __init__(self, messages):
        self._messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, payload):
        self.sent = payload

    async def recv(self):
        if not self._messages:
            raise asyncio.CancelledError()
        return self._messages.pop(0)


class FakeCoinbaseHistoryExchange(FakeCoinbaseExchange):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.fetch_ohlcv_calls = []

    async def fetch_ohlcv(self, symbol, timeframe="1h", since=None, limit=100, params=None):
        params = dict(params or {})
        self.fetch_ohlcv_calls.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "since": since,
                "limit": limit,
                "params": params,
            }
        )
        timeframe_seconds = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }.get(timeframe, 3600)
        if since is None:
            count = min(int(limit or 5), 5)
            start_seconds = 1_710_000_000
        else:
            start_seconds = int(params.get("start") or int(since / 1000))
            end_seconds = int(params.get("end") or (start_seconds + (timeframe_seconds * int(limit or 300))))
            count = min(max(int((end_seconds - start_seconds) / timeframe_seconds), 0), 300)
        return [
            [(start_seconds + (index * timeframe_seconds)) * 1000, 1, 2, 0.5, 1.5, 10]
            for index in range(count)
        ]


class FakeCoinbaseCrossValuationExchange(FakeCoinbaseExchange):
    async def load_markets(self):
        self.markets = {
            "AAVE/EUR": {"symbol": "AAVE/EUR", "active": True},
            "BTC/EUR": {"symbol": "BTC/EUR", "active": True},
            "BTC/USD": {"symbol": "BTC/USD", "active": True},
        }
        return self.markets

    async def fetch_ticker(self, symbol):
        self.fetch_ticker_calls.append(symbol)
        prices = {
            "AAVE/EUR": {"last": 100.0, "bid": 99.5, "ask": 100.5},
            "BTC/EUR": {"last": 50000.0, "bid": 49950.0, "ask": 50050.0},
            "BTC/USD": {"last": 65000.0, "bid": 64950.0, "ask": 65050.0},
        }
        quote = prices[str(symbol)]
        return {"symbol": symbol, **quote}

    async def fetch_balance(self):
        return {"total": {"EUR": 1000.0, "AAVE": 2.0}}


class FakeStrictCoinbaseExchange(FakeCoinbaseExchange):
    async def load_markets(self):
        self.markets = {
            "BTC/USD": {"symbol": "BTC/USD", "active": True},
            "ETH/USD": {"symbol": "ETH/USD", "active": True},
        }
        return self.markets

    async def fetch_ticker(self, symbol):
        if symbol not in self.markets:
            raise ccxt.BadSymbol(f"coinbase does not have market symbol {symbol}")
        return await super().fetch_ticker(symbol)

    async def fetch_order_book(self, symbol, limit=100):
        if symbol not in self.markets:
            raise ccxt.BadSymbol(f"coinbase does not have market symbol {symbol}")
        return await super().fetch_order_book(symbol, limit=limit)

    async def fetch_ohlcv(self, symbol, timeframe="1h", since=None, limit=100, params=None):
        if symbol not in self.markets:
            raise ccxt.BadSymbol(f"coinbase does not have market symbol {symbol}")
        return await super().fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit, params=params)

    async def fetch_trades(self, symbol, limit=None):
        if symbol not in self.markets:
            raise ccxt.BadSymbol(f"coinbase does not have market symbol {symbol}")
        return await super().fetch_trades(symbol, limit=limit)


class FakeBuggyCoinbaseCreateOrderExchange(FakeCoinbaseExchange):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.id = "coinbase"

    async def create_order(self, symbol, order_type, side, amount, price, params):
        self.id = 123456789
        return await super().create_order(symbol, order_type, side, amount, price, params)


class FakeCoinbaseDerivativeExchange(FakeCoinbaseExchange):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.fetch_positions_calls = []
        self.has["fetchPositions"] = True

    async def load_markets(self):
        self.markets = {
            "BTC/USD:USD": {
                "symbol": "BTC/USD:USD",
                "active": True,
                "base": "BTC",
                "quote": "USD",
                "settle": "USD",
                "spot": False,
                "contract": True,
                "future": True,
            },
            "ETH/USD:USD": {
                "symbol": "ETH/USD:USD",
                "active": True,
                "base": "ETH",
                "quote": "USD",
                "settle": "USD",
                "spot": False,
                "contract": True,
                "future": True,
            },
        }
        return self.markets

    async def fetch_positions(self, symbols=None):
        self.fetch_positions_calls.append(symbols)
        return [{"symbol": "BTC/USD:USD", "contracts": 1.0, "side": "long"}]


def test_coinbase_ccxt_broker_supports_market_data_and_order_methods(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        config = SimpleNamespace(
            exchange="coinbase",
            api_key="key",
            secret="secret",
            password="passphrase",
            uid=None,
            mode="live",
            sandbox=False,
            timeout=15000,
            options={},
            params={"clientOrderId": "coinbase-client"},
        )
        broker = CCXTBroker(config)

        await broker.connect()

        assert broker.session.connector["resolver"] == "threaded-resolver"
        assert "BTC/USD" in await broker.fetch_symbols()
        assert broker.supported_market_venues() == ["auto", "spot", "derivative"]
        assert (await broker.fetch_ticker("BTC/USD"))["bid"] == 64999.0
        assert len(await broker.fetch_ohlcv("BTC/USD", limit=3)) == 3
        assert (await broker.fetch_orderbook("BTC/USD"))["asks"][0][0] == 65001.0
        assert (await broker.fetch_balance())["free"]["USD"] == 500.0
        assert (await broker.fetch_open_orders("BTC/USD", limit=5))[0]["status"] == "open"

        order = await broker.create_order(
            symbol="BTC/USD",
            side="buy",
            amount=0.010000123,
            type="limit",
            price=65000.129,
            params={"timeInForce": "GTC"},
        )
        assert order["amount"] == 0.01000012
        assert order["price"] == 65000.13
        assert order["params"]["clientOrderId"] == "coinbase-client"
        assert order["params"]["timeInForce"] == "GTC"

        stop_limit_order = await broker.create_order(
            symbol="BTC/USD",
            side="buy",
            amount=0.01,
            type="stop_limit",
            price=64950.12,
            stop_price=65010.0,
        )
        assert stop_limit_order["type"] == "stop_limit"
        assert stop_limit_order["stop_price"] == 65010.0
        assert stop_limit_order["params"]["stopPrice"] == 65010.0

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_restores_exchange_id_after_create_order(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeBuggyCoinbaseCreateOrderExchange, raising=False)

    async def scenario():
        broker = CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()
        assert broker.exchange.id == "coinbase"

        order = await broker.create_order(
            symbol="BTC/USD",
            side="buy",
            amount=0.01,
            type="limit",
            price=65000.0,
        )

        assert order["id"] == "cb-1"
        assert broker.exchange.id == "coinbase"

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_skips_unsupported_symbols(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeStrictCoinbaseExchange, raising=False)

    async def scenario():
        broker = CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()

        assert broker.supports_symbol("BTC/USD") is True
        assert broker.supports_symbol("EUR/USD") is False
        assert await broker.fetch_ticker("EUR/USD") is None
        assert await broker.fetch_orderbook("EUR/USD") == {"bids": [], "asks": []}
        assert await broker.fetch_ohlcv("EUR/USD", timeframe="1h", limit=50) == []
        assert await broker.fetch_trades("EUR/USD", limit=20) == []

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_derivative_mode_uses_native_positions(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseDerivativeExchange, raising=False)

    async def scenario():
        broker = CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={"market_type": "derivative", "defaultSubType": "future"},
                params={},
            )
        )

        await broker.connect()
        positions = await broker.fetch_positions()

        assert broker.exchange.cfg["options"]["defaultType"] == "future"
        assert broker.symbols == ["BTC/USD:USD", "ETH/USD:USD"]
        assert broker.supported_market_venues() == ["auto", "spot", "derivative"]
        assert broker._supports_positions_endpoint() is True
        assert positions[0]["symbol"] == "BTC/USD:USD"
        assert broker.exchange.fetch_positions_calls == [None]

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_derives_spot_positions_and_equity(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        broker = broker_mod.CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()

        balances = await broker.fetch_balance()
        positions = await broker.fetch_positions()

        assert balances["cash"] == 500.0
        assert balances["position_value"] == 16250.0
        assert balances["equity"] == 16750.0
        assert len(positions) == 1
        assert positions[0]["asset_code"] == "BTC"
        assert positions[0]["symbol"] == "BTC/USD"
        assert positions[0]["amount"] == 0.25
        assert positions[0]["value"] == 16250.0

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_caches_spot_account_snapshot_between_balance_and_positions(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        broker = broker_mod.CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()

        balances = await broker.fetch_balance()
        positions = await broker.fetch_positions()

        assert balances["equity"] == 16750.0
        assert positions[0]["symbol"] == "BTC/USD"
        assert broker.exchange.fetch_balance_calls == 1
        assert broker.exchange.fetch_ticker_calls == ["BTC/USD"]

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_caches_open_orders_snapshot(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        broker = broker_mod.CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()

        first = await broker.fetch_open_orders_snapshot(symbols=["BTC/USD"], limit=10)
        second = await broker.fetch_open_orders_snapshot(symbols=["BTC/USD"], limit=10)

        assert first == second
        assert broker.exchange.fetch_open_orders_calls == [{"symbol": None, "limit": 10}]

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_values_cash_and_assets_through_cross_pairs(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseCrossValuationExchange, raising=False)

    async def scenario():
        broker = broker_mod.CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()

        balances = await broker.fetch_balance()
        positions = await broker.fetch_positions()

        assert round(balances["cash"], 2) == 1300.0
        assert round(balances["position_value"], 2) == 260.0
        assert round(balances["equity"], 2) == 1560.0
        assert balances["asset_balances"]["EUR"] == 1000.0
        assert balances["asset_balances"]["AAVE"] == 2.0
        assert len(positions) == 1
        assert positions[0]["asset_code"] == "AAVE"
        assert positions[0]["symbol"] == "AAVE/EUR"
        assert round(positions[0]["value"], 2) == 260.0

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_normalizes_private_key_newlines(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        config = SimpleNamespace(
            exchange="coinbase",
            api_key="organizations/test/apiKeys/key-1",
            secret="-----BEGIN EC PRIVATE KEY-----\\nline-1\\nline-2\\n-----END EC PRIVATE KEY-----\\n",
            password=None,
            uid=None,
            mode="live",
            sandbox=False,
            timeout=15000,
            options={},
            params={},
        )
        broker = CCXTBroker(config)

        await broker.connect()

        assert broker.secret == "-----BEGIN EC PRIVATE KEY-----\nline-1\nline-2\n-----END EC PRIVATE KEY-----\n"
        assert broker.exchange.cfg["secret"] == broker.secret

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_normalizes_single_line_pem_from_ui(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        config = SimpleNamespace(
            exchange="coinbase",
            api_key='"organizations/test/apiKeys/key-1"',
            secret='"-----BEGIN EC PRIVATE KEY----- line-1 line-2 -----END EC PRIVATE KEY-----"',
            password=None,
            uid=None,
            mode="live",
            sandbox=False,
            timeout=15000,
            options={},
            params={},
        )
        broker = CCXTBroker(config)

        await broker.connect()

        assert broker.api_key == "organizations/test/apiKeys/key-1"
        assert broker.secret == "-----BEGIN EC PRIVATE KEY-----\nline-1line-2\n-----END EC PRIVATE KEY-----\n"
        assert broker.exchange.cfg["secret"] == broker.secret

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_accepts_uuid_id_and_json_private_key_bundle(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(broker_mod.aiohttp, "ClientSession", lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs))
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseExchange, raising=False)

    async def scenario():
        config = SimpleNamespace(
            exchange="coinbase",
            api_key="",
            secret='{"id":"2ffe3f58-d600-47a8-a147-1c55854eddc8","privateKey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}',
            password=None,
            uid=None,
            mode="live",
            sandbox=False,
            timeout=15000,
            options={},
            params={},
        )
        broker = CCXTBroker(config)

        await broker.connect()

        assert broker.api_key == "2ffe3f58-d600-47a8-a147-1c55854eddc8"
        assert broker.secret == "-----BEGIN EC PRIVATE KEY-----\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n-----END EC PRIVATE KEY-----\n"
        assert broker.exchange.cfg["secret"] == broker.secret

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_ccxt_broker_signs_private_requests_with_bearer_jwt():
    import broker.ccxt_broker as broker_mod

    broker = broker_mod.CCXTBroker(
        SimpleNamespace(
            exchange="coinbase",
            api_key="organizations/test/apiKeys/key-1",
            secret=(
                "-----BEGIN EC PRIVATE KEY-----\n"
                "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                "-----END EC PRIVATE KEY-----\n"
            ),
            password=None,
            uid=None,
            mode="live",
            sandbox=False,
            timeout=15000,
            options={},
            params={},
        )
    )
    broker._normalize_credentials()
    exchange_class = broker._exchange_class()
    exchange = exchange_class({"apiKey": broker.api_key, "secret": broker.secret, "options": {}})

    signed = exchange.sign("brokerage/accounts", api=["v3", "private"], method="GET", params={})

    assert "Authorization" in signed["headers"]
    assert "CB-ACCESS-KEY" not in signed["headers"]
    token = signed["headers"]["Authorization"].split(" ", 1)[1]
    headers = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False})

    assert headers["kid"] == "organizations/test/apiKeys/key-1"
    assert payload["sub"] == "organizations/test/apiKeys/key-1"
    assert payload["iss"] == "cdp"
    assert payload["uri"] == "GET api.coinbase.com/api/v3/brokerage/accounts"


def test_coinbase_ccxt_broker_backfills_requested_ohlcv_limit(monkeypatch):
    import broker.ccxt_broker as broker_mod

    monkeypatch.setattr(
        broker_mod.aiohttp,
        "TCPConnector",
        lambda family=None, resolver=None, ttl_dns_cache=None: {
            "family": family,
            "resolver": resolver,
            "ttl_dns_cache": ttl_dns_cache,
        },
    )
    monkeypatch.setattr(broker_mod.aiohttp, "ThreadedResolver", lambda: "threaded-resolver")
    monkeypatch.setattr(
        broker_mod.aiohttp,
        "ClientSession",
        lambda connector=None, **kwargs: FakeSession(connector=connector, **kwargs),
    )
    monkeypatch.setattr(broker_mod.ccxt, "coinbase", FakeCoinbaseHistoryExchange, raising=False)

    async def scenario():
        broker = broker_mod.CCXTBroker(
            SimpleNamespace(
                exchange="coinbase",
                api_key="organizations/test/apiKeys/key-1",
                secret=(
                    "-----BEGIN EC PRIVATE KEY-----\n"
                    "MHcCAQEEIAqSV4qAfY1Nm0xd6k95EZ39suUWAuze5Vuhn671kB9OoAoGCCqGSM49\n"
                    "AwEHoUQDQgAEcgYO1ly0wyz23wipRFpoM6Oyvh6WB1wy9EB8PHhrNw5VSJsAqsb7\n"
                    "gc1E+mZ1HVX3H8eKNlw8GrQCQJsZ5ExllA==\n"
                    "-----END EC PRIVATE KEY-----\n"
                ),
                password=None,
                uid=None,
                mode="live",
                sandbox=False,
                timeout=15000,
                options={},
                params={},
            )
        )

        await broker.connect()
        candles_240 = await broker.fetch_ohlcv("BTC/USD", timeframe="1h", limit=240)
        candles_500 = await broker.fetch_ohlcv("BTC/USD", timeframe="1h", limit=500)
        cached_240 = await broker.fetch_ohlcv("BTC/USD", timeframe="1h", limit=240)

        assert len(candles_240) == 240
        assert len(candles_500) == 500
        assert len(cached_240) == 240
        assert all(call["since"] is not None for call in broker.exchange.fetch_ohlcv_calls)
        assert any(int(call["limit"]) == 300 for call in broker.exchange.fetch_ohlcv_calls)
        assert len([call for call in broker.exchange.fetch_ohlcv_calls if int(call["limit"]) == 240]) == 1

        await broker.close()

    asyncio.run(scenario())


def test_coinbase_websocket_normalizes_product_ids_to_app_symbols(monkeypatch):
    import market_data.websocket.coinbase_web_socket as ws_mod

    payload = json.dumps(
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "65000.10",
            "best_bid": "64999.50",
            "best_ask": "65000.50",
            "volume_24h": "120.5",
            "time": "2026-03-10T10:00:00Z",
        }
    )
    monkeypatch.setattr(ws_mod.websockets, "connect", lambda url: FakeSocket([payload]))

    async def scenario():
        bus = EventBus()
        client = CoinbaseWebSocket(symbols=["BTC-USD"], event_bus=bus)

        try:
            await client.connect()
        except asyncio.CancelledError:
            pass

        event = await bus.queue.get()
        assert event.data["symbol"] == "BTC/USD"
        assert event.data["bid"] == 64999.5
        assert event.data["ask"] == 65000.5

    asyncio.run(scenario())
