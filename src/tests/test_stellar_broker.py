import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from broker.broker_factory import BrokerFactory
from broker.stellar_broker import StellarBroker
from config.config import AppConfig, BrokerConfig, RiskConfig, SystemConfig

TEST_PUBLIC_KEY = "G" + ("A" * 55)
TEST_SECRET_KEY = "S" + ("B" * 55)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


class FakeRateLimitedResponse(FakeResponse):
    def __init__(self, payload, url):
        super().__init__(payload)
        self.url = url

    def raise_for_status(self):
        raise aiohttp.ClientResponseError(
            request_info=SimpleNamespace(real_url=self.url),
            history=(),
            status=429,
            message="Too Many Requests",
            headers={"Retry-After": "0"},
        )


class FakeStellarSession:
    def __init__(self):
        self.closed = False
        self.last_trade_aggregations_params = None

    def request(self, method, url, params=None, json=None):
        if url.endswith(f"/accounts/{TEST_PUBLIC_KEY}"):
            return FakeResponse(
                {
                    "id": TEST_PUBLIC_KEY,
                    "balances": [
                        {"asset_type": "native", "balance": "100.0", "selling_liabilities": "10.0"},
                        {
                            "asset_type": "credit_alphanum4",
                            "asset_code": "USDC",
                            "asset_issuer": "GUSDC",
                            "balance": "250.0",
                            "selling_liabilities": "25.0",
                        },
                    ],
                }
            )

        if url.endswith("/order_book"):
            return FakeResponse(
                {
                    "bids": [{"price": "0.0990", "amount": "120.0"}],
                    "asks": [{"price": "0.1010", "amount": "100.0"}],
                }
            )

        if url.endswith("/trades"):
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "price": {"n": 101, "d": 1000},
                                "base_amount": "10.0",
                                "counter_amount": "1.01",
                                "base_asset_type": "native",
                                "counter_asset_type": "credit_alphanum4",
                                "counter_asset_code": "USDC",
                            }
                        ]
                    }
                }
            )

        if url.endswith("/trade_aggregations"):
            self.last_trade_aggregations_params = dict(params or {})
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "timestamp": 1700000000000,
                                "open": "0.098",
                                "high": "0.102",
                                "low": "0.097",
                                "close": "0.101",
                                "base_volume": "500.0",
                            }
                        ]
                    }
                }
            )

        if url.endswith(f"/accounts/{TEST_PUBLIC_KEY}/offers"):
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "id": "777",
                                "price": "0.1000000",
                                "amount": "10.0",
                                "selling": {
                                    "asset_type": "credit_alphanum4",
                                    "asset_code": "USDC",
                                    "asset_issuer": "GUSDC",
                                },
                                "buying": {"asset_type": "native"},
                            }
                        ]
                    }
                }
            )

        if url.endswith(f"/accounts/{TEST_PUBLIC_KEY}/trades"):
            return FakeResponse({"_embedded": {"records": []}})

        if url.endswith("/assets"):
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "asset_code": "AQUA",
                                "asset_issuer": "GAQUA",
                                "num_accounts": 900,
                                "accounts": {"authorized": 850},
                            },
                            {
                                "asset_code": "USDC2",
                                "asset_issuer": "GUSDC2",
                                "num_accounts": 999,
                                "accounts": {"authorized": 990},
                            },
                            {
                                "asset_code": "yXLM",
                                "asset_issuer": "GYXLM",
                                "num_accounts": 650,
                                "accounts": {"authorized": 640},
                            },
                        ]
                    }
                }
            )

        if url.rstrip("/") == "https://horizon-testnet.stellar.org":
            return FakeResponse({"core_latest_ledger": 1})

        raise AssertionError(f"Unhandled Stellar URL: {method} {url}")

    async def close(self):
        self.closed = True


class FakeRateLimitedAccountSession(FakeStellarSession):
    def request(self, method, url, params=None, json=None):
        if url.endswith(f"/accounts/{TEST_PUBLIC_KEY}"):
            return FakeRateLimitedResponse({}, url)
        return super().request(method, url, params=params, json=json)


class FakePagedAssetSession(FakeStellarSession):
    def request(self, method, url, params=None, json=None):
        if url.endswith("/assets"):
            cursor = (params or {}).get("cursor")
            if not cursor:
                return FakeResponse(
                    {
                        "_links": {
                            "next": {
                                "href": "https://horizon.stellar.org/assets?cursor=page-2&limit=10&order=desc"
                            }
                        },
                        "_embedded": {
                            "records": [
                                {
                                    "asset_code": "0",
                                    "asset_issuer": "GZEROASSETAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                    "num_accounts": 1000,
                                    "accounts": {"authorized": 999},
                                    "balances": {"authorized": "1000.0000000"},
                                    "liquidity_pools_amount": "10.0000000",
                                },
                                {
                                    "asset_code": "000001",
                                    "asset_issuer": "GNUMERICASSETAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                    "num_accounts": 500,
                                    "accounts": {"authorized": 500},
                                    "balances": {"authorized": "500.0000000"},
                                    "liquidity_pools_amount": "5.0000000",
                                },
                            ]
                        },
                    }
                )
            return FakeResponse(
                {
                    "_links": {},
                    "_embedded": {
                        "records": [
                            {
                                "asset_code": "AQUA",
                                "asset_issuer": "GAQUA",
                                "num_accounts": 900,
                                "accounts": {"authorized": 850},
                                "balances": {"authorized": "1000000.0000000"},
                                "liquidity_pools_amount": "1000.0000000",
                            },
                            {
                                "asset_code": "USDC",
                                "asset_issuer": "GUSDC",
                                "num_accounts": 1200,
                                "accounts": {"authorized": 1150},
                                "balances": {"authorized": "2500000.0000000"},
                                "liquidity_pools_amount": "2500.0000000",
                            },
                        ]
                    },
                }
            )
        return super().request(method, url, params=params, json=json)


class FakeTrustedAssetSession(FakeStellarSession):
    def request(self, method, url, params=None, json=None):
        if url.endswith(f"/accounts/{TEST_PUBLIC_KEY}"):
            return FakeResponse(
                {
                    "id": TEST_PUBLIC_KEY,
                    "balances": [
                        {"asset_type": "native", "balance": "100.0", "selling_liabilities": "10.0"},
                        {
                            "asset_type": "credit_alphanum4",
                            "asset_code": "USDC",
                            "asset_issuer": "GUSDC",
                            "balance": "250.0",
                            "selling_liabilities": "25.0",
                        },
                        {
                            "asset_type": "credit_alphanum4",
                            "asset_code": "AIRT",
                            "asset_issuer": "GAIRT",
                            "balance": "75.0",
                            "selling_liabilities": "0.0",
                        },
                    ],
                }
            )
        if url.endswith("/assets"):
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "asset_code": "AQUA",
                                "asset_issuer": "GAQUA",
                                "num_accounts": 900,
                                "accounts": {"authorized": 850},
                            },
                            {
                                "asset_code": "YXLM",
                                "asset_issuer": "GYXLM",
                                "num_accounts": 650,
                                "accounts": {"authorized": 640},
                            },
                        ]
                    }
                }
            )
        return super().request(method, url, params=params, json=json)


class FakeOrderbook429AfterSuccessSession(FakeStellarSession):
    def __init__(self):
        super().__init__()
        self.orderbook_calls = 0

    def request(self, method, url, params=None, json=None):
        if url.endswith("/order_book"):
            self.orderbook_calls += 1
            if self.orderbook_calls == 1:
                return FakeResponse(
                    {
                        "bids": [{"price": "0.0990", "amount": "120.0"}],
                        "asks": [{"price": "0.1010", "amount": "100.0"}],
                    }
                )
            return FakeRateLimitedResponse({}, url)
        return super().request(method, url, params=params, json=json)


class FakeSparseOhlcvSession(FakeStellarSession):
    def request(self, method, url, params=None, json=None):
        if url.endswith("/trade_aggregations"):
            self.last_trade_aggregations_params = dict(params or {})
            return FakeResponse({"_embedded": {"records": []}})
        if url.endswith("/trades"):
            return FakeResponse(
                {
                    "_embedded": {
                        "records": [
                            {
                                "created_at": "2026-03-09T00:05:00Z",
                                "price": {"n": 100, "d": 1000},
                                "base_amount": "10.0",
                                "counter_amount": "1.0",
                            },
                            {
                                "created_at": "2026-03-09T12:30:00Z",
                                "price": {"n": 106, "d": 1000},
                                "base_amount": "15.0",
                                "counter_amount": "1.59",
                            },
                            {
                                "created_at": "2026-03-09T20:10:00Z",
                                "price": {"n": 98, "d": 1000},
                                "base_amount": "5.0",
                                "counter_amount": "0.49",
                            },
                            {
                                "created_at": "2026-03-09T23:45:00Z",
                                "price": {"n": 103, "d": 1000},
                                "base_amount": "20.0",
                                "counter_amount": "2.06",
                            },
                        ]
                    }
                }
            )
        return super().request(method, url, params=params, json=json)


class FakeRateLimitedOhlcvSession(FakeStellarSession):
    def __init__(self):
        super().__init__()
        self.trade_aggregation_calls = 0

    def request(self, method, url, params=None, json=None):
        if url.endswith("/trade_aggregations"):
            self.trade_aggregation_calls += 1
            if self.trade_aggregation_calls == 1:
                self.last_trade_aggregations_params = dict(params or {})
                return FakeResponse(
                    {
                        "_embedded": {
                            "records": [
                                {
                                    "timestamp": 1700000000000,
                                    "open": "0.098",
                                    "high": "0.102",
                                    "low": "0.097",
                                    "close": "0.101",
                                    "base_volume": "500.0",
                                }
                            ]
                        }
                    }
                )
            return FakeRateLimitedResponse({}, url)
        return super().request(method, url, params=params, json=json)


class FakeNoTradesTickerSession(FakeStellarSession):
    def __init__(self):
        super().__init__()
        self.trade_calls = 0

    def request(self, method, url, params=None, json=None):
        if url.endswith("/trades"):
            self.trade_calls += 1
            raise AssertionError("fetch_ticker should not request /trades for Stellar")
        return super().request(method, url, params=params, json=json)


class FakeTrades429AfterSuccessSession(FakeStellarSession):
    def __init__(self):
        super().__init__()
        self.trade_calls = 0

    def request(self, method, url, params=None, json=None):
        if url.endswith("/trades"):
            self.trade_calls += 1
            if self.trade_calls == 1:
                return super().request(method, url, params=params, json=json)
            return FakeRateLimitedResponse({}, url)
        return super().request(method, url, params=params, json=json)


class FakeTrades400Session(FakeStellarSession):
    def request(self, method, url, params=None, json=None):
        if url.endswith("/trades"):
            raise aiohttp.ClientResponseError(
                request_info=SimpleNamespace(real_url=url),
                history=(),
                status=400,
                message="Bad Request",
                headers={},
            )
        return super().request(method, url, params=params, json=json)


class FakeSdkAsset:
    def __init__(self, code, issuer=None):
        self.code = code
        self.issuer = issuer

    @classmethod
    def native(cls):
        return cls("XLM", None)


class FakeKeypair:
    @classmethod
    def from_secret(cls, secret):
        instance = cls()
        instance.secret = secret
        return instance


class FakeNetwork:
    TESTNET_NETWORK_PASSPHRASE = "TESTNET"
    PUBLIC_NETWORK_PASSPHRASE = "PUBLIC"


class FakeBuiltTransaction:
    def __init__(self, operations):
        self.operations = operations
        self.signed_with = None

    def sign(self, signer):
        self.signed_with = signer


class FakeTransactionBuilder:
    def __init__(self, source_account, network_passphrase, base_fee):
        self.source_account = source_account
        self.network_passphrase = network_passphrase
        self.base_fee = base_fee
        self.operations = []

    def append_manage_buy_offer_op(self, **kwargs):
        self.operations.append({"kind": "manage_buy", **kwargs})
        return self

    def append_manage_sell_offer_op(self, **kwargs):
        self.operations.append({"kind": "manage_sell", **kwargs})
        return self

    def set_timeout(self, seconds):
        self.timeout = seconds
        return self

    def build(self):
        return FakeBuiltTransaction(self.operations)


class FakeServerAsync:
    last_transaction = None

    def __init__(self, horizon_url=None, client=None):
        self.horizon_url = horizon_url
        self.client = client

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def load_account(self, public_key):
        return {"account_id": public_key}

    async def submit_transaction(self, transaction):
        FakeServerAsync.last_transaction = transaction
        return {"hash": "stellar-tx"}


def test_broker_factory_routes_stellar_exchange(monkeypatch):
    import broker.broker_factory as broker_factory_module

    monkeypatch.setattr(broker_factory_module, "StellarBroker", lambda cfg: ("stellar", cfg.exchange))

    config = AppConfig(
        broker=BrokerConfig(type="crypto", exchange="stellar", api_key=TEST_PUBLIC_KEY, secret=TEST_SECRET_KEY),
        risk=RiskConfig(),
        system=SystemConfig(),
        strategy="LSTM",
    )

    broker = BrokerFactory.create(config)
    assert broker == ("stellar", "stellar")


def test_broker_factory_rejects_binance_for_us_customers():
    config = AppConfig(
        broker=BrokerConfig(type="crypto", exchange="binance", customer_region="us", api_key="a", secret="b"),
        risk=RiskConfig(),
        system=SystemConfig(),
        strategy="LSTM",
    )

    try:
        BrokerFactory.create(config)
    except ValueError as exc:
        assert "Binance.com is not available for US customers" in str(exc)
    else:
        raise AssertionError("Expected US Binance jurisdiction validation to reject binance.com")


def test_broker_factory_rejects_binanceus_for_non_us_customers():
    config = AppConfig(
        broker=BrokerConfig(type="crypto", exchange="binanceus", customer_region="global", api_key="a", secret="b"),
        risk=RiskConfig(),
        system=SystemConfig(),
        strategy="LSTM",
    )

    try:
        BrokerFactory.create(config)
    except ValueError as exc:
        assert "Binance US is only available for US customers" in str(exc)
    else:
        raise AssertionError("Expected non-US Binance jurisdiction validation to reject Binance US")


def test_stellar_broker_rejects_invalid_public_key():
    try:
        StellarBroker(
            SimpleNamespace(
                api_key="5VnocTRZTCkGetIP6o5bvot7AS6rKvH9LzJegvzT33IeXlpGyDGdcZYxaRiws6RC",
                secret=TEST_SECRET_KEY,
                mode="paper",
                params={},
            )
        )
    except ValueError as exc:
        assert "Invalid Stellar public key" in str(exc)
    else:
        raise AssertionError("Expected invalid Stellar public key to be rejected")


def test_stellar_broker_allows_read_only_connect_without_secret(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeStellarSession)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"]},
            )
        )

        await broker.connect()
        symbols = await broker.fetch_symbols()
        assert "XLM/USDC" in symbols
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_rate_limited_account_lookup_uses_empty_snapshot(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeRateLimitedAccountSession)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "rate_limit_retries": 0},
            )
        )

        await broker.connect()
        balances = await broker.fetch_balance()
        symbols = await broker.fetch_symbols()

        assert balances["free"] == {}
        assert balances["used"] == {}
        assert balances["total"] == {}
        assert "AQUA/XLM" in symbols
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_uses_cached_assets_when_startup_is_rate_limited(monkeypatch, tmp_path):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeRateLimitedAccountSession)

    cache_path = tmp_path / "stellar_asset_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "network_asset_codes": ["XLM", "USDC", "AQUA"],
                "account_asset_codes": ["XLM", "USDC"],
                "asset_registry": [
                    {"code": "XLM", "issuer": None},
                    {"code": "USDC", "issuer": "GUSDC"},
                    {"code": "AQUA", "issuer": "GAQUA"},
                ],
            }
        ),
        encoding="utf-8",
    )

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={
                    "quote_assets": ["USDC", "XLM"],
                    "rate_limit_retries": 0,
                    "cache_path": str(cache_path),
                },
            )
        )

        await broker.connect()
        symbols = await broker.fetch_symbols()
        markets = await broker.fetch_markets()

        assert "AQUA/USDC" in symbols
        assert markets["AQUA/USDC"]["base_asset_issuer"] == "GAQUA"
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_discovers_valid_assets_after_junk_first_page(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakePagedAssetSession)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "asset_limit": 10, "asset_scan_pages": 3},
            )
        )

        await broker.connect()
        symbols = await broker.fetch_symbols()

        assert "AQUA/USDC" in symbols
        assert all(not any(char.isdigit() for char in symbol) for symbol in symbols)
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_includes_account_trustline_symbols(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeTrustedAssetSession)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "symbol_limit": 10},
            )
        )

        await broker.connect()
        symbols = await broker.fetch_symbols()
        markets = await broker.fetch_markets()

        assert "AIRT/USDC" in symbols
        assert markets["AIRT/USDC"]["base_asset_issuer"] == "GAIRT"
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_reuses_cached_orderbook_after_rate_limit(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeOrderbook429AfterSuccessSession)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "rate_limit_retries": 0},
            )
        )

        await broker.connect()
        first_book = await broker.fetch_orderbook("XLM/USDC", limit=1)
        second_book = await broker.fetch_orderbook("XLM/USDC", limit=1)

        assert first_book["bids"][0][0] == 0.099
        assert second_book == first_book
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_normalizes_market_data_and_orders(monkeypatch):
    import broker.stellar_broker as stellar_module

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", FakeStellarSession)
    monkeypatch.setattr(stellar_module, "Asset", FakeSdkAsset)
    monkeypatch.setattr(stellar_module, "Keypair", FakeKeypair)
    monkeypatch.setattr(stellar_module, "Network", FakeNetwork)
    monkeypatch.setattr(stellar_module, "TransactionBuilder", FakeTransactionBuilder)
    monkeypatch.setattr(stellar_module, "ServerAsync", FakeServerAsync)
    monkeypatch.setattr(stellar_module, "AiohttpClient", lambda: object())

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=TEST_SECRET_KEY,
                mode="paper",
                params={"quote_assets": ["USDC", "XLM"]},
            )
        )

        await broker.connect()

        symbols = await broker.fetch_symbols()
        markets = await broker.fetch_markets()
        assert "XLM/USDC" in symbols
        assert "AQUA/USDC" in symbols
        assert "YXLM/USDC" in symbols
        assert all(not any(char.isdigit() for char in symbol) for symbol in symbols)
        assert "USDC2/USDC" not in symbols
        assert "USDC/XLM" not in symbols
        assert markets["AQUA/USDC"]["base_asset_code"] == "AQUA"
        assert markets["AQUA/USDC"]["base_asset_issuer"] == "GAQUA"
        assert markets["AQUA/USDC"]["base_asset_type"] == "credit_alphanum4"
        assert markets["XLM/USDC"]["quote_asset_issuer"] == "GUSDC"
        assert markets["XLM/USDC"]["quote_asset_type"] == "credit_alphanum4"

        ticker = await broker.fetch_ticker("XLM/USDC")
        assert ticker["bid"] == 0.099
        assert ticker["ask"] == 0.101
        assert round(ticker["last"], 3) == 0.1

        candles = await broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1)
        assert candles[0][1:5] == [0.098, 0.102, 0.097, 0.101]

        balances = await broker.fetch_balance()
        assert balances["free"]["XLM"] == 90.0
        assert balances["free"]["USDC"] == 225.0

        orders = await broker.fetch_open_orders()
        assert orders[0]["symbol"] == "XLM/USDC"
        assert orders[0]["side"] == "buy"

        created = await broker.create_order("XLM/USDC", "buy", 15, type="limit", price=0.11)
        assert created["id"] == "stellar-tx"
        assert FakeServerAsync.last_transaction.operations[0]["kind"] == "manage_buy"
        assert FakeServerAsync.last_transaction.operations[0]["price"] == "0.1100000"

        canceled = await broker.cancel_order("777")
        assert canceled["status"] == "canceled"
        assert FakeServerAsync.last_transaction.operations[0]["kind"] == "manage_sell"
        assert FakeServerAsync.last_transaction.operations[0]["amount"] == "0"

        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_maps_4h_timeframe_to_stellar_resolution(monkeypatch):
    import broker.stellar_broker as stellar_module

    captured_session = {}

    def session_factory():
        session = FakeStellarSession()
        captured_session["session"] = session
        return session

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"]},
            )
        )

        await broker.connect()
        await broker.fetch_ohlcv("XLM/USDC", timeframe="4h", limit=2)

        session = captured_session["session"]
        assert session.last_trade_aggregations_params is not None
        assert session.last_trade_aggregations_params["resolution"] == 14400000
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_builds_ohlcv_from_trades_when_aggregations_are_empty(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeSparseOhlcvSession()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"]},
            )
        )

        await broker.connect()
        candles = await broker.fetch_ohlcv("XLM/USDC", timeframe="1d", limit=1)
        assert len(candles) == 1
        assert candles[0][1:6] == [0.1, 0.106, 0.098, 0.103, 50.0]
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_reuses_cached_ohlcv_after_rate_limit(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeRateLimitedOhlcvSession()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "ohlcv_cache_ttl": 0.0},
            )
        )

        await broker.connect()
        first = await broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1)
        second = await broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1)
        assert first == second
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_coalesces_concurrent_ohlcv_requests(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeStellarSession()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "ohlcv_cache_ttl": 0.0},
            )
        )

        await broker.connect()
        call_count = {"trade_aggregations": 0}

        async def fake_request(method, path, params=None, payload=None):
            if path == "/trade_aggregations":
                call_count["trade_aggregations"] += 1
                await asyncio.sleep(0.05)
                return {
                    "_embedded": {
                        "records": [
                            {
                                "timestamp": 1700000000000,
                                "open": "0.098",
                                "high": "0.102",
                                "low": "0.097",
                                "close": "0.101",
                                "base_volume": "500.0",
                            }
                        ]
                    }
                }
            raise AssertionError(f"Unexpected request path: {path}")

        broker._request = fake_request

        first, second = await asyncio.gather(
            broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1),
            broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1),
        )

        assert first == second
        assert call_count["trade_aggregations"] == 1
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_enters_ohlcv_cooldown_after_rate_limit_without_cache(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeStellarSession()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={
                    "quote_assets": ["USDC", "XLM"],
                    "ohlcv_cache_ttl": 0.0,
                    "ohlcv_cooldown_seconds": 60.0,
                    "rate_limit_retries": 0,
                },
            )
        )

        await broker.connect()
        call_count = {"trade_aggregations": 0, "trades": 0}

        async def fake_request(method, path, params=None, payload=None):
            if path == "/trade_aggregations":
                call_count["trade_aggregations"] += 1
                raise aiohttp.ClientResponseError(
                    request_info=SimpleNamespace(real_url="https://horizon.stellar.org/trade_aggregations"),
                    history=(),
                    status=429,
                    message="Too Many Requests",
                    headers={"Retry-After": "0"},
                )
            raise AssertionError(f"Unexpected request path: {path}")

        async def fake_fetch_trades(symbol, limit=None):
            call_count["trades"] += 1
            raise aiohttp.ClientResponseError(
                request_info=SimpleNamespace(real_url="https://horizon.stellar.org/trades"),
                history=(),
                status=429,
                message="Too Many Requests",
                headers={"Retry-After": "0"},
            )

        broker._request = fake_request
        broker.fetch_trades = fake_fetch_trades

        first = await broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1)
        second = await broker.fetch_ohlcv("XLM/USDC", timeframe="1h", limit=1)

        assert first == []
        assert second == []
        assert call_count["trade_aggregations"] == 1
        assert call_count["trades"] == 1
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_fetch_ticker_uses_orderbook_mid_without_trades(monkeypatch):
    import broker.stellar_broker as stellar_module

    captured_session = {}

    def session_factory():
        session = FakeNoTradesTickerSession()
        captured_session["session"] = session
        return session

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"]},
            )
        )

        await broker.connect()
        ticker = await broker.fetch_ticker("XLM/USDC")
        assert ticker["bid"] == 0.099
        assert ticker["ask"] == 0.101
        assert round(ticker["last"], 3) == 0.1
        assert captured_session["session"].trade_calls == 0
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_reuses_cached_trades_after_rate_limit(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeTrades429AfterSuccessSession()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "trades_cache_ttl": 0.0, "rate_limit_retries": 0},
            )
        )

        await broker.connect()
        first = await broker.fetch_trades("XLM/USDC", limit=1)
        second = await broker.fetch_trades("XLM/USDC", limit=1)
        assert first == second
        await broker.close()

    asyncio.run(scenario())


def test_stellar_broker_returns_empty_trades_for_invalid_market(monkeypatch):
    import broker.stellar_broker as stellar_module

    def session_factory():
        return FakeTrades400Session()

    monkeypatch.setattr(stellar_module.aiohttp, "ClientSession", session_factory)

    async def scenario():
        broker = StellarBroker(
            SimpleNamespace(
                api_key=TEST_PUBLIC_KEY,
                secret=None,
                mode="live",
                params={"quote_assets": ["USDC", "XLM"], "rate_limit_retries": 0},
            )
        )

        await broker.connect()
        trades = await broker.fetch_trades("XLM/USDC", limit=1)
        assert trades == []
        await broker.close()

    asyncio.run(scenario())
