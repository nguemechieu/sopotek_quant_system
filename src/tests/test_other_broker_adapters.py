import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import broker.paper_broker as paper_module
from broker.alpaca_broker import AlpacaBroker
from broker.oanda_broker import OandaBroker
from broker.paper_broker import PaperBroker
from market_data.ticker_buffer import TickerBuffer


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


class FakeOandaSession:
    def __init__(self, *args, **kwargs):
        self.closed = False
        self.last_order_payload = None
        self.last_close_payload = None

    def request(self, method, url, headers=None, params=None, json=None):
        if url.endswith("/pricing"):
            return FakeResponse(
                {
                    "prices": [
                        {
                            "instrument": "EUR_USD",
                            "bids": [{"price": "1.1000", "liquidity": 100000}],
                            "asks": [{"price": "1.1002", "liquidity": 100000}],
                        }
                    ]
                }
            )
        if "/candles" in url:
            return FakeResponse(
                {
                    "candles": [
                        {"complete": True, "time": "t1", "mid": {"o": "1.0", "h": "2.0", "l": "0.5", "c": "1.5"}, "volume": 10},
                        {"complete": True, "time": "t2", "mid": {"o": "1.5", "h": "2.5", "l": "1.0", "c": "2.0"}, "volume": 12},
                    ]
                }
            )
        if url.endswith("/summary"):
            return FakeResponse({"account": {"currency": "USD", "balance": "1000", "NAV": "1100", "marginUsed": "100"}})
        if url.endswith("/instruments"):
            return FakeResponse({"instruments": [{"name": "EUR_USD"}, {"name": "GBP_USD"}]})
        if url.endswith("/orders") and method == "GET":
            return FakeResponse({"orders": [{"id": "1", "instrument": "EUR_USD", "state": "PENDING"}]})
        if url.endswith("/orders") and method == "POST":
            self.last_order_payload = json
            return FakeResponse({"orderCreateTransaction": {"id": "2"}})
        if url.endswith("/openPositions"):
            return FakeResponse(
                {
                    "positions": [
                        {
                            "instrument": "EUR_USD",
                            "long": {"units": "3", "averagePrice": "1.2"},
                            "short": {"units": "2", "averagePrice": "1.3"},
                            "pl": "12",
                            "unrealizedPL": "8",
                            "marginUsed": "50",
                            "positionValue": "600",
                        }
                    ]
                }
            )
        if "/positions/" in url and url.endswith("/close"):
            self.last_close_payload = json
            if "shortUnits" in (json or {}):
                return FakeResponse({"shortOrderCreateTransaction": {"id": "3", "instrument": "EUR_USD", "type": "MARKET_ORDER"}})
            return FakeResponse({"longOrderCreateTransaction": {"id": "4", "instrument": "EUR_USD", "type": "MARKET_ORDER"}})
        if "/cancel" in url:
            return FakeResponse({"orderCancelTransaction": {"id": "1"}})
        if "/orders/" in url:
            return FakeResponse({"order": {"id": "1", "instrument": "EUR_USD"}})
        if url.endswith("/trades"):
            return FakeResponse({"trades": [{"instrument": "EUR_USD"}]})
        raise AssertionError(f"Unhandled Oanda URL: {method} {url}")

    async def close(self):
        self.closed = True


class FakeAlpacaREST:
    def __init__(self, api_key, secret, base_url, api_version="v2"):
        self.api_key = api_key
        self.secret = secret
        self.base_url = base_url

    def get_account(self):
        return SimpleNamespace(status="ACTIVE", cash="5000", equity="5200", buying_power="7000")

    def get_latest_trade(self, symbol):
        return SimpleNamespace(price=201.5)

    def get_latest_quote(self, symbol):
        return SimpleNamespace(bid_price=201.0, ask_price=202.0)

    def get_bars(self, symbol, timeframe, limit=100):
        return [
            SimpleNamespace(t="t1", o=1.0, h=2.0, l=0.5, c=1.5, v=10),
            SimpleNamespace(t="t2", o=1.5, h=2.5, l=1.0, c=2.0, v=11),
        ]

    def list_assets(self, status="active"):
        return [SimpleNamespace(symbol="AAPL", tradable=True), SimpleNamespace(symbol="TSLA", tradable=True)]

    def submit_order(self, **kwargs):
        return SimpleNamespace(
            id="alp-1",
            symbol=kwargs["symbol"],
            side=kwargs["side"],
            type=kwargs["type"],
            status="accepted",
            qty=str(kwargs["qty"]),
            filled_qty="0",
            limit_price=str(kwargs.get("limit_price", 0)),
            stop_price=str(kwargs.get("stop_price", 0)),
            filled_avg_price="0",
        )

    def cancel_order(self, order_id):
        return {"id": order_id, "status": "canceled"}

    def cancel_all_orders(self):
        return [{"status": "canceled"}]

    def get_order(self, order_id):
        return SimpleNamespace(id=order_id, symbol="AAPL", side="buy", type="market", status="filled", qty="2", filled_qty="2", filled_avg_price="200")

    def list_orders(self, status="all", limit=None):
        orders = [
            SimpleNamespace(id="1", symbol="AAPL", side="buy", type="market", status="new", qty="2", filled_qty="0", filled_avg_price="0"),
            SimpleNamespace(id="2", symbol="TSLA", side="sell", type="limit", status="filled", qty="1", filled_qty="1", limit_price="300", filled_avg_price="300"),
        ]
        return orders[:limit] if limit else orders

    def list_positions(self):
        return [SimpleNamespace(symbol="AAPL", qty="2", avg_entry_price="199", market_value="402")]

    def close(self):
        return None


def test_oanda_broker_normalizes_common_methods(monkeypatch):
    import broker.oanda_broker as oanda_module

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", FakeOandaSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
        assert (await broker.fetch_ticker("EUR/USD"))["ask"] == 1.1002
        assert (await broker.fetch_orderbook("EUR/USD"))["bids"][0][0] == 1.1
        assert len(await broker.fetch_ohlcv("EUR/USD", timeframe="1h", limit=2)) == 2
        assert (await broker.fetch_balance())["equity"] == 1100.0
        assert await broker.fetch_symbols() == ["EUR_USD", "GBP_USD"]
        positions = await broker.fetch_positions()
        assert len(positions) == 2
        assert positions[0]["symbol"] == "EUR_USD"
        assert {item["position_side"] for item in positions} == {"long", "short"}
        assert len(await broker.fetch_open_orders("EUR/USD")) == 1
        assert len(await broker.fetch_closed_orders("EUR/USD")) == 0
        await broker.close()

    asyncio.run(scenario())


def test_alpaca_broker_normalizes_common_methods(monkeypatch):
    import broker.alpaca_broker as alpaca_module

    monkeypatch.setattr(alpaca_module, "tradeapi", SimpleNamespace(REST=FakeAlpacaREST))

    async def scenario():
        broker = AlpacaBroker(SimpleNamespace(api_key="key", secret="secret", mode="paper", sandbox=False))
        assert (await broker.fetch_ticker("AAPL"))["bid"] == 201.0
        assert (await broker.fetch_orderbook("AAPL"))["asks"][0][0] == 202.0
        assert len(await broker.fetch_ohlcv("AAPL", timeframe="1h", limit=2)) == 2
        assert "AAPL" in await broker.fetch_symbols()
        assert (await broker.fetch_balance())["cash"] == 5000.0
        assert (await broker.fetch_positions())[0]["symbol"] == "AAPL"
        order = await broker.create_order("AAPL", "buy", 2, type="limit", price=200)
        assert order["symbol"] == "AAPL"
        stop_limit = await broker.create_order("AAPL", "buy", 1, type="stop_limit", price=200, stop_price=201)
        assert stop_limit["type"] == "stop_limit"
        assert stop_limit["stop_price"] == 201.0
        assert len(await broker.fetch_closed_orders(limit=5)) == 1
        await broker.close()

    asyncio.run(scenario())


def test_non_ccxt_brokers_report_supported_market_venues(monkeypatch):
    import broker.oanda_broker as oanda_module
    import broker.alpaca_broker as alpaca_module

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", FakeOandaSession)
    monkeypatch.setattr(alpaca_module, "tradeapi", SimpleNamespace(REST=FakeAlpacaREST))

    oanda = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
    alpaca = AlpacaBroker(SimpleNamespace(api_key="key", secret="secret", mode="paper", sandbox=False))
    paper = PaperBroker(SimpleNamespace(logger=None, paper_balance=1000.0, initial_balance=1000.0, mode="paper", params={}))

    assert oanda.supported_market_venues() == ["auto", "otc"]
    assert alpaca.supported_market_venues() == ["auto", "spot"]
    assert paper.supported_market_venues() == ["auto", "spot", "derivative", "option", "otc"]


def test_paper_broker_exposes_normalized_api(monkeypatch):
    class DummyController:
        def __init__(self):
            self.logger = None
            self.paper_balance = 1000.0
            self.symbols = ["BTC/USDT"]
            self.candle_buffers = {}
            self.ticker_buffer = TickerBuffer()
            self.ticker_stream = SimpleNamespace(get=lambda symbol: None)
            self.time_frame = "1h"
            self.broker = None

            self.ticker_buffer.update(
                "BTC/USDT",
                {"symbol": "BTC/USDT", "price": 100.0, "bid": 99.9, "ask": 100.1},
            )

    async def fake_market_data_broker(self, symbol=None):
        return None

    monkeypatch.setattr(PaperBroker, "_ensure_market_data_broker", fake_market_data_broker)

    async def scenario():
        controller = DummyController()
        broker = PaperBroker(controller)
        controller.broker = broker
        await broker.connect()
        ticker = await broker.fetch_ticker("BTC/USDT")
        assert ticker["last"] == 100.0
        orderbook = await broker.fetch_orderbook("BTC/USDT")
        assert orderbook["bids"][0][0] == 100.0
        order = await broker.create_order("BTC/USDT", "buy", 1, type="market")
        assert order["status"] == "filled"
        stop_limit = await broker.create_order("BTC/USDT", "buy", 1, type="stop_limit", price=99.0, stop_price=101.0)
        assert stop_limit["status"] == "open"
        assert stop_limit["stop_price"] == 101.0
        assert (await broker.fetch_balance())["free"]["USDT"] == 900.0
        assert (await broker.fetch_positions())[0]["symbol"] == "BTC/USDT"
        assert (await broker.fetch_positions(symbols=["BTC/USDT"]))[0]["symbol"] == "BTC/USDT"
        await broker.close()

    asyncio.run(scenario())


def test_paper_broker_bootstraps_public_market_data(monkeypatch):
    class FakeMarketDataBroker:
        def __init__(self, config):
            self.config = config
            self.closed = False

        async def connect(self):
            return True

        async def close(self):
            self.closed = True

        async def fetch_ticker(self, symbol):
            return {"symbol": symbol, "last": 123.4, "bid": 123.3, "ask": 123.5}

        async def fetch_orderbook(self, symbol, limit=50):
            return {"symbol": symbol, "bids": [[123.3, 5.0]], "asks": [[123.5, 4.0]]}

        async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
            return [["t1", 120.0, 125.0, 119.0, 123.4, 42.0]]

        async def fetch_symbols(self):
            return ["BTC/USDT", "ETH/USDT"]

    class DummyController:
        def __init__(self):
            self.logger = None
            self.paper_balance = 1000.0
            self.symbols = ["BTC/USDT"]
            self.candle_buffers = {}
            self.ticker_buffer = TickerBuffer()
            self.ticker_stream = SimpleNamespace(get=lambda symbol: None, update=lambda symbol, ticker: None)
            self.time_frame = "1h"
            self.broker = None
            self.config = SimpleNamespace(
                broker=SimpleNamespace(params={"paper_data_exchange": "binanceus"})
            )

    monkeypatch.setattr(paper_module, "CCXTBroker", FakeMarketDataBroker)

    async def scenario():
        controller = DummyController()
        broker = PaperBroker(controller)
        controller.broker = broker
        market_data_config = broker._build_market_data_config()
        assert market_data_config.mode == "live"
        assert market_data_config.sandbox is False
        await broker.connect()

        ticker = await broker.fetch_ticker("BTC/USDT")
        assert ticker["last"] == 123.4
        assert controller.ticker_buffer.latest("BTC/USDT")["last"] == 123.4

        orderbook = await broker.fetch_orderbook("BTC/USDT")
        assert orderbook["asks"][0][0] == 123.5

        candles = await broker.fetch_ohlcv("BTC/USDT", timeframe="1h", limit=1)
        assert candles[0][4] == 123.4

        symbols = await broker.fetch_symbols()
        assert "ETH/USDT" in symbols

        await broker.close()
        assert broker.market_data_broker is None

    asyncio.run(scenario())


def test_base_broker_close_all_positions_uses_opposite_side():
    class DummyBroker(PaperBroker):
        def __init__(self):
            controller = SimpleNamespace(
                logger=None,
                paper_balance=1000.0,
                symbols=["BTC/USDT"],
                candle_buffers={},
                ticker_buffer=TickerBuffer(),
                ticker_stream=SimpleNamespace(get=lambda symbol: None),
                time_frame="1h",
                broker=None,
            )
            super().__init__(controller)
            self.orders_sent = []
            self.positions = {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "amount": 2.0,
                    "entry_price": 100.0,
                    "side": "long",
                }
            }

        async def connect(self):
            return True

        async def close(self):
            return True

        async def create_order(self, symbol, side, amount, type="market", price=None, params=None, **kwargs):
            order = {
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "type": type,
            }
            self.orders_sent.append(order)
            return order

    async def scenario():
        broker = DummyBroker()
        results = await broker.close_all_positions()
        assert len(results) == 1
        assert broker.orders_sent[0]["symbol"] == "BTC/USDT"
        assert broker.orders_sent[0]["side"] == "sell"
        assert broker.orders_sent[0]["amount"] == 2.0

    asyncio.run(scenario())


def test_oanda_broker_formats_order_payload(monkeypatch):
    import broker.oanda_broker as oanda_module

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", FakeOandaSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
        await broker.create_order("EUR/USD", "buy", 1, type="market")
        payload = broker.session.last_order_payload["order"]
        assert payload["instrument"] == "EUR_USD"
        assert payload["type"] == "MARKET"
        assert payload["timeInForce"] == "FOK"
        assert payload["units"] == "1"

        await broker.create_order("EUR/USD", "sell", 2, type="limit", price=1.23456)
        payload = broker.session.last_order_payload["order"]
        assert payload["type"] == "LIMIT"
        assert payload["timeInForce"] == "GTC"
        assert payload["units"] == "-2"
        assert payload["price"] == "1.23456"

        await broker.create_order("EUR/USD", "buy", 1, type="stop_limit", price=1.24001, stop_price=1.23501)
        payload = broker.session.last_order_payload["order"]
        assert payload["type"] == "STOP"
        assert payload["price"] == "1.23501"
        assert payload["priceBound"] == "1.24001"

        await broker.create_order(
            "EUR/USD",
            "buy",
            1,
            type="market",
            stop_loss=1.21001,
            take_profit=1.26001,
        )
        payload = broker.session.last_order_payload["order"]
        assert payload["stopLossOnFill"]["price"] == "1.21001"
        assert payload["takeProfitOnFill"]["price"] == "1.26001"
        await broker.close()

    asyncio.run(scenario())


def test_oanda_broker_cancels_orders(monkeypatch):
    import broker.oanda_broker as oanda_module

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", FakeOandaSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
        canceled = await broker.cancel_order("1", symbol="EUR/USD")
        assert canceled["id"] == "1"
        assert canceled["status"] == "canceled"
        assert canceled["symbol"] == "EUR_USD"

        all_canceled = await broker.cancel_all_orders(symbol="EUR/USD")
        assert len(all_canceled) == 1
        assert all_canceled[0]["status"] == "canceled"
        await broker.close()

    asyncio.run(scenario())


def test_oanda_broker_closes_specific_hedge_leg(monkeypatch):
    import broker.oanda_broker as oanda_module

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", FakeOandaSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
        positions = await broker.fetch_positions(symbols=["EUR/USD"])
        short_leg = next(item for item in positions if item["position_side"] == "short")
        result = await broker.close_position("EUR/USD", position=short_leg)
        assert result["position_side"] == "short"
        assert broker.session.last_close_payload == {"shortUnits": "ALL"}
        await broker.close()

    asyncio.run(scenario())


def test_oanda_broker_surfaces_reject_reason(monkeypatch):
    import aiohttp
    import broker.oanda_broker as oanda_module

    class RejectResponse(FakeResponse):
        def __init__(self, payload, status=400, message="Bad Request"):
            super().__init__(payload)
            self.status = status
            self.message = message

        def raise_for_status(self):
            raise aiohttp.ClientResponseError(
                request_info=None,
                history=(),
                status=self.status,
                message=self.message,
                headers=None,
            )

        async def text(self):
            return json.dumps(self.payload)

    class RejectSession(FakeOandaSession):
        def request(self, method, url, headers=None, params=None, json=None):
            if url.endswith("/instruments"):
                return FakeResponse({"instruments": [{"name": "EUR_USD"}]})
            if url.endswith("/orders") and method == "POST":
                return RejectResponse(
                    {
                        "errorMessage": "The account has insufficient margin available.",
                        "orderRejectTransaction": {"rejectReason": "INSUFFICIENT_MARGIN"},
                    }
                )
            return super().request(method, url, headers=headers, params=params, json=json)

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", RejectSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="live"))
        try:
            await broker.create_order("EUR/USD", "buy", 1, type="market")
        except RuntimeError as exc:
            message = str(exc)
            assert "insufficient margin available" in message.lower()
            assert "insufficient_margin" in message.lower()
        else:
            raise AssertionError("Expected Oanda rejection to raise RuntimeError")
        await broker.close()

    asyncio.run(scenario())


def test_oanda_broker_paginates_ohlcv_history(monkeypatch):
    import broker.oanda_broker as oanda_module

    class PagingOandaSession:
        def __init__(self, *args, **kwargs):
            self.closed = False
            self.calls = []

        def request(self, method, url, headers=None, params=None, json=None):
            if "/candles" in url:
                params = params or {}
                self.calls.append(dict(params))
                cursor = params.get("to")
                if not cursor:
                    candles = [
                        {"complete": True, "time": "2026-01-03T00:00:00Z", "mid": {"o": "1.3", "h": "1.4", "l": "1.2", "c": "1.35"}, "volume": 13},
                        {"complete": True, "time": "2026-01-04T00:00:00Z", "mid": {"o": "1.35", "h": "1.5", "l": "1.3", "c": "1.45"}, "volume": 14},
                    ]
                else:
                    candles = [
                        {"complete": True, "time": "2026-01-01T00:00:00Z", "mid": {"o": "1.0", "h": "1.2", "l": "0.9", "c": "1.1"}, "volume": 10},
                        {"complete": True, "time": "2026-01-02T00:00:00Z", "mid": {"o": "1.1", "h": "1.3", "l": "1.0", "c": "1.2"}, "volume": 11},
                        {"complete": True, "time": "2026-01-03T00:00:00Z", "mid": {"o": "1.3", "h": "1.4", "l": "1.2", "c": "1.35"}, "volume": 13},
                    ]
                return FakeResponse({"candles": candles})
            raise AssertionError(f"Unhandled Oanda URL: {method} {url}")

        async def close(self):
            self.closed = True

    monkeypatch.setattr(oanda_module.aiohttp, "ClientSession", PagingOandaSession)

    async def scenario():
        broker = OandaBroker(SimpleNamespace(api_key="token", account_id="acct-1", mode="practice"))
        broker.MAX_OHLCV_COUNT = 2

        candles = await broker.fetch_ohlcv("EUR/USD", timeframe="1h", limit=4)

        assert len(candles) == 4
        assert [row[0] for row in candles] == [
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
            "2026-01-04T00:00:00Z",
        ]
        assert len(broker.session.calls) == 2
        assert broker.session.calls[1]["to"] == "2026-01-03T00:00:00Z"
        await broker.close()

    asyncio.run(scenario())


def test_paper_broker_falls_back_for_xlm_usdt_daily_history(monkeypatch):
    class FakeMarketDataBroker:
        def __init__(self, config):
            self.config = config
            self.exchange = config.exchange

        async def connect(self):
            return True

        async def close(self):
            return True

        async def fetch_ticker(self, symbol):
            if self.exchange == "binanceus" and symbol == "XLM/USDT":
                raise RuntimeError("symbol not available")
            return {"symbol": symbol, "last": 0.125, "bid": 0.124, "ask": 0.126}

        async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
            if self.exchange == "binanceus" and symbol == "XLM/USDT":
                return []
            return [[1710000000000, 0.10, 0.13, 0.09, 0.125, 1000000.0]]

        async def fetch_symbols(self):
            if self.exchange == "binanceus":
                return ["BTC/USDT"]
            return ["BTC/USDT", "XLM/USDT"]

    class DummyController:
        def __init__(self):
            self.logger = None
            self.paper_balance = 1000.0
            self.symbols = ["XLM/USDT"]
            self.candle_buffers = {}
            self.ticker_buffer = TickerBuffer()
            self.ticker_stream = SimpleNamespace(get=lambda symbol: None, update=lambda symbol, ticker: None)
            self.time_frame = "1d"
            self.broker = None
            self.config = SimpleNamespace(
                broker=SimpleNamespace(params={"paper_data_exchanges": ["binanceus", "binance"]})
            )

    monkeypatch.setattr(paper_module, "CCXTBroker", FakeMarketDataBroker)

    async def scenario():
        controller = DummyController()
        broker = PaperBroker(controller)
        controller.broker = broker
        await broker.connect()

        candles = await broker.fetch_ohlcv("XLM/USDT", timeframe="1d", limit=300)
        ticker = await broker.fetch_ticker("XLM/USDT")

        assert candles[0][4] == 0.125
        assert ticker["last"] == 0.125
        assert broker.market_data_exchange == "binance"

        await broker.close()

    asyncio.run(scenario())
