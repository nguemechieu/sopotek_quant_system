import asyncio
import os
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parent / "test_runtime_storage.sqlite3"
os.environ["SOPOTEK_DATABASE_URL"] = f"sqlite:///{DB_PATH.as_posix()}"

from event_bus.event_bus import EventBus
from execution.execution_manager import ExecutionManager
from execution.order_router import OrderRouter
from storage.database import engine, init_database
from storage.market_data_repository import MarketDataRepository
from storage.trade_repository import TradeRepository


class MockBroker:
    exchange_name = "paper"

    def __init__(self):
        self._balance = {"free": {"USDT": 1000.0}}
        exchange = type("Exchange", (), {})()
        exchange.markets = {"BTC/USDT": {"active": True}}
        exchange.amount_to_precision = lambda symbol, amount: amount
        self.exchange = exchange

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
        return {
            "id": "order-123",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "type": type,
            "price": 100.0 if price is None else price,
            "status": "filled",
            "timestamp": "2026-03-10T12:00:00+00:00",
            "params": params or {},
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }

    async def fetch_balance(self):
        return self._balance

    async def fetch_ticker(self, symbol):
        return {"symbol": symbol, "price": 100.0, "bid": 99.9, "ask": 100.1}


class TrackingBroker(MockBroker):
    def __init__(self):
        super().__init__()
        self.fetch_order_calls = 0

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
        return {
            "id": "order-track-1",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "type": type,
            "price": 100.0 if price is None else price,
            "status": "submitted",
            "filled": 0.0,
            "timestamp": "2026-03-10T12:00:00+00:00",
        }

    async def fetch_order(self, order_id, symbol=None):
        self.fetch_order_calls += 1
        return {
            "id": order_id,
            "symbol": symbol or "BTC/USDT",
            "side": "buy",
            "amount": 0.25,
            "type": "limit",
            "price": 101.0,
            "status": "filled" if self.fetch_order_calls >= 1 else "open",
            "filled": 0.25,
            "timestamp": "2026-03-10T12:00:05+00:00",
        }


def setup_function(_func):
    engine.dispose()
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_database()


def teardown_function(_func):
    engine.dispose()
    if DB_PATH.exists():
        DB_PATH.unlink()


def test_market_data_repository_round_trips_candles():
    repo = MarketDataRepository()

    inserted = repo.save_candles(
        "BTC/USDT",
        "1h",
        [
            [1710000000000, 100.0, 101.0, 99.5, 100.5, 10.0],
            [1710003600000, 100.5, 102.0, 100.0, 101.5, 12.0],
        ],
        exchange="binanceus",
    )
    duplicate_inserted = repo.save_candles(
        "BTC/USDT",
        "1h",
        [
            [1710000000000, 100.0, 101.0, 99.5, 100.5, 10.0],
        ],
        exchange="binanceus",
    )
    candles = repo.get_candles("BTC/USDT", timeframe="1h", limit=10, exchange="binanceus")

    assert inserted == 2
    assert duplicate_inserted == 0
    assert len(candles) == 2
    assert candles[0][1:6] == [100.0, 101.0, 99.5, 100.5, 10.0]


def test_execution_manager_persists_trade_history():
    broker = MockBroker()
    bus = EventBus()
    repo = TradeRepository()
    notifications = []
    manager = ExecutionManager(
        broker=broker,
        event_bus=bus,
        router=OrderRouter(broker),
        trade_repository=repo,
        trade_notifier=notifications.append,
    )

    execution = asyncio.run(
        manager.execute(symbol="BTC/USDT", side="buy", amount=0.25, price=100.0)
    )
    trades = repo.get_trades(limit=10)

    assert execution["id"] == "order-123"
    assert len(trades) == 1
    assert trades[0].symbol == "BTC/USDT"
    assert trades[0].order_id == "order-123"
    assert notifications[0]["symbol"] == "BTC/USDT"
    assert notifications[0]["size"] == 0.25


def test_execution_manager_updates_persisted_order_status_in_place():
    async def scenario():
        broker = TrackingBroker()
        bus = EventBus()
        repo = TradeRepository()
        manager = ExecutionManager(
            broker=broker,
            event_bus=bus,
            router=OrderRouter(broker),
            trade_repository=repo,
            trade_notifier=lambda *_args, **_kwargs: None,
        )
        manager._order_tracking_interval = 0.01
        manager._order_tracking_timeout = 0.25

        await manager.execute(symbol="BTC/USDT", side="buy", amount=0.25, type="limit", price=100.0)
        await asyncio.sleep(0.05)
        await manager.stop()

        trades = repo.get_trades(limit=10)
        assert len(trades) == 1
        assert trades[0].order_id == "order-track-1"
        assert trades[0].status == "filled"
        assert trades[0].price == 101.0

    asyncio.run(scenario())
