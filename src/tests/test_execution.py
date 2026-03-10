import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from event_bus.event_bus import EventBus
from event_bus.event_types import EventType
from execution.execution_manager import ExecutionManager
from execution.order_router import OrderRouter


class MockBroker:
    def __init__(self, balance=None, markets=None):
        self.orders = []
        self._balance = balance if balance is not None else {}

        exchange = type("Exchange", (), {})()
        exchange.markets = markets or {}
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
        order = {
            "id": "order-1",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "type": type,
            "price": price,
            "status": "filled",
            "params": params or {},
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }
        self.orders.append(order)
        return order

    async def fetch_balance(self):
        return self._balance

    async def fetch_ticker(self, symbol):
        return {"symbol": symbol, "price": 100.0, "bid": 100.0, "ask": 100.0}


class RateLimitedBroker(MockBroker):
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
        raise RuntimeError("429 Too Many Requests")


class TrackingBroker(MockBroker):
    def __init__(self, balance=None, markets=None):
        super().__init__(balance=balance, markets=markets)
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
        order = {
            "id": "tracked-1",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "type": type,
            "price": price,
            "status": "submitted",
            "filled": 0.0,
            "params": params or {},
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }
        self.orders.append(order)
        return order

    async def fetch_order(self, order_id, symbol=None):
        self.fetch_order_calls += 1
        status = "open" if self.fetch_order_calls == 1 else "filled"
        filled = 0.0 if status == "open" else 0.5
        return {
            "id": order_id,
            "symbol": symbol or "BTC/USDT",
            "side": "buy",
            "type": "limit",
            "amount": 0.5,
            "filled": filled,
            "price": 101.0,
            "status": status,
        }


def test_execute_accepts_keyword_order_arguments():
    broker = MockBroker(balance={"free": {"USDT": 1000}})
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(
        manager.execute(symbol="BTC/USDT", side="buy", amount=0.01, price=42000)
    )

    assert order["symbol"] == "BTC/USDT"
    assert order["side"] == "buy"
    assert order["amount"] == 0.01

    fill_event = asyncio.run(bus.queue.get())
    assert fill_event.type == EventType.FILL
    assert fill_event.data["symbol"] == "BTC/USDT"
    assert fill_event.data["side"] == "BUY"
    assert fill_event.data["qty"] == 0.01


def test_execute_accepts_legacy_signal_payload():
    broker = MockBroker(balance={"free": {"ETH": 5}})
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(manager.execute({"symbol": "ETH/USDT", "signal": "SELL", "size": 2}))

    assert order["symbol"] == "ETH/USDT"
    assert order["side"] == "sell"
    assert order["amount"] == 2


def test_execute_scales_buy_order_to_available_quote_balance():
    broker = MockBroker(
        balance={"free": {"USDT": 250}},
        markets={"BTC/USDT": {"active": True, "limits": {"cost": {"min": 10}}}},
    )
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(
        manager.execute(symbol="BTC/USDT", side="buy", amount=5, price=100)
    )

    assert order["amount"] == 2.45


def test_execute_skips_inactive_market():
    broker = MockBroker(
        balance={"free": {"USDT": 1000}},
        markets={"MKR/USDT": {"active": False}},
    )
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(
        manager.execute(symbol="MKR/USDT", side="buy", amount=1, price=100)
    )

    assert order is None
    assert broker.orders == []


def test_execute_cools_down_on_rate_limit():
    broker = RateLimitedBroker(balance={"free": {"USDT": 1000}})
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(
        manager.execute(symbol="BTC/USDT", side="buy", amount=0.01, price=100)
    )

    assert order is None
    assert manager._cooldown_remaining("BTC/USDT") > 0


def test_execute_propagates_stop_loss_and_take_profit():
    broker = MockBroker(balance={"free": {"USDT": 1000}})
    bus = EventBus()
    manager = ExecutionManager(broker, bus, OrderRouter(broker))

    order = asyncio.run(
        manager.execute(
            symbol="BTC/USDT",
            side="buy",
            amount=0.01,
            price=42000,
            stop_loss=41000,
            take_profit=45000,
        )
    )

    assert order["stop_loss"] == 41000
    assert order["take_profit"] == 45000
    assert broker.orders[0]["stop_loss"] == 41000
    assert broker.orders[0]["take_profit"] == 45000


def test_execute_notifier_receives_trade_log_fields():
    broker = MockBroker(balance={"free": {"USDT": 1000}})
    bus = EventBus()
    received = {}

    def notifier(trade):
        received.update(trade)

    manager = ExecutionManager(broker, bus, OrderRouter(broker), trade_notifier=notifier)

    asyncio.run(
        manager.execute(
            symbol="BTC/USDT",
            side="buy",
            amount=0.01,
            price=42000,
            stop_loss=41000,
            take_profit=45000,
        )
    )

    assert received["symbol"] == "BTC/USDT"
    assert received["side"] == "BUY"
    assert received["order_type"] == "market"
    assert received["status"] == "filled"
    assert received["order_id"] == "order-1"
    assert received["stop_loss"] == 41000
    assert received["take_profit"] == 45000
    assert received["timestamp"]


def test_execute_tracks_submitted_order_until_filled():
    async def scenario():
        broker = TrackingBroker(balance={"free": {"USDT": 1000}})
        bus = EventBus()
        notifications = []
        manager = ExecutionManager(broker, bus, OrderRouter(broker), trade_notifier=notifications.append)
        manager._order_tracking_interval = 0.01
        manager._order_tracking_timeout = 0.25

        order = await manager.execute(
            symbol="BTC/USDT",
            side="buy",
            amount=0.5,
            type="limit",
            price=100.0,
        )

        assert order["status"] == "submitted"
        assert bus.queue.empty()

        await asyncio.sleep(0.05)

        fill_event = await asyncio.wait_for(bus.queue.get(), timeout=0.2)
        assert fill_event.type == EventType.FILL
        assert fill_event.data["qty"] == 0.5

        statuses = [update["status"] for update in notifications]
        assert "submitted" in statuses
        assert "filled" in statuses

        await manager.stop()

    asyncio.run(scenario())
