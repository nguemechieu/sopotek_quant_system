import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.sopotek_trading import SopotekTrading
from engines.risk_engine import RiskEngine
from event_bus.event_bus import EventBus
from event_bus.event_types import EventType


class DummyBroker:
    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
        return []

    async def fetch_balance(self):
        return {"total": {"USDT": 10000}}

    async def create_order(self, *args, **kwargs):
        return {"status": "filled"}


class FakeDataset:
    def __init__(self, frame):
        self.frame = frame
        self.empty = frame.empty

    def to_candles(self):
        rows = []
        for row in self.frame.itertuples(index=False):
            rows.append([row.timestamp, row.open, row.high, row.low, row.close, row.volume])
        return rows


def _sample_frame():
    return pd.DataFrame(
        [
            {"timestamp": 1, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10.0},
            {"timestamp": 2, "open": 100.5, "high": 102.0, "low": 100.0, "close": 101.5, "volume": 12.0},
        ]
    )


def _controller():
    return SimpleNamespace(
        broker=DummyBroker(),
        symbols=["BTC/USDT"],
        time_frame="1h",
        limit=200,
        strategy_name="Trend Following",
        strategy_params={},
        max_portfolio_risk=0.10,
        max_risk_per_trade=0.02,
        max_position_size_pct=0.10,
        max_gross_exposure_pct=2.0,
        balances={"total": {"USDT": 10000}},
        initial_capital=10000,
        market_data_repository=None,
        trade_repository=None,
        handle_trade_execution=lambda trade: None,
        publish_ai_signal=lambda *args, **kwargs: None,
        publish_strategy_debug=lambda *args, **kwargs: None,
    )


def test_agent_orchestrator_executes_trade_through_specialized_agents():
    controller = _controller()
    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None
    dataset = FakeDataset(_sample_frame())
    captured = {}

    async def fake_get_symbol_dataset(**kwargs):
        return dataset

    async def fake_execute(**kwargs):
        captured.update(kwargs)
        return {"status": "filled", "reason": "submitted"}

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.execution_manager.execute = fake_execute
    trading.signal_engine.generate_signal = lambda **kwargs: {
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.5,
        "confidence": 0.84,
        "reason": "agent pipeline breakout",
        "strategy_name": "Trend Following",
    }

    result = asyncio.run(trading.process_symbol("BTC/USDT", timeframe="15m", limit=50))

    assert result["status"] == "filled"
    assert captured["symbol"] == "BTC/USDT"
    assert captured["side"] == "buy"
    agent_names = [entry["agent"] for entry in trading.agent_memory_snapshot(limit=10)]
    assert "SignalAgent" in agent_names
    assert "RegimeAgent" in agent_names
    assert "PortfolioAgent" in agent_names
    assert "RiskAgent" in agent_names
    assert "ExecutionAgent" in agent_names
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "execution_manager"


def test_agent_orchestrator_stops_when_risk_agent_rejects_trade():
    controller = _controller()
    trading = SopotekTrading(controller=controller)
    trading.risk_engine = SimpleNamespace(
        account_equity=10000,
        adjust_trade=lambda _price, _amount: (False, 0.0, "risk agent blocked the setup"),
    )
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None
    dataset = FakeDataset(_sample_frame())

    async def fake_get_symbol_dataset(**kwargs):
        return dataset

    async def fake_execute(**kwargs):
        raise AssertionError("execution should not run after a risk rejection")

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.execution_manager.execute = fake_execute
    trading.signal_engine.generate_signal = lambda **kwargs: {
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.5,
        "confidence": 0.84,
        "reason": "agent pipeline breakout",
        "strategy_name": "Trend Following",
    }

    result = asyncio.run(trading.process_symbol("BTC/USDT", timeframe="15m", limit=50))

    assert result is None
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "risk_engine"
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["status"] == "rejected"
    latest = trading.agent_memory.latest("RiskAgent")
    assert latest is not None
    assert latest["stage"] == "rejected"


def test_agent_orchestrator_persists_agent_ledger_entries_when_repository_is_available():
    saved = []

    class Repo:
        def save_decision(self, **kwargs):
            saved.append(dict(kwargs))
            return SimpleNamespace(**kwargs)

    controller = _controller()
    controller.agent_decision_repository = Repo()
    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None
    dataset = FakeDataset(_sample_frame())

    async def fake_get_symbol_dataset(**kwargs):
        return dataset

    async def fake_execute(**kwargs):
        return {"status": "filled", "reason": "submitted"}

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.execution_manager.execute = fake_execute
    trading.signal_engine.generate_signal = lambda **kwargs: {
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.5,
        "confidence": 0.84,
        "reason": "agent pipeline breakout",
        "strategy_name": "Trend Following",
    }

    result = asyncio.run(trading.process_symbol("BTC/USDT", timeframe="15m", limit=50))

    assert result["status"] == "filled"
    assert len(saved) >= 5
    assert {row["agent_name"] for row in saved} >= {"SignalAgent", "RegimeAgent", "PortfolioAgent", "RiskAgent", "ExecutionAgent"}
    assert len({row["decision_id"] for row in saved if row.get("decision_id")}) == 1
    assert all(row.get("symbol") == "BTC/USDT" for row in saved)


def test_event_bus_dispatch_once_routes_typed_publish_to_async_handler():
    async def scenario():
        bus = EventBus()
        received = {}

        async def handler(event):
            received["type"] = event.type
            received["data"] = dict(event.data)

        bus.subscribe(EventType.MARKET_DATA, handler)
        await bus.publish(EventType.MARKET_DATA, {"symbol": "BTC/USDT", "timeframe": "15m"})
        await bus.dispatch_once()
        return received

    received = asyncio.run(scenario())

    assert received["type"] == EventType.MARKET_DATA
    assert received["data"] == {"symbol": "BTC/USDT", "timeframe": "15m"}


def test_process_symbol_uses_event_driven_runtime_when_no_custom_handler():
    controller = _controller()
    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None
    dataset = FakeDataset(_sample_frame())
    captured = {}

    async def fake_get_symbol_dataset(**kwargs):
        return dataset

    async def fake_execute(**kwargs):
        captured["order"] = dict(kwargs)
        return {"status": "filled", "reason": "submitted"}

    async def fail_legacy_path(_context):
        raise AssertionError("legacy orchestrator path should not run")

    original_runtime = trading.event_driven_runtime.process_market_data

    async def tracked_runtime(context, timeout=None):
        captured["runtime_symbol"] = context["symbol"]
        return await original_runtime(context, timeout=timeout)

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.execution_manager.execute = fake_execute
    trading.agent_orchestrator.run = fail_legacy_path
    trading.event_driven_runtime.process_market_data = tracked_runtime
    trading.signal_engine.generate_signal = lambda **kwargs: {
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.5,
        "confidence": 0.84,
        "reason": "event runtime breakout",
        "strategy_name": "Trend Following",
    }

    result = asyncio.run(trading.process_symbol("BTC/USDT", timeframe="15m", limit=50))

    assert result["status"] == "filled"
    assert captured["runtime_symbol"] == "BTC/USDT"
    assert captured["order"]["symbol"] == "BTC/USDT"
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "execution_manager"
