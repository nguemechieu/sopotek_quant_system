import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import core.sopotek_trading as sopotek_trading_module
import worker.symbol_worker as symbol_worker_module
from core.sopotek_trading import SopotekTrading
from engines.risk_engine import RiskEngine
from worker.symbol_worker import SymbolWorker


class DummyBroker:
    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100):
        return []

    async def fetch_balance(self):
        return {"total": {"USDT": 10000}}

    async def create_order(self, *args, **kwargs):
        return {"status": "filled"}


class CleanupBroker(DummyBroker):
    def __init__(self, positions=None, orders=None):
        self.positions = list(positions or [])
        self.orders = list(orders or [])
        self.canceled = []

    async def fetch_positions(self, symbols=None):
        return list(self.positions)

    async def fetch_open_orders(self, symbol=None, limit=None):
        return list(self.orders)

    async def cancel_order(self, order_id, symbol=None):
        self.canceled.append({"order_id": order_id, "symbol": symbol})
        return {"id": order_id, "status": "canceled", "symbol": symbol}


class ExplodingStrategy:
    def generate_signal(self, candles):
        raise AssertionError("fallback strategy path should not be used")


class ExplodingExecutionManager:
    async def execute(self, **kwargs):
        raise AssertionError("execution manager should not be called directly by SymbolWorker")


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


def test_symbol_worker_uses_centralized_signal_processor(monkeypatch):
    calls = []

    async def fast_sleep(_seconds):
        return None

    async def processor(symbol, timeframe=None, limit=None, publish_debug=True):
        calls.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "limit": limit,
                "publish_debug": publish_debug,
            }
        )
        worker.running = False

    monkeypatch.setattr(symbol_worker_module.asyncio, "sleep", fast_sleep)

    worker = SymbolWorker(
        symbol="BTC/USDT",
        broker=DummyBroker(),
        strategy=ExplodingStrategy(),
        execution_manager=ExplodingExecutionManager(),
        timeframe="15m",
        limit=120,
        signal_processor=processor,
    )

    asyncio.run(worker.run())

    assert calls == [
        {
            "symbol": "BTC/USDT",
            "timeframe": "15m",
            "limit": 120,
            "publish_debug": True,
        }
    ]


def test_sopotek_trading_process_symbol_routes_through_central_pipeline():
    published_ai = []
    published_debug = []

    async def apply_news_bias(symbol, signal):
        updated = dict(signal)
        updated["reason"] = f"{signal['reason']} | news aligned"
        return updated

    controller = SimpleNamespace(
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
        publish_ai_signal=lambda symbol, signal, candles=None: published_ai.append((symbol, dict(signal), list(candles or []))),
        publish_strategy_debug=lambda symbol, signal, candles=None, features=None: published_debug.append(
            (symbol, dict(signal), list(candles or []), features)
        ),
        apply_news_bias_to_signal=apply_news_bias,
    )

    trading = SopotekTrading(controller=controller)
    dataset = FakeDataset(_sample_frame())

    async def fake_get_symbol_dataset(**kwargs):
        assert kwargs["symbol"] == "BTC/USDT"
        assert kwargs["timeframe"] == "15m"
        assert kwargs["limit"] == 50
        return dataset

    captured = {}

    async def fake_process_signal(symbol, signal, dataset=None):
        captured["symbol"] = symbol
        captured["signal"] = dict(signal)
        captured["dataset"] = dataset
        return {"status": "filled", "reason": "submitted"}

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.signal_engine.generate_signal = lambda **kwargs: {
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.25,
        "confidence": 0.78,
        "reason": "breakout detected",
        "strategy_name": "Trend Following",
    }
    trading.process_signal = fake_process_signal

    result = asyncio.run(trading.process_symbol("BTC/USDT", timeframe="15m", limit=50))

    assert result["status"] == "filled"
    assert captured["symbol"] == "BTC/USDT"
    assert captured["signal"]["reason"].endswith("news aligned")
    assert captured["dataset"] is dataset
    assert published_ai and published_ai[0][0] == "BTC/USDT"
    assert published_debug and published_debug[0][0] == "BTC/USDT"
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "execution_manager"


def test_sopotek_trading_process_symbol_uses_symbol_assigned_strategy_variants():
    controller = SimpleNamespace(
        broker=DummyBroker(),
        symbols=["EUR/USD"],
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
        assigned_strategies_for_symbol=lambda symbol: [
            {"strategy_name": "Trend Following", "weight": 0.40, "score": 4.0, "rank": 1},
            {"strategy_name": "EMA Cross | London Session Aggressive", "weight": 0.60, "score": 9.0, "rank": 2},
        ],
    )

    trading = SopotekTrading(controller=controller)
    dataset = FakeDataset(_sample_frame())

    async def fake_get_symbol_dataset(**kwargs):
        return dataset

    chosen = {}

    async def fake_process_signal(symbol, signal, dataset=None):
        chosen["symbol"] = symbol
        chosen["signal"] = dict(signal)
        return {"status": "filled"}

    calls = []

    def fake_generate_signal(**kwargs):
        calls.append(kwargs["strategy_name"])
        if kwargs["strategy_name"] == "EMA Cross | London Session Aggressive":
            return {
                "symbol": "EUR/USD",
                "side": "buy",
                "amount": 0.5,
                "confidence": 0.72,
                "reason": "assigned strategy fired",
                "strategy_name": kwargs["strategy_name"],
            }
        return None

    trading.data_hub.get_symbol_dataset = fake_get_symbol_dataset
    trading.signal_engine.generate_signal = fake_generate_signal
    trading.process_signal = fake_process_signal

    result = asyncio.run(trading.process_symbol("EUR/USD", timeframe="1h", limit=50, publish_debug=False))

    assert result["status"] == "filled"
    assert calls == ["Trend Following", "EMA Cross | London Session Aggressive"]
    assert chosen["signal"]["strategy_name"] == "EMA Cross | London Session Aggressive"
    assert chosen["signal"]["strategy_assignment_weight"] == 0.60


def test_sopotek_trading_scales_basic_risk_rejections_into_smaller_orders():
    controller = SimpleNamespace(
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
    )

    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None

    captured = {}

    async def fake_execute(**kwargs):
        captured.update(kwargs)
        return {"status": "filled", "amount": kwargs["amount"], "reason": "submitted"}

    trading.execution_manager.execute = fake_execute

    result = asyncio.run(
        trading.process_signal(
            "BTC/USDT",
            {
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 25.0,
                "price": 100.0,
                "confidence": 0.80,
                "reason": "oversized breakout signal",
                "strategy_name": "Trend Following",
            },
            dataset=FakeDataset(_sample_frame()),
        )
    )

    assert result["status"] == "filled"
    assert captured["amount"] == 10.0
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "risk_engine"
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["status"] == "approved"


def test_sopotek_trading_cancels_stale_orders_before_exit_like_signal():
    controller = SimpleNamespace(
        broker=CleanupBroker(
            positions=[{"symbol": "BTC/USDT", "contracts": 0.25, "side": "long"}],
            orders=[
                {"id": "open-1", "symbol": "BTC/USDT", "status": "open"},
                {"id": "open-2", "symbol": "BTC/USDT", "status": "new"},
            ],
        ),
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
    )

    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None

    executed = {}

    async def fake_execute(**kwargs):
        executed.update(kwargs)
        return {"status": "filled", "amount": kwargs["amount"], "reason": "submitted"}

    trading.execution_manager.execute = fake_execute

    result = asyncio.run(
        trading.process_signal(
            "BTC/USDT",
            {
                "symbol": "BTC/USDT",
                "side": "sell",
                "amount": 1.0,
                "price": 100.0,
                "confidence": 0.80,
                "reason": "exit long on reversal",
                "strategy_name": "Trend Following",
            },
            dataset=FakeDataset(_sample_frame()),
        )
    )

    assert result["status"] == "filled"
    assert [row["order_id"] for row in controller.broker.canceled] == ["open-1", "open-2"]
    assert executed["side"] == "sell"


def test_sopotek_trading_does_not_cancel_orders_for_same_direction_signal():
    controller = SimpleNamespace(
        broker=CleanupBroker(
            positions=[{"symbol": "BTC/USDT", "contracts": 0.25, "side": "long"}],
            orders=[{"id": "open-1", "symbol": "BTC/USDT", "status": "open"}],
        ),
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
    )

    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None

    async def fake_execute(**kwargs):
        return {"status": "filled", "amount": kwargs["amount"], "reason": "submitted"}

    trading.execution_manager.execute = fake_execute

    asyncio.run(
        trading.process_signal(
            "BTC/USDT",
            {
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 1.0,
                "price": 100.0,
                "confidence": 0.80,
                "reason": "trend continuation",
                "strategy_name": "Trend Following",
            },
            dataset=FakeDataset(_sample_frame()),
        )
    )

    assert controller.broker.canceled == []


def test_sopotek_trading_uses_open_only_orders_for_hedge_entries():
    controller = SimpleNamespace(
        broker=DummyBroker(),
        symbols=["EUR/USD"],
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
        hedging_enabled=True,
        hedging_is_active=lambda broker=None: True,
    )

    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None
    trading.broker.hedging_supported = True

    captured = {}

    async def fake_execute(**kwargs):
        captured.update(kwargs)
        return {"status": "filled", "amount": kwargs["amount"], "reason": "submitted"}

    trading.execution_manager.execute = fake_execute

    asyncio.run(
        trading.process_signal(
            "EUR/USD",
            {
                "symbol": "EUR/USD",
                "side": "sell",
                "amount": 1.0,
                "price": 1.10,
                "confidence": 0.80,
                "reason": "fresh hedge entry",
                "strategy_name": "Trend Following",
            },
            dataset=FakeDataset(_sample_frame()),
        )
    )

    assert captured["params"]["positionFill"] == "OPEN_ONLY"


def test_sopotek_trading_blocks_trade_when_margin_closeout_guard_is_triggered():
    controller = SimpleNamespace(
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
        balances={"equity": 10000.0, "raw": {"marginCloseoutPercent": 0.72}},
        initial_capital=10000,
        market_data_repository=None,
        trade_repository=None,
        handle_trade_execution=lambda trade: None,
        margin_closeout_snapshot=lambda balances=None: {
            "enabled": True,
            "available": True,
            "ratio": 0.72,
            "threshold": 0.50,
            "blocked": True,
            "reason": "Margin closeout risk is 72.00%, above the configured limit of 50.00%. New trades are blocked.",
        },
    )

    trading = SopotekTrading(controller=controller)
    trading.risk_engine = RiskEngine(account_equity=10000, max_position_size_pct=0.10)
    trading.portfolio_allocator = None
    trading.portfolio_risk_engine = None

    async def fake_execute(**kwargs):
        raise AssertionError("execution manager should not run when margin closeout guard blocks the trade")

    trading.execution_manager.execute = fake_execute

    result = asyncio.run(
        trading.process_signal(
            "BTC/USDT",
            {
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 1.0,
                "price": 100.0,
                "confidence": 0.80,
                "reason": "breakout signal",
                "strategy_name": "Trend Following",
            },
            dataset=FakeDataset(_sample_frame()),
        )
    )

    assert result is None
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["stage"] == "margin_closeout_guard"
    assert trading.pipeline_status_snapshot()["BTC/USDT"]["status"] == "rejected"


def test_sopotek_trading_start_tolerates_invalid_initial_capital_text(monkeypatch):
    controller = SimpleNamespace(
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
        balances={"total": {"USDT": 15000}},
        initial_capital="abc",
        market_data_repository=None,
        trade_repository=None,
        handle_trade_execution=lambda trade: None,
    )

    trading = SopotekTrading(controller=controller)
    async def _noop_start(*args, **kwargs):
        return None

    monkeypatch.setattr(trading.execution_manager, "start", _noop_start)
    monkeypatch.setattr(sopotek_trading_module.MultiSymbolOrchestrator, "start", _noop_start)
    trading.behavior_guard.record_equity = lambda _equity: None

    asyncio.run(trading.start())

    assert trading.risk_engine is not None
    assert abs(float(trading.risk_engine.account_equity) - 15000.0) < 1e-9
