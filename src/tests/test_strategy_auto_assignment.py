import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.app_controller import AppController


class _Settings:
    def __init__(self):
        self.store = {}

    def value(self, key, default=None):
        return self.store.get(key, default)

    def setValue(self, key, value):
        self.store[key] = value


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def exception(self, *_args, **_kwargs):
        return None

    def debug(self, *_args, **_kwargs):
        return None


class _FakeRanker:
    def __init__(self):
        self.calls = []

    def rank(self, data, symbol, timeframe=None, strategy_names=None, top_n=None):
        self.calls.append((symbol, timeframe, tuple(strategy_names or []), len(data)))
        if symbol == "EUR/USD" and str(timeframe or "") == "4h":
            top_strategy = "MACD Trend"
            top_score = 12.0
            top_profit = 180.0
            top_sharpe = 1.9
            top_equity = 10180.0
        elif symbol == "EUR/USD":
            top_strategy = "EMA Cross"
            top_score = 9.0
            top_profit = 140.0
            top_sharpe = 1.6
            top_equity = 10140.0
        else:
            top_strategy = "MACD Trend"
            top_score = 9.0
            top_profit = 140.0
            top_sharpe = 1.6
            top_equity = 10140.0
        alt_strategy = "Trend Following"
        return pd.DataFrame(
            [
                {
                    "strategy_name": top_strategy,
                    "score": top_score,
                    "total_profit": top_profit,
                    "sharpe_ratio": top_sharpe,
                    "win_rate": 0.61,
                    "final_equity": top_equity,
                    "max_drawdown": 90.0,
                    "closed_trades": 18,
                },
                {
                    "strategy_name": alt_strategy,
                    "score": 5.0,
                    "total_profit": 80.0,
                    "sharpe_ratio": 1.1,
                    "win_rate": 0.55,
                    "final_equity": 10080.0,
                    "max_drawdown": 110.0,
                    "closed_trades": 14,
                },
            ]
        )


def _sample_frame(rows=160):
    return pd.DataFrame(
        {
            "timestamp": list(range(rows)),
            "open": [100.0 + i for i in range(rows)],
            "high": [101.0 + i for i in range(rows)],
            "low": [99.0 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [1000.0 + i for i in range(rows)],
        }
    )


def _saved_assignment(symbol, strategy_name, timeframe="1h", assignment_mode="single", assignment_source="auto"):
    return [
        {
            "strategy_name": strategy_name,
            "score": 9.0,
            "weight": 1.0,
            "symbol": symbol,
            "timeframe": timeframe,
            "assignment_mode": assignment_mode,
            "assignment_source": assignment_source,
            "rank": 1,
            "total_profit": 0.0,
            "sharpe_ratio": 0.0,
            "win_rate": 0.0,
            "final_equity": 0.0,
            "max_drawdown": 0.0,
            "closed_trades": 0,
        }
    ]


def _make_controller():
    settings = _Settings()
    refresh_calls = []
    ranker = _FakeRanker()
    frame = _sample_frame()

    controller = AppController.__new__(AppController)
    controller.settings = settings
    controller.logger = _Logger()
    controller.multi_strategy_enabled = True
    controller.max_symbol_strategies = 2
    controller.symbol_strategy_assignments = {}
    controller.symbol_strategy_rankings = {}
    controller.symbol_strategy_locks = set()
    controller.strategy_auto_assignment_enabled = True
    controller.strategy_auto_assignment_ready = False
    controller.strategy_auto_assignment_in_progress = False
    controller.strategy_auto_assignment_progress = {}
    controller._strategy_auto_assignment_task = None
    controller.time_frame = "1h"
    controller.strategy_assignment_scan_timeframes = ["1h"]
    controller.strategy_name = "Trend Following"
    controller.initial_capital = 10000
    controller.symbols = ["EUR/USD", "BTC/USDT"]
    controller.candle_buffers = {
        "EUR/USD": {"1h": frame.copy()},
        "BTC/USDT": {"1h": frame.copy()},
    }
    async def request_candle_data(symbol, timeframe="1h", limit=None):
        frame_for_timeframe = frame.copy()
        controller.candle_buffers.setdefault(str(symbol), {})[str(timeframe)] = frame_for_timeframe
        return frame_for_timeframe

    controller.request_candle_data = request_candle_data
    controller.trading_system = SimpleNamespace(
        strategy=SimpleNamespace(list=lambda: ["Trend Following", "EMA Cross", "MACD Trend"]),
        refresh_strategy_preferences=lambda: refresh_calls.append(True),
    )
    controller.terminal = None
    controller._build_strategy_ranker = lambda strategy_registry: ranker
    controller._refresh_calls = refresh_calls
    controller._ranker = ranker
    return controller


def test_assign_strategy_to_symbol_marks_manual_lock_and_source():
    controller = _make_controller()

    assigned = controller.assign_strategy_to_symbol("btc_usdt", "Trend Following", timeframe="4h")

    assert assigned[0]["assignment_source"] == "manual"
    assert controller.symbol_strategy_assignment_locked("BTC/USDT") is True


def test_auto_rank_and_assign_strategies_assigns_unlocked_symbols_and_preserves_manual_locks():
    controller = _make_controller()
    manual_assignment = controller.assign_strategy_to_symbol("BTC/USDT", "Trend Following", timeframe="4h")
    controller._refresh_calls.clear()

    result = asyncio.run(controller.auto_rank_and_assign_strategies(timeframe="1h"))

    eur_assigned = controller.assigned_strategies_for_symbol("EUR/USD")
    btc_assigned = controller.assigned_strategies_for_symbol("BTC/USDT")
    btc_ranked = controller.ranked_strategies_for_symbol("BTC/USDT")

    assert eur_assigned[0]["strategy_name"] == "EMA Cross"
    assert eur_assigned[0]["assignment_source"] == "auto"
    assert controller.symbol_strategy_assignment_locked("EUR/USD") is False

    assert btc_assigned == manual_assignment
    assert controller.symbol_strategy_assignment_locked("BTC/USDT") is True
    assert btc_ranked == []

    assert result["assigned_symbols"] == ["EUR/USD"]
    assert result["restored_symbols"] == ["BTC/USDT"]
    assert result["skipped_symbols"] == []
    assert controller._ranker.calls == [("EUR/USD", "1h", ("Trend Following", "EMA Cross", "MACD Trend"), 160)]
    assert controller._refresh_calls == [True]




def test_auto_rank_and_assign_strategies_selects_best_timeframe_for_symbol():
    controller = _make_controller()
    controller.symbols = ["EUR/USD"]
    controller.candle_buffers = {
        "EUR/USD": {
            "1h": _sample_frame(),
            "4h": _sample_frame(),
        }
    }
    controller.strategy_assignment_scan_timeframes = ["1h", "4h"]

    result = asyncio.run(controller.auto_rank_and_assign_strategies(timeframe="1h"))
    assigned = controller.assigned_strategies_for_symbol("EUR/USD")
    ranked = controller.ranked_strategies_for_symbol("EUR/USD")

    assert result["assigned_symbols"] == ["EUR/USD"]
    assert assigned[0]["strategy_name"] == "MACD Trend"
    assert assigned[0]["timeframe"] == "4h"
    assert ranked[0]["strategy_name"] == "MACD Trend"
    assert ranked[0]["timeframe"] == "4h"
    assert result["scan_timeframes"] == ["1h", "4h"]

def test_strategy_auto_assignment_status_reports_ready_after_scan():
    controller = _make_controller()

    asyncio.run(controller.auto_rank_and_assign_strategies(timeframe="1h"))
    status = controller.strategy_auto_assignment_status()

    assert status["ready"] is True
    assert status["running"] is False
    assert status["assigned_symbols"] == 2
    assert status["completed"] == 2
    assert status["total"] == 2


def test_auto_rank_and_assign_strategies_restores_saved_assignments_without_rescanning():
    controller = _make_controller()
    controller.symbol_strategy_assignments = {
        "EUR/USD": _saved_assignment("EUR/USD", "EMA Cross"),
        "BTC/USDT": _saved_assignment("BTC/USDT", "MACD Trend", assignment_mode="ranked"),
    }
    controller._refresh_calls.clear()
    controller._ranker.calls.clear()

    result = asyncio.run(controller.auto_rank_and_assign_strategies(timeframe="1h"))
    status = controller.strategy_auto_assignment_status()

    assert result["assigned_symbols"] == []
    assert result["restored_symbols"] == ["EUR/USD", "BTC/USDT"]
    assert result["skipped_symbols"] == []
    assert controller._ranker.calls == []
    assert controller._refresh_calls == [True]
    assert status["ready"] is True
    assert status["message"] == "Loaded saved strategy assignments for 2 symbols."


def test_schedule_strategy_auto_assignment_only_scans_symbols_missing_saved_state():
    controller = _make_controller()
    controller.symbol_strategy_assignments = {
        "EUR/USD": _saved_assignment("EUR/USD", "EMA Cross"),
    }
    scheduled = {}

    async def fake_auto_rank_and_assign_strategies(symbols=None, timeframe=None, force=False):
        scheduled["symbols"] = list(symbols or [])
        scheduled["timeframe"] = timeframe
        scheduled["force"] = force
        return {"assigned_symbols": list(symbols or [])}

    controller.auto_rank_and_assign_strategies = fake_auto_rank_and_assign_strategies

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        task = controller.schedule_strategy_auto_assignment(symbols=controller.symbols, timeframe="1h", force=False)
        loop.run_until_complete(task)
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    assert scheduled["symbols"] == ["BTC/USDT"]
    assert scheduled["timeframe"] == "1h"
    assert scheduled["force"] is False


def test_auto_rank_and_assign_strategies_preserves_locked_default_symbols_on_restart():
    controller = _make_controller()
    controller.symbols = ["EUR/USD"]
    controller._mark_symbol_strategy_assignment_locked("EUR/USD", True)
    controller._refresh_calls.clear()
    controller._ranker.calls.clear()

    result = asyncio.run(controller.auto_rank_and_assign_strategies(timeframe="1h"))

    assert result["restored_symbols"] == ["EUR/USD"]
    assert controller._ranker.calls == []
    assert controller._refresh_calls == [True]
