import sys
import threading
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backtesting.backtest_engine import BacktestEngine
from backtesting.report_generator import ReportGenerator
from backtesting.simulator import Simulator


class SequenceStrategy:
    def generate_signal(self, candles, strategy_name=None):
        length = len(candles)
        if length == 3:
            return {"side": "buy", "amount": 1, "reason": "entry"}
        if length == 5:
            return {"side": "sell", "amount": 1, "reason": "exit"}
        return None


def make_frame():
    return pd.DataFrame(
        [
            [1, 100, 101, 99, 100, 10],
            [2, 101, 102, 100, 101, 11],
            [3, 102, 103, 101, 102, 12],
            [4, 103, 104, 102, 103, 13],
            [5, 110, 111, 109, 110, 14],
            [6, 111, 112, 110, 111, 15],
        ],
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )


def test_backtest_engine_executes_trades_and_builds_equity_curve():
    engine = BacktestEngine(strategy=SequenceStrategy(), simulator=Simulator(initial_balance=1000))

    results = engine.run(make_frame(), symbol="BTC/USDT")

    assert len(results) == 2
    assert list(results["side"]) == ["BUY", "SELL"]
    assert results.iloc[-1]["pnl"] == 8.0
    assert len(engine.equity_curve) == 6


def test_backtest_engine_closes_open_position_at_end():
    class HoldUntilEnd:
        def generate_signal(self, candles, strategy_name=None):
            if len(candles) == 2:
                return {"side": "buy", "amount": 1}
            return None

    engine = BacktestEngine(strategy=HoldUntilEnd(), simulator=Simulator(initial_balance=1000))
    results = engine.run(make_frame(), symbol="ETH/USDT")

    assert len(results) == 2
    assert results.iloc[-1]["reason"] == "end_of_test"
    assert results.iloc[-1]["side"] == "SELL"


def test_backtest_engine_respects_stop_event():
    class HoldStrategy:
        def generate_signal(self, candles, strategy_name=None):
            if len(candles) == 2:
                return {"side": "buy", "amount": 1}
            return None

    stop_event = threading.Event()
    stop_event.set()
    engine = BacktestEngine(strategy=HoldStrategy(), simulator=Simulator(initial_balance=1000))

    results = engine.run(make_frame(), symbol="ETH/USDT", stop_event=stop_event)

    assert results.empty
    assert engine.equity_curve == []


def test_report_generator_exports_files(tmp_path):
    trades = pd.DataFrame(
        [
            {"timestamp": 1, "symbol": "BTC/USDT", "side": "BUY", "type": "ENTRY", "price": 100, "amount": 1, "pnl": 0.0, "equity": 1000},
            {"timestamp": 2, "symbol": "BTC/USDT", "side": "SELL", "type": "EXIT", "price": 110, "amount": 1, "pnl": 10.0, "equity": 1010},
        ]
    )
    generator = ReportGenerator(trades=trades, equity_history=[1000, 1010], output_dir=tmp_path)

    report = generator.generate()
    pdf_path = generator.export_pdf()
    sheet_path = generator.export_excel()

    assert report["total_profit"] == 10.0
    assert report["closed_trades"] == 1
    assert pdf_path.exists()
    assert sheet_path.exists()
