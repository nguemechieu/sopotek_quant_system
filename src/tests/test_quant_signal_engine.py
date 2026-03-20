from quant.signal_engine import SignalEngine
from strategy.strategy_registry import StrategyRegistry


def _sample_candles():
    base = 1700000000000
    rows = [
        (100, 101, 99, 100.0),
        (100, 101, 99.5, 100.3),
        (100.2, 101.2, 99.8, 100.6),
        (100.4, 101.4, 100.0, 100.9),
        (100.8, 101.6, 100.4, 101.1),
        (101.2, 105.0, 101.0, 104.8),
    ]
    candles = []
    for index, (open_, high, low, close) in enumerate(rows):
        candles.append([base + index * 3600000, open_, high, low, close, 10 + index])
    return candles


def test_signal_engine_adds_regime_and_engine_metadata():
    registry = StrategyRegistry()
    strategy = registry.get("Breakout")
    strategy.rsi_period = 2
    strategy.breakout_lookback = 5
    strategy.ema_fast = 3
    strategy.ema_slow = 5
    strategy.atr_period = 2

    engine = SignalEngine(registry)
    signal = engine.generate_signal(
        candles=_sample_candles(),
        strategy_name="Breakout",
        symbol="BTC/USDT",
    )

    assert signal is not None
    assert signal["side"] == "buy"
    assert signal["symbol"] == "BTC/USDT"
    assert signal["regime"] in {"trending_up", "trending_down", "range", "volatile_range", "range_low_edge", "range_high_edge"}
    assert signal["signal_engine_version"] == "signal-engine-v1"
