from strategy.strategy import Strategy
from strategy.strategy_registry import StrategyRegistry


def test_compute_features_returns_empty_frame_for_invalid_candles():
    strategy = Strategy()

    df = strategy.compute_features([{"bad": "shape"}, ["too", "short"]])

    assert df.empty
    assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]


def test_generate_signal_skips_short_ohlcv_history():
    strategy = Strategy()
    candles = [
        [1700000000000 + i * 3600000, 100 + i, 101 + i, 99 + i, 100.5 + i, 10 + i]
        for i in range(10)
    ]

    assert strategy.generate_signal(candles) is None


def test_strategy_registry_can_switch_active_strategy():
    registry = StrategyRegistry()

    registry.set_active("Mean Reversion")

    resolved = registry._resolve_strategy()

    assert resolved.strategy_name == "Mean Reversion"


def test_strategy_registry_includes_expanded_strategy_library():
    registry = StrategyRegistry()

    available = set(registry.list())

    assert {
        "Trend Following",
        "Mean Reversion",
        "Breakout",
        "AI Hybrid",
        "EMA Cross",
        "Momentum Continuation",
        "Pullback Trend",
        "Volatility Breakout",
        "MACD Trend",
        "Range Fade",
    }.issubset(available)
    assert len(available) == 617
    assert "Trend Following | Scalp Conservative" in available
    assert "AI Hybrid | Institutional Prime" in available


def test_strategy_variant_names_resolve_to_base_signal_family():
    resolved = Strategy.resolve_signal_strategy_name("EMA Cross | London Session Aggressive")

    assert resolved == "EMA Cross"


def test_strategy_registry_applies_variant_parameters():
    registry = StrategyRegistry()

    strategy = registry.get("Trend Following | Scalp Conservative")

    assert strategy is not None
    assert strategy.strategy_name == "Trend Following | Scalp Conservative"
    assert strategy.ema_fast == 8
    assert strategy.ema_slow == 21
    assert strategy.min_confidence == 0.64
    assert strategy.signal_amount == 0.50


def test_breakout_strategy_generates_buy_signal_on_range_break():
    strategy = Strategy(strategy_name="Breakout")
    strategy.rsi_period = 2
    strategy.breakout_lookback = 5
    strategy.ema_fast = 3
    strategy.ema_slow = 5
    strategy.atr_period = 2

    candles = []
    base = 1700000000000
    rows = [
        (100, 101, 99, 100.0),
        (100, 101, 99.5, 100.3),
        (100.2, 101.2, 99.8, 100.6),
        (100.4, 101.4, 100.0, 100.9),
        (100.8, 101.6, 100.4, 101.1),
        (101.2, 105.0, 101.0, 104.8),
    ]
    for index, (open_, high, low, close) in enumerate(rows):
        candles.append([base + index * 3600000, open_, high, low, close, 10 + index])

    signal = strategy.generate_signal(candles)

    assert signal is not None
    assert signal["side"] == "buy"


def test_ema_cross_strategy_generates_buy_signal_on_bullish_cross():
    strategy = Strategy(strategy_name="EMA Cross")
    strategy.rsi_period = 2
    strategy.ema_fast = 2
    strategy.ema_slow = 4
    strategy.atr_period = 2

    candles = []
    base = 1700000000000
    closes = [105.0, 104.0, 103.0, 102.0, 103.0, 104.5]
    prev_close = closes[0]
    for index, close in enumerate(closes):
        open_ = prev_close
        high = max(open_, close) + 0.6
        low = min(open_, close) - 0.6
        candles.append([base + index * 3600000, open_, high, low, close, 10 + index])
        prev_close = close

    signal = strategy.generate_signal(candles)

    assert signal is not None
    assert signal["side"] == "buy"
    assert "EMA fast crossed above EMA slow" in signal["reason"]
