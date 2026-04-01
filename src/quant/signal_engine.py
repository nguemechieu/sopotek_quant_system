from typing import Any

from quant.regime_engine import RegimeEngine


class SignalEngine:
    VERSION = "signal-engine-v1"

    def __init__(self, strategy_registry, regime_engine=None):
        self.strategy_registry = strategy_registry
        self.regime_engine = regime_engine or RegimeEngine()

    def _resolve_strategy(self, strategy_name=None):
        if hasattr(self.strategy_registry, "resolve_strategy"):
            return self.strategy_registry.resolve_strategy(strategy_name)

        resolve_method = getattr(self.strategy_registry, "_resolve_strategy", None)
        if callable(resolve_method):
            return resolve_method(strategy_name)

        return self.strategy_registry

    def generate_signal(self, candles=None, dataset=None, strategy_name=None, symbol=None):
        strategy: Any = self._resolve_strategy(strategy_name)
        if strategy is None:
            return None

        feature_frame = None
        if dataset is not None and getattr(dataset, "frame", None) is not None:
            feature_frame = strategy.compute_features(dataset.to_candles())
        elif candles is not None:
            feature_frame = strategy.compute_features(candles)

        if feature_frame is None or feature_frame.empty:
            return None

        regime = self.regime_engine.classify_frame(feature_frame)

        signal = None
        if hasattr(strategy, "generate_signal_from_features"):
            signal = strategy.generate_signal_from_features(feature_frame, strategy_name=strategy_name)
        elif candles is not None and hasattr(strategy, "generate_signal"):
            signal = strategy.generate_signal(candles, strategy_name=strategy_name)

        if not signal:
            return None

        normalized = dict(signal)
        normalized.setdefault("regime", regime)
        normalized.setdefault("feature_version", feature_frame.iloc[-1].get("feature_version", "quant-v1"))
        normalized.setdefault("signal_engine_version", self.VERSION)
        if symbol:
            normalized["symbol"] = str(symbol).upper().strip()
        if strategy_name:
            normalized["strategy_name"] = str(strategy_name)
        return normalized
