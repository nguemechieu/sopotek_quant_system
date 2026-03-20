from strategy.strategy import Strategy


class StrategyRegistry:

    def __init__(self):
        self.strategies = {}
        self.active_name = None
        self.default_strategy = Strategy()
        self._register_builtin_strategies()

    def _register_builtin_strategies(self):
        for definition in Strategy.STRATEGY_CATALOG:
            name = str(definition.get("name") or "").strip()
            if not name:
                continue
            if name not in self.strategies:
                strategy = Strategy(strategy_name=name)
                params = dict(definition.get("params") or {})
                if params:
                    strategy.apply_parameters(**params)
                self.register(name, strategy)

    # ===============================
    # REGISTER
    # ===============================

    def register(self, name, strategy):
        self.strategies[name] = strategy
        if self.active_name is None:
            self.active_name = name

    # ===============================
    # GET STRATEGY
    # ===============================

    def get(self, name):
        normalized = Strategy.normalize_strategy_name(name)
        return self.strategies.get(normalized)

    # ===============================
    # LIST STRATEGIES
    # ===============================

    def list(self):
        return list(self.strategies.keys())

    def set_active(self, name):
        normalized = Strategy.normalize_strategy_name(name)
        if normalized in self.strategies:
            self.active_name = normalized

    def configure(self, strategy_name=None, params=None):
        target_name = Strategy.normalize_strategy_name(strategy_name or self.active_name)
        self.set_active(target_name)
        target = self._resolve_strategy(target_name)
        if hasattr(target, "set_strategy_name"):
            target.set_strategy_name(target_name)
        if isinstance(params, dict) and hasattr(target, "apply_parameters"):
            target.apply_parameters(**params)
        return target

    def _resolve_strategy(self, strategy_name=None):
        normalized = Strategy.normalize_strategy_name(strategy_name) if strategy_name else None
        if normalized and normalized in self.strategies:
            selected = self.strategies[normalized]
            if selected is not self:
                return selected

        if self.active_name and self.active_name in self.strategies:
            selected = self.strategies[self.active_name]
            if selected is not self:
                return selected

        if self.strategies:
            first = next(iter(self.strategies.values()))
            if first is not self:
                return first

        return self.default_strategy

    def generate_ai_signal(self, candles, strategy_name=None):
        strategy = self._resolve_strategy(strategy_name)

        if hasattr(strategy, "generate_ai_signal"):
            signal = strategy.generate_ai_signal(candles)
            if signal:
                return signal

        if hasattr(strategy, "generate_signal"):
            return strategy.generate_signal(candles)

        return None

    def generate_signal(self, candles, strategy_name=None):
        # Prefer AI path when available; fallback to classical rule-based signal.
        return self.generate_ai_signal(candles, strategy_name=strategy_name)
