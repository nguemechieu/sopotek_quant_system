from sopotek_trading.backend.strategy.breakout_strategy import BreakoutStrategy
from sopotek_trading.backend.strategy.mean_reversion_strategy import MeanReversionStrategy
from sopotek_trading.backend.strategy.ml_strategy import MLStrategy
from sopotek_trading.backend.strategy.regime_detector import RegimeDetector
from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy


class StrategyOrchestrator:

    def __init__(self, strategies, metrics):

        self.model_name = 'ml'
        self.regime = RegimeDetector()
        self.strategies = strategies
        self.metrics = metrics

        if self.regime == "uptrend":

            self.strategies = [TrendStrategy(), BreakoutStrategy()]

        elif self.regime == "sideways":
 
            self.strategies = [MeanReversionStrategy()]

        elif self.regime == "high_volatility":

         self.strategies = [BreakoutStrategy()]
        elif self.regime == "mean_reversion":
            self.strategies = [MeanReversionStrategy()]

        else:
            self.strategies = [MLStrategy(model=self.model_name)]




    async def generate_signal(self, symbol, data):

        best_score = -1
        best_signal = None

        for strategy in self.strategies:

            signal = await strategy.generate_signal(symbol, data)

            if not signal:
                continue

            name = strategy.name

            win_rate = self.metrics.win_rate(name)

            confidence = signal.get("confidence", 0.5)

            score = win_rate * confidence

            if score > best_score:

                best_score = score
                best_signal = signal

        return best_signal