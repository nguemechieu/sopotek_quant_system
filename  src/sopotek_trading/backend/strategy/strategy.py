

from sopotek_trading.backend.models.ml.ml_models_manager import MLModelManager
from sopotek_trading.backend.research.regime import RegimeDetector
from sopotek_trading.backend.strategy.breakout_strategy import BreakoutStrategy
from sopotek_trading.backend.strategy.macd_strategy import MACDStrategy
from sopotek_trading.backend.strategy.mean_reversion_strategy import MeanReversionStrategy
from sopotek_trading.backend.strategy.momentum_strategy import MomentumStrategy
from sopotek_trading.backend.strategy.orderbook_strategy import OrderbookStrategy
from sopotek_trading.backend.strategy.rsi_strategy_ import RSIStrategy
from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy
from sopotek_trading.backend.utils.utils import candles_to_df


class Strategy:

    def __init__(self, controller):

        self.logger = controller.logger
        self.controller = controller

        # Use a proper models directory
        self.model_manager = MLModelManager(
            controller=self.controller,
            model_dir="../../models/ml"
        )

        self.strategies = [TrendStrategy(), BreakoutStrategy(),
                           MLModelManager(controller=self.controller),MACDStrategy(),RSIStrategy(),
                           MeanReversionStrategy(),OrderbookStrategy(), MomentumStrategy()]

    async def generate_signal(self, symbol: str, df):
        if not symbol or not df:
            return {
                "symbol": symbol,
                "signal": "HOLD",
                "entry_price": 0,
                "stop_price": 0,
                "confidence": 0,
                "volatility":0,
                "regime":"Not specified!"
            }


        # Ensure symbol is registered
        self.model_manager.register_symbol(symbol)

        # Ensure model is trained
        if not self.model_manager.is_trained(symbol):
            await self.model_manager.train(symbol, df)
             # Wait for next cycle
            return {
                "symbol": symbol,
                "signal": "HOLD",
                "entry_price": 0,
                "stop_price": 0,
                "confidence": 0,
                "volatility":0,
                "regime":"Wait for next cycle!"
            }


        # Predict
        prediction = self.model_manager.predict(symbol, df)

        if prediction is None:
            return {
                "symbol": symbol,
                "signal": "HOLD",
                "entry_price": 0,
                "stop_price": 0,
                "confidence": 0,
                "volatility":0,
                "regime":RegimeDetector().detect(prices=candles_to_df(df))
            }

        signal = prediction


        entry_price = float(df["close"].iloc[-1])

        return {
            "symbol": symbol,
            "signal": signal,
            "entry_price": entry_price,
            "stop_price": entry_price * 0.99,
            "confidence": float(prediction["confidence"]),
            "volatility": float(df["close"].pct_change().std())
        }

    def get_strategies(self):

        return  self.controller.strategies