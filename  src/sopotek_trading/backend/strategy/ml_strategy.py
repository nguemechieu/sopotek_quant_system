import logging

logger = logging.getLogger(__name__)


class MLStrategy:

    name = "../../models/ml"

    def __init__(self, model):

        self.model = model

    async def generate_signal(self, symbol, df):

        try:

            features = df.iloc[-1]

            prob_up = self.model.predict(features)

            price = features["close"]

            atr = features.get("atr", 1)

            if prob_up > 0.65:

                stop = price - atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "BUY",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": prob_up,
                    "volatility": atr
                }

            if prob_up < 0.35:

                stop = price + atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "SELL",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": 1 - prob_up,
                    "volatility": atr
                }

            return None

        except Exception as e:

            logger.error(f"MLStrategy error {symbol}: {e}")

            return None