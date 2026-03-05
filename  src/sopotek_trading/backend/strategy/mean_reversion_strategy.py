import logging
import pandas as pd

from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

logger = logging.getLogger(__name__)


class MeanReversionStrategy:

    name = "mean_reversion"

    async def generate_signal(self, symbol: str, df: pd.DataFrame):

        try:

            if len(df) < 100:
                return None

            close = df["close"]

            bb = BollingerBands(close, window=20, window_dev=2)

            df["bb_high"] = bb.bollinger_hband()
            df["bb_low"] = bb.bollinger_lband()

            df["rsi"] = RSIIndicator(close, window=14).rsi()

            df["atr"] = AverageTrueRange(
                df["high"],
                df["low"],
                df["close"],
                window=14
            ).average_true_range()

            last = df.iloc[-1]

            price = last["close"]
            rsi = last["rsi"]

            bb_high = last["bb_high"]
            bb_low = last["bb_low"]

            atr = last["atr"]

            # BUY
            if price < bb_low and rsi < 30:

                stop = price - atr * 1.5

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "BUY",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": min((30 - rsi) / 30, 1),
                    "volatility": atr
                }

            # SELL
            if price > bb_high and rsi > 70:

                stop = price + atr * 1.5

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "SELL",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": min((rsi - 70) / 30, 1),
                    "volatility": atr
                }

            return None

        except Exception as e:

            logger.error(f"MeanReversionStrategy error {symbol}: {e}")

            return None