import logging
import pandas as pd

from ta.volatility import AverageTrueRange

logger = logging.getLogger(__name__)


class BreakoutStrategy:

    name = "breakout"

    async def generate_signal(self, symbol: str, df: pd.DataFrame):

        try:

            if len(df) < 50:
                return None

            df["atr"] = AverageTrueRange(
                df["high"],
                df["low"],
                df["close"],
                window=14
            ).average_true_range()

            df["rolling_high"] = df["high"].rolling(20).max()
            df["rolling_low"] = df["low"].rolling(20).min()

            last = df.iloc[-1]

            price = last["close"]
            atr = last["atr"]

            high_break = last["rolling_high"]
            low_break = last["rolling_low"]

            # BUY breakout
            if price > high_break:

                stop = price - atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "BUY",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": 0.8,
                    "volatility": atr
                }

            # SELL breakout
            if price < low_break:

                stop = price + atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "SELL",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": 0.8,
                    "volatility": atr
                }

            return None

        except Exception as e:

            logger.error(f"BreakoutStrategy error {symbol}: {e}")

            return None