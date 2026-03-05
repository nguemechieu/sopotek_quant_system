import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TrendStrategy:

    name = "trend"

    async def generate_signal(self, symbol: str, df):

        try:

            df = df[df["symbol"] == symbol]

            if len(df) < 200:
                return None

            last = df.iloc[-1]

            price = last["close"]

            ema50 = last.get("ema50")
            ema200 = last.get("ema200")

            rsi = last.get("rsi")
            atr = last.get("atr")

            if any(pd.isna(v) for v in [ema50, ema200, rsi, atr]):
                return None

            # ---------------- BUY ----------------

            if ema50 > ema200 and rsi > 55 and price > ema50:

                stop = price - atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "BUY",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": min((rsi - 50) / 50, 1),
                    "volatility": atr
                }

            # ---------------- SELL ----------------

            if ema50 < ema200 and rsi < 45 and price < ema50:

                stop = price + atr * 2

                return {
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": "SELL",
                    "entry_price": price,
                    "stop_price": stop,
                    "confidence": min((50 - rsi) / 50, 1),
                    "volatility": atr
                }

            return None

        except Exception as e:

            logger.error(f"TrendStrategy error {symbol}: {e}")

            return None