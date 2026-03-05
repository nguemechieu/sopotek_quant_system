class MomentumStrategy:

    async def generate_signal(self, symbol, df):

        if len(df) < 50:
            return None

        ma_fast = df["close"].rolling(10).mean().iloc[-1]
        ma_slow = df["close"].rolling(30).mean().iloc[-1]

        price = df["close"].iloc[-1]

        if ma_fast > ma_slow:

            return {
                "symbol": symbol,
                "signal": "BUY",
                "entry_price": price,
                "confidence": 0.7,
            }

        if ma_fast < ma_slow:

            return {
                "symbol": symbol,
                "signal": "SELL",
                "entry_price": price,
                "confidence": 0.7,

            }

        return None