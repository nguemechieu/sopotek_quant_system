import numpy as np


class RegimeDetector:

    def detect(self, df):

        returns = df["close"].pct_change().dropna()

        volatility = returns.std()

        trend = np.polyfit(range(len(df)), df["close"], 1)[0]

        if volatility > 0.04:
            return "high_volatility"

        if trend > 0:
            return "uptrend"

        if trend < 0:
            return "downtrend"

        return "sideways"