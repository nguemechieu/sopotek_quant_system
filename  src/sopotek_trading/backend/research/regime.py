import numpy as np
import pandas as pd


class RegimeDetector:

    def __init__(self, window=50):
        self.window = window

    def detect(self, prices):
        returns = np.diff(np.log(prices))
        vol = np.std(returns[-self.window:])

        trend = prices[-1] - np.mean(prices[-self.window:])

        if vol > 0.03:
            return "HIGH_VOL"
        elif trend > 0:
            return "BULL"
        else:
            return "BEAR"


#gmm = GaussianMixture(n_components=3)
#regimes = gmm.fit_predict(macro_features)

#
# if regime == "CRISIS":
#     risk_multiplier = 0.3
#
# elif avg_corr > 0.8:
#     risk_multiplier = 0.5
#
# elif forecast_vol > threshold:
#     risk_multiplier = 0.6
#
# else:
#     risk_multiplier = 1.0
#
# final_position_size = base_size * risk_multiplier