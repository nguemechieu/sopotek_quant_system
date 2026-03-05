import numpy as np


from sklearn.preprocessing import StandardScaler
from hmmlearn.hmm import GaussianHMM


class MarkovRegimeDetector:

    def __init__(self, n_regimes=4):
        self.n_regimes = n_regimes
        self.model = GaussianHMM(
            n_components=n_regimes,
            covariance_type="full",
            n_iter=500,
            random_state=42
        )

        self.scaler = StandardScaler()
        self.fitted = False
        self.regime_map = {}

    # ==========================================================
    # FEATURE ENGINEERING
    # ==========================================================

    def _build_features(self, df):

        df = df.copy()

        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        df["volatility"] = df["log_return"].rolling(20).std()

        df["ema_fast"] = df["close"].ewm(span=20).mean()
        df["ema_slow"] = df["close"].ewm(span=50).mean()

        df["trend_strength"] = (
                (df["ema_fast"] - df["ema_slow"]) / df["close"]
        )

        df["atr"] = (
            (df["high"] - df["low"]).rolling(14).mean()
        )

        features = df[[
            "log_return",
            "volatility",
            "trend_strength",
            "atr"
        ]].dropna()

        return features

    # ==========================================================
    # FIT
    # ==========================================================

    def fit(self, df):

        features = self._build_features(df)

        scaled = self.scaler.fit_transform(features)

        self.model.fit(scaled)

        hidden_states = self.model.predict(scaled)

        self._map_regimes(features, hidden_states)

        self.fitted = True

    # ==========================================================
    # MAP STATES TO HUMAN LABELS
    # ==========================================================

    def _map_regimes(self, features, states):

        features = features.copy()
        features["state"] = states

        regime_map = {}

        for state in range(self.n_regimes):

            state_data = features[features["state"] == state]

            avg_return = state_data["log_return"].mean()
            avg_vol = state_data["volatility"].mean()

            if avg_vol > features["volatility"].mean() * 1.4:
                regime_map[state] = "HIGH_VOL"

            elif avg_return > 0:
                regime_map[state] = "TREND_UP"

            elif avg_return < 0:
                regime_map[state] = "TREND_DOWN"

            else:
                regime_map[state] = "SIDEWAYS"

        self.regime_map = regime_map

    # ==========================================================
    # PREDICT CURRENT REGIME
    # ==========================================================

    def predict(self, df):

        if not self.fitted:
            raise Exception("MarkovRegimeDetector must be fitted first.")

        features = self._build_features(df)

        if features.empty:
            return {
                "regime": "UNKNOWN",
                "probability": 0.0
            }

        scaled = self.scaler.transform(features)

        probabilities = self.model.predict_proba(scaled)

        last_probs = probabilities[-1]

        state = np.argmax(last_probs)
        regime = self.regime_map.get(state, "UNKNOWN")

        return {
            "regime": regime,
            "probability": float(last_probs[state]),
            "state_vector": last_probs.tolist()
        }