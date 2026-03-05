import os
import joblib
import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split

from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam

from sopotek_trading.backend.utils.utils import candles_to_df


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def _add_features(data):

    # Accept list (ccxt) OR DataFrame
    if isinstance(data, list):
        df = candles_to_df(data)
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError("Unsupported data format")

    df = df.sort_index()

    # Log returns
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # True range / ATR
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1))
        )
    )

    df["atr"] = df["tr"].rolling(14).mean()

    # Trend filters
    df["ema_fast"] = df["close"].ewm(span=20).mean()
    df["ema_slow"] = df["close"].ewm(span=50).mean()

    df["trend"] = (df["ema_fast"] > df["ema_slow"]).astype(int)

    # Volatility regime
    df["volatility"] = df["log_return"].rolling(20).std()

    df["high_vol_regime"] = (
            df["volatility"] > df["volatility"].rolling(100).mean()
    ).astype(int)

    df = df.dropna()

    return df


# ============================================================
# INSTITUTIONAL ML ENGINE
# ============================================================

class MLSignal:

    def __init__(self, lookback=60):

        self.lookback = lookback
        self.model = None
        self.scaler = MinMaxScaler()
        self.is_trained = False

        self.feature_cols = [
            "close",
            "atr",
            "volatility",
            "trend",
            "high_vol_regime"
        ]

    # ========================================================
    # PREPARE DATA
    # ========================================================

    def _prepare_data(self, data):

        df = _add_features(data)

        if len(df) <= self.lookback:
            return None

        features = df[self.feature_cols].values
        scaled = self.scaler.fit_transform(features)

        X, y = [], []

        for i in range(self.lookback, len(scaled)):
            X.append(scaled[i - self.lookback:i])

            future_return = df["log_return"].iloc[i]
            y.append(1 if future_return > 0 else 0)

        return np.array(X), np.array(y)

    # ========================================================
    # TRAIN
    # ========================================================

    def train(self, data):

        prepared = self._prepare_data(data)

        if prepared is None:
            print("Not enough data to train.")
            return False

        X, y = prepared

        if len(X) < 10:
            print("Insufficient sequences.")
            return False

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )

        self.model = Sequential([
            Input(shape=(self.lookback, len(self.feature_cols))),

            LSTM(128, return_sequences=True),
            Dropout(0.3),

            LSTM(64),
            Dropout(0.3),

            Dense(32, activation="relu"),
            Dense(1, activation="sigmoid")
        ])

        self.model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss="binary_crossentropy",
            metrics=["accuracy"]
        )

        early_stop = EarlyStopping(
            monitor="val_loss",
            patience=5,
            restore_best_weights=True
        )

        self.model.fit(
            X_train,
            y_train,
            validation_data=(X_val, y_val),
            epochs=50,
            batch_size=64,
            callbacks=[early_stop],
            verbose=0
        )

        self.is_trained = True
        return True

    # ========================================================
    # PREDICT
    # ========================================================

    def predict(self, data):

        if not self.is_trained or self.model is None:
            raise Exception("Model not trained.")

        df = _add_features(data)

        if len(df) <= self.lookback:
            return {
                "signal": "HOLD",
                "confidence": 0.0,
                "probability_up": 0.5,
                "volatility": 0.0,
                "regime": "unknown",
                "current_price": 0.0
            }

        features = df[self.feature_cols].values
        scaled = self.scaler.transform(features)

        X = scaled[-self.lookback:]
        X = np.reshape(X, (1, self.lookback, len(self.feature_cols)))

        probability_up = float(self.model.predict(X, verbose=0)[0][0])

        current_price = float(df["close"].iloc[-1])
        current_volatility = float(df["volatility"].iloc[-1])
        regime = "high_vol" if df["high_vol_regime"].iloc[-1] else "normal"

        if probability_up > 0.60:
            signal = "BUY"
        elif probability_up < 0.40:
            signal = "SELL"
        else:
            signal = "HOLD"

        confidence = abs(probability_up - 0.5) * 2

        return {
            "signal": signal,
            "confidence": confidence,
            "probability_up": probability_up,
            "volatility": current_volatility,
            "regime": regime,
            "current_price": current_price
        }

    # ========================================================
    # SAVE / LOAD
    # ========================================================

    def save(self, path):

        if not self.model:
            return

        os.makedirs(path, exist_ok=True)

        model_path = os.path.join(path, "model.keras")
        scaler_path = os.path.join(path, "scaler.pkl")

        self.model.save(model_path)
        joblib.dump(self.scaler, scaler_path)

    def load(self, path):

        model_path = os.path.join(path, "model.keras")
        scaler_path = os.path.join(path, "scaler.pkl")

        self.model = load_model(model_path)
        self.scaler = joblib.load(scaler_path)

        self.is_trained = True