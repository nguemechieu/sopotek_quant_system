# strategy/strategy.py

import pandas as pd
import numpy as np

from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange


class Strategy:
    PRESET_ALIASES = {
        "DEFAULT": "Trend Following",
        "EMA_RSI": "Trend Following",
        "TREND": "Trend Following",
        "TREND FOLLOWING": "Trend Following",
        "MEAN REVERSION": "Mean Reversion",
        "MEAN_REVERSION": "Mean Reversion",
        "BREAKOUT": "Breakout",
        "AI": "AI Hybrid",
        "AI HYBRID": "AI Hybrid",
        "LSTM": "AI Hybrid",
    }

    def __init__(self, model=None, strategy_name="Trend Following"):

        self.model = model
        self.strategy_name = self.normalize_strategy_name(strategy_name)

        # Strategy parameters
        self.rsi_period = 14
        self.ema_fast = 20
        self.ema_slow = 50
        self.atr_period = 14
        self.oversold_threshold = 35
        self.overbought_threshold = 65
        self.breakout_lookback = 20
        self.signal_amount = 1.0

        self.min_confidence = 0.55

    @classmethod
    def normalize_strategy_name(cls, strategy_name):
        label = str(strategy_name or "Trend Following").strip()
        if not label:
            return "Trend Following"
        return cls.PRESET_ALIASES.get(label.upper(), label)

    def set_strategy_name(self, strategy_name):
        self.strategy_name = self.normalize_strategy_name(strategy_name)

    def apply_parameters(self, **params):
        for key, value in params.items():
            if hasattr(self, key):
                setattr(self, key, value)

    # ==========================================================
    # FEATURE ENGINEERING
    # ==========================================================

    def compute_features(self, candles):
        if not candles:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        normalized = []
        for row in candles:
            if isinstance(row, (list, tuple)) and len(row) >= 6:
                normalized.append(list(row[:6]))

        if not normalized:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        df = pd.DataFrame(
            normalized,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df.dropna(subset=numeric_cols, inplace=True)

        if len(df) < max(self.ema_slow, self.atr_period, self.rsi_period):
            return pd.DataFrame(columns=df.columns)

        # Indicators
        df["rsi"] = RSIIndicator(df["close"], self.rsi_period).rsi()

        df["ema_fast"] = EMAIndicator(
            df["close"], self.ema_fast
        ).ema_indicator()

        df["ema_slow"] = EMAIndicator(
            df["close"], self.ema_slow
        ).ema_indicator()

        df["atr"] = AverageTrueRange(
            df["high"],
            df["low"],
            df["close"],
            self.atr_period
        ).average_true_range()

        bb_period = max(self.ema_fast, 2)
        rolling_mean = df["close"].rolling(window=bb_period, min_periods=1).mean()
        rolling_std = df["close"].rolling(window=bb_period, min_periods=1).std().fillna(0.0)
        df["upper_band"] = rolling_mean + (2.0 * rolling_std)
        df["lower_band"] = rolling_mean - (2.0 * rolling_std)

        breakout_period = max(int(self.breakout_lookback), 2)
        df["breakout_high"] = df["high"].rolling(window=breakout_period, min_periods=1).max().shift(1)
        df["breakout_low"] = df["low"].rolling(window=breakout_period, min_periods=1).min().shift(1)

        df.dropna(inplace=True)

        return df

    # ==========================================================
    # SIGNAL GENERATION
    # ==========================================================

    def generate_signal(self, candles, strategy_name=None):
        selected_name = self.normalize_strategy_name(strategy_name or self.strategy_name)
        if selected_name == "AI Hybrid":
            ai_signal = self.generate_ai_signal(candles)
            if ai_signal:
                return ai_signal
            selected_name = "Trend Following"

        df = self.compute_features(candles)

        if df.empty:
            return None

        row = df.iloc[-1]
        close_price = float(row["close"])

        # Trend
        trend_up = row["ema_fast"] > row["ema_slow"]
        trend_down = row["ema_fast"] < row["ema_slow"]

        # RSI
        rsi = row["rsi"]

        if selected_name == "Trend Following":
            if trend_up and rsi < self.oversold_threshold:
                return {
                    "side": "buy",
                    "amount": self.signal_amount,
                    "confidence": 0.60,
                    "reason": "EMA trend up + RSI oversold"
                }
            if trend_down and rsi > self.overbought_threshold:
                return {
                    "side": "sell",
                    "amount": self.signal_amount,
                    "confidence": 0.60,
                    "reason": "EMA trend down + RSI overbought"
                }

        elif selected_name == "Mean Reversion":
            if close_price <= float(row["lower_band"]) and rsi <= self.oversold_threshold:
                return {
                    "side": "buy",
                    "amount": self.signal_amount,
                    "confidence": 0.58,
                    "reason": "Lower band reversion + RSI oversold"
                }
            if close_price >= float(row["upper_band"]) and rsi >= self.overbought_threshold:
                return {
                    "side": "sell",
                    "amount": self.signal_amount,
                    "confidence": 0.58,
                    "reason": "Upper band reversion + RSI overbought"
                }

        elif selected_name == "Breakout":
            breakout_high = row.get("breakout_high")
            breakout_low = row.get("breakout_low")
            if pd.notna(breakout_high) and close_price > float(breakout_high) and trend_up:
                return {
                    "side": "buy",
                    "amount": self.signal_amount,
                    "confidence": 0.62,
                    "reason": "Breakout above prior range high"
                }
            if pd.notna(breakout_low) and close_price < float(breakout_low) and trend_down:
                return {
                    "side": "sell",
                    "amount": self.signal_amount,
                    "confidence": 0.62,
                    "reason": "Breakout below prior range low"
                }

        return None

    # ==========================================================
    # AI SIGNAL
    # ==========================================================

    def generate_ai_signal(self, candles):

        if self.model is None:
            return None

        df = self.compute_features(candles)

        if df.empty:
            return None

        features = df.iloc[-1][[
            "rsi",
            "ema_fast",
            "ema_slow",
            "atr",
            "volume"
        ]].values.reshape(1, -1)

        prob = self.model.predict_proba(features)[0]

        confidence = max(prob)

        if confidence < self.min_confidence:
            return None

        side = "buy" if prob[1] > prob[0] else "sell"

        return {
            "side": side,
            "amount": 1,
            "confidence": float(confidence),
            "reason": "AI model prediction"
        }
