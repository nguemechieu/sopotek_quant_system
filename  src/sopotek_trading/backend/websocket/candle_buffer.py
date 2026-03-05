import logging
from collections import defaultdict

import pandas as pd


class CandleBuffer:

    def __init__(self, max_length=200):
        self.logger = logging.getLogger(__name__)
        self.max_length = max_length
        self.buffers = defaultdict(
            lambda: pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
        )

    # ======================================================
    # UPDATE BUFFER
    # ======================================================

    def update(self, symbol, candle):
        
        
        print("candle",candle)
        df = self.buffers[symbol]

        new_row = {
            "timestamp":int( candle["timestamp"]),
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "volume": float(candle["volume"]),
        }
        

        # Faster than concat
        df.loc[len(df)] = new_row

        # Keep last N rows only
        if len(df) > self.max_length:
            self.buffers[symbol] = df.iloc[-self.max_length:].reset_index(drop=True)

    # ======================================================
    # GET DATA
    # ======================================================

    def get(self, symbol="EURUSD"):

        df = self.buffers.get(symbol)

        if df is None or df.empty:
            return None

        return df.copy()

    def set(self, symbol: str, df):
     """
        Replace full historical dataframe for a symbol.
        Used for initial model training.
        """

     if df is None or df.empty:
        return

    # ---------------------------------
    # Ensure required columns exist
    # ---------------------------------
     required_cols = {"timestamp", "open", "high", "low", "close", "volume"}

     if not required_cols.issubset(df.columns):
        raise ValueError(
            f"CandleBuffer.set(): Missing columns for {symbol}"
        )

    # ---------------------------------
    # Sort by timestamp (critical)
    # ---------------------------------
     df = df.sort_values("timestamp")

    # ---------------------------------
    # Trim to max_length
    # ---------------------------------
     if len(df) > self.max_length:
        df = df.iloc[-self.max_length:]

    # ---------------------------------
    # Reset index for clean operations
    # ---------------------------------
     df = df.reset_index(drop=True)

    # ---------------------------------
    # Store COPY to avoid mutation bugs
    # ---------------------------------
     self.buffers[symbol] = df.copy()

