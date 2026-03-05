import pandas as pd


class CandleBuffer:

    def __init__(self, max_length=500):
        self.max_length = max_length
        self.buffers = {}

    # ==========================================
    # UPDATE (Add new candle)
    # ==========================================

    def update(self, symbol, candle):

        """
        candle format:
        {
            "timestamp": int,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": float
        }
        """

        if symbol not in self.buffers:
            self.buffers[symbol] = []

        self.buffers[symbol].append(candle)

        # Keep max length
        if len(self.buffers[symbol]) > self.max_length:
            self.buffers[symbol].pop(0)

    # ==========================================
    # GET DATAFRAME
    # ==========================================

    def get(self, symbol):

        if symbol not in self.buffers:
            return None

        data = self.buffers[symbol]

        if not data:
            return None

        df = pd.DataFrame(data)

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)

        return df.copy()