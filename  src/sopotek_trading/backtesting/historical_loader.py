import ccxt
import pandas as pd

from sopotek_trading.backend.risk.institutional_risk import logger


def load_binance_data(symbol="BTC/USDT", timeframe="1h", limit=1000):

    exchange = ccxt.binanceus()

    candles = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )
    logger.debug(df.head())
    logger.debug(df.tail())

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    return df