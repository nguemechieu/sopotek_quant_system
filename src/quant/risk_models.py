import math

import pandas as pd


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def to_series(values):
    if isinstance(values, pd.Series):
        return values.dropna().astype(float)
    return pd.Series(list(values or []), dtype="float64").dropna()


def close_returns(frame):
    if frame is None or getattr(frame, "empty", True):
        return pd.Series(dtype="float64")
    if "close" not in frame.columns:
        return pd.Series(dtype="float64")
    closes = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if len(closes) < 3:
        return pd.Series(dtype="float64")
    return closes.pct_change().dropna()


def historical_var(returns, confidence=0.95):
    series = to_series(returns)
    if series.empty:
        return 0.0
    alpha = max(0.01, min(0.99, 1.0 - float(confidence)))
    quantile = float(series.quantile(alpha))
    return max(0.0, -quantile)


def historical_cvar(returns, confidence=0.95):
    series = to_series(returns)
    if series.empty:
        return 0.0
    alpha = max(0.01, min(0.99, 1.0 - float(confidence)))
    cutoff = float(series.quantile(alpha))
    tail = series[series <= cutoff]
    if tail.empty:
        return historical_var(series, confidence=confidence)
    return max(0.0, -float(tail.mean()))


def annualized_volatility(returns, periods_per_year=252):
    series = to_series(returns)
    if len(series) < 2:
        return 0.0
    return max(0.0, float(series.std(ddof=0)) * math.sqrt(max(1, int(periods_per_year))))


def correlation(left_returns, right_returns):
    left = to_series(left_returns)
    right = to_series(right_returns)
    if left.empty or right.empty:
        return 0.0
    aligned = pd.concat([left.reset_index(drop=True), right.reset_index(drop=True)], axis=1).dropna()
    if len(aligned) < 5:
        return 0.0
    corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
    if pd.isna(corr):
        return 0.0
    return float(corr)


def kelly_fraction(returns, side="buy", cap=0.25):
    series = to_series(returns)
    if len(series) < 5:
        return 0.0

    signed = series if str(side).lower() == "buy" else -series
    edge = float(signed.mean())
    variance = float(signed.var(ddof=0))
    if variance <= 0:
        return 0.0

    raw = edge / variance
    if raw <= 0:
        return 0.0
    return max(0.0, min(float(cap), float(raw)))
