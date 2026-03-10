import os
import sys
from pathlib import Path

import pandas as pd
from PySide6.QtWidgets import QApplication

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.chart.chart_widget import ChartWidget


class DummyController:
    pass


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_chart_widget_supports_fractal_and_zigzag_indicators():
    _app()
    widget = ChartWidget("BTC/USDT", "1h", DummyController())

    fractal_key = widget.add_indicator("Fractal", 5)
    zigzag_key = widget.add_indicator("ZigZag", 5)

    assert fractal_key == "FRACTAL_5"
    assert zigzag_key == "ZIGZAG_5"

    df = pd.DataFrame(
        {
            "timestamp": [1700000000 + i * 60 for i in range(9)],
            "open": [1.0, 1.6, 2.8, 2.0, 1.1, 1.9, 3.2, 2.1, 1.2],
            "high": [1.3, 2.2, 5.0, 2.4, 1.4, 2.5, 6.0, 2.6, 1.5],
            "low": [0.8, 1.1, 2.0, 1.4, 0.4, 1.2, 2.1, 1.3, 0.7],
            "close": [1.1, 1.9, 3.1, 1.7, 0.9, 2.2, 3.8, 1.8, 1.0],
            "volume": [10, 12, 18, 11, 14, 15, 19, 13, 9],
        }
    )

    widget.update_candles(df)

    fractal_highs, fractal_lows = widget.indicator_items[fractal_key]
    zigzag_curve = widget.indicator_items[zigzag_key][0]

    assert len(fractal_highs.points()) >= 2
    assert len(fractal_lows.points()) >= 1

    x_data, y_data = zigzag_curve.getData()
    assert x_data is not None
    assert y_data is not None
    assert len(x_data) >= 3
    assert len(y_data) >= 3


def test_chart_widget_accepts_utc_timestamp_series():
    _app()
    widget = ChartWidget("EUR/USD", "4h", DummyController())

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-10T00:00:00+00:00",
                    "2026-03-10T04:00:00+00:00",
                    "2026-03-10T08:00:00+00:00",
                ],
                utc=True,
            ),
            "open": [1.10, 1.11, 1.12],
            "high": [1.12, 1.13, 1.14],
            "low": [1.09, 1.10, 1.11],
            "close": [1.11, 1.12, 1.13],
            "volume": [1000, 1100, 900],
        }
    )

    widget.update_candles(df)

    assert widget._last_x is not None
    assert len(widget._last_x) == 3
    assert widget._last_x[1] > widget._last_x[0]
