import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.chart.chart_items import CandlestickItem


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_candlestick_item_supports_chart_widget_api():
    _app()
    item = CandlestickItem(body_width=10.0, up_color="#00ff00", down_color="#ff0000")

    item.set_body_width(12.0)
    item.set_colors("#26a69a", "#ef5350")
    item.setData(
        [
            [1.0, 10.0, 12.0, 9.0, 13.0],
            [2.0, 12.0, 11.0, 10.0, 14.0],
            [3.0, 11.5, 11.5, 11.0, 12.0],
        ]
    )

    rect = item.boundingRect()

    assert rect.width() > 0
    assert rect.height() > 0
    assert rect.left() <= -10.0
    assert rect.right() >= 15.0
    assert rect.top() <= 9.0
    assert rect.bottom() >= 14.0
