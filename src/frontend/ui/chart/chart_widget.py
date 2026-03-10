import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore
from PySide6.QtWidgets import QFrame, QLabel, QHBoxLayout, QSplitter, QVBoxLayout, QWidget
from pyqtgraph import DateAxisItem, InfiniteLine, PlotWidget, ScatterPlotItem, SignalProxy, TextItem, mkPen

from frontend.ui.chart.chart_items import CandlestickItem
from frontend.ui.chart.indicator_utils import (
    accumulation_distribution,
    accelerator,
    adx,
    alligator,
    atr,
    awesome,
    bears_power,
    bollinger,
    bulls_power,
    cci,
    demarker,
    ema,
    envelopes,
    force_index,
    gator,
    ichimoku,
    lwma,
    macd,
    market_facilitation_index,
    momentum,
    money_flow_index,
    obv,
    parabolic_sar,
    rsi,
    rvi,
    sma,
    smma,
    standard_deviation,
    stochastic,
    true_range,
    williams_r,
)


class ChartWidget(QWidget):
    sigMouseMoved = QtCore.Signal(object)

    def __init__(self, symbol: str, timeframe: str, controller, candle_up_color: str = "#26a69a", candle_down_color: str = "#ef5350"):
        super().__init__()
        self.controller = controller
        self.symbol = symbol
        self.timeframe = timeframe
        self.candle_up_color = candle_up_color
        self.candle_down_color = candle_down_color
        self._last_candles = None
        self.show_bid_ask_lines = True
        self._last_bid = None
        self._last_ask = None

        self.indicators = []
        self.indicator_items = {}
        self.indicator_panes = {}
        self.heatmap_buffer = []
        self.max_heatmap_rows = 220
        self.max_heatmap_levels = 120
        self._last_heatmap_price_range = None
        self._last_df = None
        self._last_x = None
        self._last_candle_stats = None
        self._watermark_initialized = False
        self._auto_fit_pending = True
        self._last_view_context = None
        self.default_visible_bars = 120
        self.chart_background = "#0a1020"
        self.panel_background = "#0c1730"
        self.grid_color = (112, 138, 184, 42)
        self.axis_color = "#8fa7c6"
        self.muted_text = "#7f95b5"
        self._last_price_change = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(8)

        self.info_bar = QFrame()
        self.info_bar.setStyleSheet(
            """
            QFrame {
                background-color: #0c1730;
                border: 1px solid #173055;
                border-radius: 12px;
            }
            """
        )
        info_layout = QHBoxLayout(self.info_bar)
        info_layout.setContentsMargins(14, 10, 14, 10)
        info_layout.setSpacing(12)

        self.market_stats_label = QLabel()
        self.market_stats_label.setStyleSheet("color: #32d296; font-weight: 700; font-size: 12px;")
        info_layout.addWidget(self.market_stats_label, 1)

        self.market_meta_label = QLabel()
        self.market_meta_label.setStyleSheet("color: #7f95b5; font-size: 12px;")
        info_layout.addWidget(self.market_meta_label, 2)

        self.ohlcv_label = QLabel()
        self.ohlcv_label.setStyleSheet("color: #d8e6ff; font-weight: 700; font-size: 12px;")
        self.ohlcv_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        info_layout.addWidget(self.ohlcv_label, 3)

        layout.addWidget(self.info_bar)

        self.splitter = QSplitter(QtCore.Qt.Orientation.Vertical)
        self.splitter.setChildrenCollapsible(False)
        self.splitter.setHandleWidth(10)
        self.splitter.setStyleSheet(
            """
            QSplitter::handle {
                background-color: #132033;
                border-top: 1px solid #24354f;
                border-bottom: 1px solid #24354f;
            }
            QSplitter::handle:hover {
                background-color: #1c3150;
            }
            """
        )
        layout.addWidget(self.splitter)

        date_axis_top = DateAxisItem(orientation="bottom")
        self.price_plot = PlotWidget(axisItems={"bottom": date_axis_top})
        self.price_plot.setLabel("right", "Price")
        self.price_plot.hideAxis("left")
        self.price_plot.showAxis("right")
        self.price_plot.hideAxis("bottom")
        self.price_plot.setMinimumHeight(360)
        self.splitter.addWidget(self.price_plot)

        self.candle_item = CandlestickItem(
            body_width=60.0,
            up_color=self.candle_up_color,
            down_color=self.candle_down_color,
        )
        self.price_plot.addItem(self.candle_item)

        self.ema_curve = self.price_plot.plot(pen=mkPen("#42a5f5", width=1.8))
        self.ema_curve.setVisible(False)

        self.signal_markers = ScatterPlotItem()
        self.trade_scatter = ScatterPlotItem()
        self.price_plot.addItem(self.signal_markers)
        self.price_plot.addItem(self.trade_scatter)

        date_axis_mid = DateAxisItem(orientation="bottom")
        self.volume_plot = PlotWidget(axisItems={"bottom": date_axis_mid})
        self.volume_plot.setXLink(self.price_plot)
        self.volume_plot.setLabel("left", "Volume")
        self.volume_plot.hideAxis("right")
        self.volume_plot.hideAxis("bottom")
        self.volume_plot.setMinimumHeight(120)
        self.splitter.addWidget(self.volume_plot)

        self.volume_bars = pg.BarGraphItem(x=[], height=[], width=60.0, brush="#5c6bc0")
        self.volume_plot.addItem(self.volume_bars)

        date_axis_bottom = DateAxisItem(orientation="bottom")
        self.heatmap_plot = PlotWidget(axisItems={"bottom": date_axis_bottom})
        self.heatmap_plot.setXLink(self.price_plot)
        self.heatmap_plot.setLabel("left", "Orderbook")
        self.heatmap_plot.setLabel("bottom", "Gregorian Time")
        self.heatmap_plot.setMinimumHeight(120)
        self.splitter.addWidget(self.heatmap_plot)

        self.heatmap_image = pg.ImageItem()
        colormap = pg.colormap.get("inferno")
        self.heatmap_image.setLookupTable(colormap.getLookupTable())
        self.heatmap_plot.addItem(self.heatmap_image)

        self._style_plot(self.price_plot, right_label="Price", show_bottom=False)
        self._style_plot(self.volume_plot, left_label="Volume", show_bottom=False)
        self._style_plot(self.heatmap_plot, left_label="Orderbook", bottom_label="Time", show_bottom=True)

        self.v_line = InfiniteLine(angle=90, movable=False, pen=mkPen((142, 164, 196, 90), width=1, style=QtCore.Qt.PenStyle.DashLine))
        self.h_line = InfiniteLine(angle=0, movable=False, pen=mkPen((142, 164, 196, 90), width=1, style=QtCore.Qt.PenStyle.DashLine))
        self.price_plot.addItem(self.v_line, ignoreBounds=True)
        self.price_plot.addItem(self.h_line, ignoreBounds=True)

        # Live price lines
        self.bid_line = InfiniteLine(
            angle=0,
            movable=False,
            pen=mkPen("#26a69a", width=1, style=QtCore.Qt.PenStyle.DashLine),
            label="Bid {value:.6f}",
            labelOpts={"position": 0.98, "color": "#26a69a", "fill": (11, 18, 32, 160)},
        )
        self.ask_line = InfiniteLine(
            angle=0,
            movable=False,
            pen=mkPen("#ef5350", width=1, style=QtCore.Qt.PenStyle.DashLine),
            label="Ask {value:.6f}",
            labelOpts={"position": 0.98, "color": "#ef5350", "fill": (11, 18, 32, 160)},
        )
        self.last_line = InfiniteLine(
            angle=0,
            movable=False,
            pen=mkPen("#32d296", width=1.15),
            label="{value:.6f}",
            labelOpts={"position": 0.98, "color": "#ffffff", "fill": (50, 210, 150, 205)},
        )

        for line in (self.bid_line, self.ask_line, self.last_line):
            line.setVisible(False)
            self.price_plot.addItem(line, ignoreBounds=True)

        self.text_item = TextItem(
            html="",
            anchor=(0.0, 1.0),
            border=mkPen((30, 56, 96, 220)),
            fill=pg.mkBrush(8, 17, 34, 225),
        )
        self.price_plot.addItem(self.text_item)

        self.watermark_item = TextItem(
            html="",
            anchor=(0.5, 0.5),
            border=None,
            fill=None,
        )
        self.watermark_item.setZValue(-10)
        self.price_plot.addItem(self.watermark_item)
        self.price_plot.getPlotItem().vb.sigRangeChanged.connect(self._update_watermark_position)

        self.proxy = SignalProxy(self.price_plot.scene().sigMouseMoved, rateLimit=60, slot=self._mouse_moved)

        self.splitter.setStretchFactor(0, 8)
        self.splitter.setStretchFactor(1, 2)
        self.splitter.setStretchFactor(2, 2)
        self.splitter.setSizes([720, 170, 170])

        self._update_chart_header()
        self._update_watermark_html()

    def _style_plot(self, plot, left_label=None, right_label=None, bottom_label=None, show_bottom=False):
        plot.setBackground(self.chart_background)
        plot.showGrid(x=True, y=True, alpha=0.18)
        plot.setMenuEnabled(False)
        plot.hideButtons()

        item = plot.getPlotItem()
        item.layout.setContentsMargins(6, 6, 10, 6)

        if left_label:
            plot.setLabel("left", left_label)
        if right_label:
            plot.setLabel("right", right_label)
        if bottom_label:
            plot.setLabel("bottom", bottom_label)

        axis_names = ("left", "right", "bottom", "top")
        for axis_name in axis_names:
            axis = item.getAxis(axis_name)
            axis.setTextPen(pg.mkColor(self.axis_color))
            axis.setPen(pg.mkPen(self.axis_color, width=1))
            axis.setStyle(tickLength=-6, autoExpandTextSpace=False)

        plot.showAxis("bottom") if show_bottom else plot.hideAxis("bottom")
        if right_label:
            plot.showAxis("right")

        item.vb.setBackgroundColor(pg.mkColor(self.chart_background))

    def _create_indicator_pane(self, key: str, label: str):
        existing = self.indicator_panes.get(key)
        if existing is not None:
            return existing

        axis = DateAxisItem(orientation="bottom")
        pane = PlotWidget(axisItems={"bottom": axis})
        pane.setXLink(self.price_plot)
        pane.hideAxis("right")
        pane.setMinimumHeight(120)
        self._style_plot(pane, left_label=label, show_bottom=False)
        self.splitter.insertWidget(max(self.splitter.count() - 1, 1), pane)
        self.indicator_panes[key] = pane

        current_sizes = self.splitter.sizes()
        if len(current_sizes) >= self.splitter.count():
            current_sizes.insert(max(len(current_sizes) - 1, 1), 130)
            self.splitter.setSizes(current_sizes[: self.splitter.count()])
        return pane

    def _create_curve(self, plot, color: str, width: float = 1.4, style=None):
        pen = mkPen(color, width=width)
        if style is not None:
            pen.setStyle(style)
        return plot.plot(pen=pen)

    def _create_histogram(self, plot, brush="#5c6bc0"):
        item = pg.BarGraphItem(x=[], height=[], width=1.0, y0=0, brush=brush)
        plot.addItem(item)
        return item

    def _set_histogram_data(self, item, x, values, width, brushes=None):
        if brushes is None:
            item.setOpts(x=x, height=values, width=width, y0=0)
        else:
            item.setOpts(x=x, height=values, width=width, y0=0, brushes=brushes)

    def _add_reference_line(self, plot, y_value: float, color: str = "#5d6d8a"):
        line = InfiniteLine(
            angle=0,
            movable=False,
            pen=mkPen(color, width=1, style=QtCore.Qt.PenStyle.DashLine),
        )
        line.setPos(y_value)
        plot.addItem(line, ignoreBounds=True)
        return line

    def _sync_view_context(self):
        context = (self.symbol, self.timeframe)
        if context != self._last_view_context:
            self._last_view_context = context
            self._auto_fit_pending = True
            self.heatmap_buffer.clear()
            self._last_heatmap_price_range = None
            self.heatmap_image.clear()

    def _should_fit_chart_view(self, x):
        if self._auto_fit_pending:
            return True

        if x is None or len(x) == 0:
            return False

        try:
            x_range, _y_range = self.price_plot.viewRange()
        except Exception:
            return True

        if len(x_range) < 2 or not np.isfinite(x_range[0]) or not np.isfinite(x_range[1]):
            return True

        min_x = float(x[0])
        max_x = float(x[-1])
        visible_span = float(x_range[1]) - float(x_range[0])
        full_span = max(max_x - min_x, 1e-9)

        if visible_span <= 0:
            return True

        if float(x_range[1]) < min_x or float(x_range[0]) > max_x:
            return True

        # If the viewport is effectively the entire history, fit to a more useful recent window.
        if visible_span >= full_span * 0.98:
            return True

        return False

    def _visible_slice_start(self, x):
        if x is None or len(x) == 0:
            return 0
        visible_bars = min(len(x), self.default_visible_bars)
        return max(0, len(x) - visible_bars)

    def _build_candle_stats(self, df, x):
        if df is None or len(df) == 0 or x is None or len(x) == 0:
            return None

        start_index = self._visible_slice_start(x)
        visible = df.iloc[start_index:].copy()
        if visible.empty:
            return None

        open_values = visible["open"].astype(float).to_numpy()
        high_values = visible["high"].astype(float).to_numpy()
        low_values = visible["low"].astype(float).to_numpy()
        close_values = visible["close"].astype(float).to_numpy()
        volume_values = visible["volume"].astype(float).to_numpy()
        visible_x = np.asarray(x[start_index:], dtype=float)

        finite_high = high_values[np.isfinite(high_values)]
        finite_low = low_values[np.isfinite(low_values)]
        finite_close = close_values[np.isfinite(close_values)]
        finite_volume = volume_values[np.isfinite(volume_values)]

        if len(finite_high) == 0 or len(finite_low) == 0 or len(finite_close) == 0:
            return None

        first_open = float(open_values[0])
        last_close = float(close_values[-1])
        variation = ((last_close - first_open) / first_open * 100.0) if abs(first_open) > 1e-12 else 0.0

        return {
            "start_index": start_index,
            "x": visible_x,
            "min_price": float(np.min(finite_low)),
            "max_price": float(np.max(finite_high)),
            "max_volume": float(np.max(finite_volume)) if len(finite_volume) else 0.0,
            "average_close": float(np.mean(finite_close)),
            "cumulative_volume": float(np.sum(finite_volume)) if len(finite_volume) else 0.0,
            "last_price": last_close,
            "variation_pct": variation,
        }

    def _fit_chart_view(self, stats, width):
        if not stats:
            return

        visible_x = np.asarray(stats["x"], dtype=float)
        if len(visible_x) == 0:
            return

        min_x = float(visible_x[0] - (width * 2.0))
        max_x = float(visible_x[-1] + (width * 2.0))
        min_y = float(stats["min_price"])
        max_y = float(stats["max_price"])
        y_span = max(max_y - min_y, max(abs(max_y) * 0.02, 1e-9))
        y_pad = y_span * 0.10

        price_vb = self.price_plot.getPlotItem().vb
        price_vb.enableAutoRange(x=False, y=False)
        price_vb.setXRange(min_x, max_x, padding=0.0)
        price_vb.setYRange(min_y - y_pad, max_y + y_pad, padding=0.0)

        volume_vb = self.volume_plot.getPlotItem().vb
        volume_vb.enableAutoRange(x=False, y=False)
        volume_vb.setYRange(0.0, max(float(stats["max_volume"]) * 1.15, 1.0), padding=0.0)

        self._auto_fit_pending = False

    def _mouse_moved(self, evt):
        pos = evt[0]
        if not self.price_plot.sceneBoundingRect().contains(pos):
            return

        mouse_point = self.price_plot.getPlotItem().vb.mapSceneToView(pos)
        x = mouse_point.x()
        y = mouse_point.y()

        self.v_line.setPos(x)
        self.h_line.setPos(y)
        self.text_item.setHtml(f"<span style='color:#e3f2fd'>Price: {y:.6f}</span>")
        self.text_item.setPos(x, y)
        self._update_ohlcv_for_x(x)

    def _active_broker_name(self):
        broker = getattr(self.controller, "broker", None)
        if broker is not None:
            name = getattr(broker, "exchange_name", None)
            if name:
                return str(name)

        config = getattr(self.controller, "config", None)
        broker_config = getattr(config, "broker", None)
        if broker_config is not None:
            exchange = getattr(broker_config, "exchange", None)
            if exchange:
                return str(exchange)

        return "Broker"

    def _symbol_parts(self):
        if "/" not in str(self.symbol):
            return str(self.symbol).upper(), ""
        base, quote = str(self.symbol).upper().split("/", 1)
        return base, quote

    def _timeframe_description(self):
        mapping = {
            "1m": "1 minute chart",
            "5m": "5 minute chart",
            "15m": "15 minute chart",
            "30m": "30 minute chart",
            "1h": "1 hour chart",
            "4h": "4 hour chart",
            "1d": "1 day chart",
            "1w": "1 week chart",
            "1mn": "1 month chart",
        }
        return mapping.get(str(self.timeframe).lower(), f"{self.timeframe} chart")

    def _update_chart_header(self):
        base, quote = self._symbol_parts()
        broker_name = self._active_broker_name().upper()

        stats = self._last_candle_stats or {}
        if quote:
            description = f"{base} priced in {quote}"
        else:
            description = self._timeframe_description()
        meta_prefix = f"{self.timeframe.upper()}  |  {broker_name}"

        if stats:
            last_price = self._format_metric(stats.get("last_price", 0.0))
            variation = float(stats.get("variation_pct", 0.0))
            cumulative_volume = self._format_volume(stats.get("cumulative_volume", 0.0))
            positive = variation >= 0
            change_color = "#32d296" if positive else "#ff5b7f"
            prefix = "+" if positive else ""
            self.market_stats_label.setText(f"{last_price}  {prefix}{variation:.2f}%  Vol {cumulative_volume}")
            self.market_stats_label.setStyleSheet(
                f"color: {change_color}; font-weight: 800; font-size: 12px;"
            )
            self.market_meta_label.setText(
                f"{meta_prefix}  |  {description}  |  Avg {self._format_metric(stats.get('average_close', 0.0))}  |  "
                f"Range {self._format_metric(stats.get('min_price', 0.0), 4)} - {self._format_metric(stats.get('max_price', 0.0), 4)}"
            )
        else:
            self.market_stats_label.setText(self._timeframe_description())
            self.market_stats_label.setStyleSheet("color: #8fa7c6; font-weight: 700; font-size: 12px;")
            if quote:
                self.market_meta_label.setText(
                    f"{meta_prefix}  |  {description}  |  Quote asset {quote} against base asset {base}"
                )
            else:
                self.market_meta_label.setText(f"{meta_prefix}  |  {self._timeframe_description()}")

    def _update_watermark_html(self):
        base, quote = self._symbol_parts()
        description = f"{base} / {quote}" if quote else base
        self.watermark_item.setHtml(
            (
                "<div style='text-align:center;'>"
                f"<div style='color: rgba(200,216,255,0.10); font-size: 42px; font-weight: 800; letter-spacing: 1px;'>{self.symbol.upper()}</div>"
                f"<div style='color: rgba(148,171,214,0.10); font-size: 24px; font-weight: 700;'>{self.timeframe.upper()}</div>"
                f"<div style='color: rgba(148,171,214,0.08); font-size: 12px; text-transform: uppercase;'>{description}</div>"
                "</div>"
            )
        )

    def refresh_context_display(self):
        self._update_chart_header()
        self._update_watermark_html()
        self._update_watermark_position()

    def _update_watermark_position(self, *_args):
        try:
            x_range, y_range = self.price_plot.viewRange()
            center_x = (float(x_range[0]) + float(x_range[1])) / 2.0
            center_y = (float(y_range[0]) + float(y_range[1])) / 2.0
            self.watermark_item.setPos(center_x, center_y)
            self._watermark_initialized = True
        except Exception:
            return

    def _format_metric(self, value, digits=6):
        try:
            numeric = float(value)
        except Exception:
            return "-"
        if abs(numeric) >= 1000:
            return f"{numeric:,.2f}"
        if abs(numeric) >= 1:
            return f"{numeric:,.{min(digits, 4)}f}"
        return f"{numeric:,.{digits}f}"

    def _format_volume(self, value):
        try:
            numeric = float(value)
        except Exception:
            return "-"
        if numeric >= 1_000_000_000:
            return f"{numeric / 1_000_000_000:.2f}B"
        if numeric >= 1_000_000:
            return f"{numeric / 1_000_000:.2f}M"
        if numeric >= 1_000:
            return f"{numeric / 1_000:.2f}K"
        return f"{numeric:.2f}"

    def _set_ohlcv_from_row(self, row):
        if row is None:
            self.ohlcv_label.setText("O -  H -  L -  C -  V -")
            return

        self.ohlcv_label.setText(
            "  ".join(
                [
                    f"O {self._format_metric(row.get('open', 0.0))}",
                    f"H {self._format_metric(row.get('high', 0.0))}",
                    f"L {self._format_metric(row.get('low', 0.0))}",
                    f"C {self._format_metric(row.get('close', 0.0))}",
                    f"V {self._format_volume(row.get('volume', 0.0))}",
                ]
            )
        )

    def _update_ohlcv_for_x(self, x_value):
        if self._last_df is None or self._last_x is None or len(self._last_x) == 0:
            self._set_ohlcv_from_row(None)
            return

        try:
            index = int(np.nanargmin(np.abs(self._last_x - float(x_value))))
        except Exception:
            index = len(self._last_df) - 1

        if index < 0 or index >= len(self._last_df):
            return

        row = self._last_df.iloc[index]
        self._set_ohlcv_from_row(row)

    def _extract_time_axis(self, df):
        if "timestamp" not in df.columns:
            return np.arange(len(df), dtype=float)

        ts = df["timestamp"]

        try:
            import pandas as pd

            # Numeric epoch input
            if pd.api.types.is_numeric_dtype(ts):
                x = pd.to_numeric(ts, errors="coerce").to_numpy(dtype=float)
                if len(x) > 0:
                    median = np.nanmedian(np.abs(x))
                    if median > 1e11:  # likely milliseconds
                        x = x / 1000.0
                return x

            dt = pd.to_datetime(ts, errors="coerce", utc=True)
            x = (dt.astype("int64") / 1e9).to_numpy(dtype=float)
            if np.isnan(x).all():
                return np.arange(len(df), dtype=float)
            return x
        except Exception:
            return np.arange(len(df), dtype=float)

    def _infer_candle_width(self, x):
        if len(x) < 2:
            return 60.0

        diffs = np.diff(x)
        diffs = diffs[np.isfinite(diffs)]
        diffs = diffs[np.abs(diffs) > 0]
        if len(diffs) == 0:
            return 60.0

        step = float(np.median(np.abs(diffs)))
        return max(min(step * 0.64, step * 0.8), 1e-6)

    def update_orderbook_heatmap(self, bids, asks):
        if not bids and not asks:
            return

        parsed_levels = []
        for level in (bids or [])[: self.max_heatmap_levels]:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                try:
                    price = float(level[0])
                    volume = float(level[1])
                    if np.isfinite(price) and np.isfinite(volume) and volume > 0:
                        parsed_levels.append((price, volume))
                except Exception:
                    continue

        for level in (asks or [])[: self.max_heatmap_levels]:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                try:
                    price = float(level[0])
                    volume = float(level[1])
                    if np.isfinite(price) and np.isfinite(volume) and volume > 0:
                        parsed_levels.append((price, volume))
                except Exception:
                    continue

        if not parsed_levels:
            return

        prices = np.array([price for price, _volume in parsed_levels], dtype=float)
        volumes = np.array([volume for _price, volume in parsed_levels], dtype=float)

        price_min = float(np.min(prices))
        price_max = float(np.max(prices))

        last_close = None
        if self._last_df is not None and not self._last_df.empty and "close" in self._last_df.columns:
            try:
                last_close = float(self._last_df["close"].iloc[-1])
            except Exception:
                last_close = None

        raw_span = max(price_max - price_min, 1e-9)
        anchor_price = last_close if last_close is not None and np.isfinite(last_close) else float(np.mean(prices))
        padding = max(raw_span * 0.2, abs(anchor_price) * 0.0015, 1e-6)
        grid_min = min(price_min, anchor_price - padding)
        grid_max = max(price_max, anchor_price + padding)

        previous_range = self._last_heatmap_price_range
        if previous_range is not None:
            prev_min, prev_max = previous_range
            grid_min = min(grid_min, float(prev_min))
            grid_max = max(grid_max, float(prev_max))
        self._last_heatmap_price_range = (grid_min, grid_max)

        if not np.isfinite(grid_min) or not np.isfinite(grid_max) or grid_max <= grid_min:
            return

        price_axis = np.linspace(grid_min, grid_max, self.max_heatmap_levels)
        column = np.zeros(self.max_heatmap_levels, dtype=float)
        for price, volume in parsed_levels:
            index = int(np.searchsorted(price_axis, price, side="left"))
            index = max(0, min(self.max_heatmap_levels - 1, index))
            column[index] += volume

        column_max = float(np.max(column))
        if column_max > 0:
            column /= column_max

        self.heatmap_buffer.append(column)
        if len(self.heatmap_buffer) > self.max_heatmap_rows:
            self.heatmap_buffer.pop(0)

        matrix = np.array(self.heatmap_buffer, dtype=float).T
        if matrix.size == 0:
            return

        matrix_max = float(np.nanmax(matrix))
        if matrix_max > 0:
            matrix = matrix / matrix_max

        if self._last_x is not None and len(self._last_x) >= 2:
            diffs = np.diff(self._last_x)
            diffs = diffs[np.isfinite(diffs)]
            diffs = diffs[np.abs(diffs) > 0]
            step = float(np.median(np.abs(diffs))) if len(diffs) else 60.0
            x_end = float(self._last_x[-1]) + (step * 0.5)
        elif self._last_x is not None and len(self._last_x) == 1:
            step = 60.0
            x_end = float(self._last_x[-1]) + (step * 0.5)
        else:
            step = 1.0
            x_end = float(matrix.shape[1])

        x_start = x_end - (step * matrix.shape[1])
        rect = QtCore.QRectF(
            x_start,
            grid_min,
            max(step * matrix.shape[1], 1e-6),
            max(grid_max - grid_min, 1e-9),
        )

        self.heatmap_image.setImage(np.flipud(matrix), autoLevels=False, levels=(0.0, 1.0))
        self.heatmap_image.setRect(rect)
        self.heatmap_plot.setYRange(grid_min, grid_max, padding=0.02)

    def add_strategy_signal(self, index, price, signal):
        if signal == "BUY":
            self.signal_markers.addPoints(x=[index], y=[price], symbol="t1", brush="#26a69a", size=12)
        elif signal == "SELL":
            self.signal_markers.addPoints(x=[index], y=[price], symbol="t", brush="#ef5350", size=12)

    def _pivot_window(self, period: int) -> int:
        return max(2, int(period) // 2)

    def _build_fractal_points(self, high, low, x, period: int):
        window = self._pivot_window(period)
        upper_x = []
        upper_y = []
        lower_x = []
        lower_y = []

        for index in range(window, len(x) - window):
            high_slice = high.iloc[index - window: index + window + 1]
            low_slice = low.iloc[index - window: index + window + 1]

            current_high = float(high.iloc[index])
            current_low = float(low.iloc[index])

            if np.isfinite(current_high) and current_high >= float(high_slice.max()):
                upper_x.append(float(x[index]))
                upper_y.append(current_high)

            if np.isfinite(current_low) and current_low <= float(low_slice.min()):
                lower_x.append(float(x[index]))
                lower_y.append(current_low)

        return (np.array(upper_x, dtype=float), np.array(upper_y, dtype=float)), (
            np.array(lower_x, dtype=float),
            np.array(lower_y, dtype=float),
        )

    def _build_zigzag_points(self, high, low, x, period: int):
        window = self._pivot_window(period)
        candidates = []

        for index in range(window, len(x) - window):
            high_slice = high.iloc[index - window: index + window + 1]
            low_slice = low.iloc[index - window: index + window + 1]
            current_high = float(high.iloc[index])
            current_low = float(low.iloc[index])

            if np.isfinite(current_high) and current_high >= float(high_slice.max()):
                candidates.append((index, "H", current_high))

            if np.isfinite(current_low) and current_low <= float(low_slice.min()):
                candidates.append((index, "L", current_low))

        if not candidates:
            return np.array([], dtype=float), np.array([], dtype=float)

        candidates.sort(key=lambda item: item[0])
        pivots = []

        for candidate in candidates:
            if not pivots:
                pivots.append(candidate)
                continue

            last_index, last_kind, last_price = pivots[-1]
            current_index, current_kind, current_price = candidate

            if current_kind == last_kind:
                if current_kind == "H" and current_price >= last_price:
                    pivots[-1] = candidate
                elif current_kind == "L" and current_price <= last_price:
                    pivots[-1] = candidate
                continue

            if current_index == last_index:
                more_extreme = (
                    current_kind == "H" and current_price >= last_price
                ) or (
                    current_kind == "L" and current_price <= last_price
                )
                if more_extreme:
                    pivots[-1] = candidate
                continue

            pivots.append(candidate)

        zz_x = np.array([float(x[index]) for index, _kind, _price in pivots], dtype=float)
        zz_y = np.array([float(price) for _index, _kind, price in pivots], dtype=float)
        return zz_x, zz_y

    def _build_fibonacci_overlay(self):
        levels = [
            (0.0, "#90caf9"),
            (0.236, "#4fc3f7"),
            (0.382, "#26a69a"),
            (0.5, "#ffd54f"),
            (0.618, "#ffb74d"),
            (0.786, "#ef5350"),
            (1.0, "#ce93d8"),
        ]
        curves = []
        labels = []
        for ratio, color in levels:
            curve = self._create_curve(
                self.price_plot,
                color,
                1.0 if ratio not in {0.0, 1.0} else 1.2,
                QtCore.Qt.PenStyle.DashLine,
            )
            label = TextItem(
                html="",
                anchor=(1.0, 0.5),
                border=None,
                fill=pg.mkBrush(11, 18, 32, 160),
            )
            self.price_plot.addItem(label)
            curves.append(curve)
            labels.append(label)
        return {"curves": curves, "labels": labels, "levels": levels}

    def add_indicator(self, name: str, period: int = 20):
        indicator = (name or "").strip().upper()
        period = max(2, int(period))
        aliases = {
            "MOVING AVERAGE": "SMA",
            "MA": "SMA",
            "EXPONENTIAL MOVING AVERAGE": "EMA",
            "WEIGHTED MOVING AVERAGE": "LWMA",
            "LINEAR WEIGHTED MOVING AVERAGE": "LWMA",
            "WMA": "LWMA",
            "SMOOTHED MOVING AVERAGE": "SMMA",
            "BOLLINGER": "BB",
            "BOLLINGER BANDS": "BB",
            "AVERAGE DIRECTIONAL MOVEMENT INDEX": "ADX",
            "AVERAGE TRUE RANGE": "ATR",
            "PARABOLIC SAR": "SAR",
            "STANDARD DEVIATION": "STDDEV",
            "ACCELERATOR OSCILLATOR": "AC",
            "AWESOME OSCILLATOR": "AO",
            "STOCHASTIC OSCILLATOR": "STOCHASTIC",
            "WILLIAMS' PERCENT RANGE": "WPR",
            "WILLIAMS PERCENT RANGE": "WPR",
            "ACCUMULATION/DISTRIBUTION": "AD",
            "ACCUMULATION DISTRIBUTION": "AD",
            "MONEY FLOW INDEX": "MFI",
            "ON BALANCE VOLUME": "OBV",
            "MARKET FACILITATION INDEX": "BW_MFI",
            "GATOR OSCILLATOR": "GATOR",
            "DONCHIAN CHANNEL": "DONCHIAN",
            "DONCHIAN CHANNELS": "DONCHIAN",
            "KELTNER CHANNEL": "KELTNER",
            "KELTNER CHANNELS": "KELTNER",
            "FIBONACCI": "FIBO",
            "FIBONACCI RETRACEMENT": "FIBO",
            "FIBO": "FIBO",
            "FRACTALS": "FRACTAL",
            "ZIG ZAG": "ZIGZAG",
        }
        indicator = aliases.get(indicator, indicator)

        if indicator in {"SMA", "EMA", "SMMA", "LWMA", "VWAP"}:
            key = f"{indicator}_{period}"
            if key in self.indicator_items:
                return key
            color_map = {
                "SMA": "#ffd54f",
                "EMA": "#80deea",
                "SMMA": "#b39ddb",
                "LWMA": "#ff8a65",
                "VWAP": "#81c784",
            }
            self.indicator_items[key] = [self._create_curve(self.price_plot, color_map.get(indicator, "#ffd54f"), 1.6)]
            self.indicators.append({"type": indicator, "period": period, "key": key})
            return key

        if indicator in {"BB", "ENVELOPES", "DONCHIAN", "KELTNER"}:
            key = f"{indicator}_{period}"
            if key in self.indicator_items:
                return key
            if indicator == "BB":
                items = [
                    self._create_curve(self.price_plot, "#ffb74d", 1.4),
                    self._create_curve(self.price_plot, "#ab47bc", 1.1),
                    self._create_curve(self.price_plot, "#ab47bc", 1.1),
                ]
            elif indicator == "ENVELOPES":
                items = [
                    self._create_curve(self.price_plot, "#90caf9", 1.3),
                    self._create_curve(self.price_plot, "#4fc3f7", 1.0),
                    self._create_curve(self.price_plot, "#4fc3f7", 1.0),
                ]
            elif indicator == "DONCHIAN":
                items = [
                    self._create_curve(self.price_plot, "#64b5f6", 1.1),
                    self._create_curve(self.price_plot, "#90caf9", 1.0, QtCore.Qt.PenStyle.DashLine),
                    self._create_curve(self.price_plot, "#64b5f6", 1.1),
                ]
            else:
                items = [
                    self._create_curve(self.price_plot, "#ffcc80", 1.2),
                    self._create_curve(self.price_plot, "#ce93d8", 1.0),
                    self._create_curve(self.price_plot, "#ce93d8", 1.0),
                ]
            self.indicator_items[key] = items
            self.indicators.append({"type": indicator, "period": period, "key": key})
            return key

        if indicator in {"ICHIMOKU", "ALLIGATOR"}:
            key = indicator
            if key in self.indicator_items:
                return key
            if indicator == "ICHIMOKU":
                items = [
                    self._create_curve(self.price_plot, "#ffca28", 1.2),
                    self._create_curve(self.price_plot, "#42a5f5", 1.2),
                    self._create_curve(self.price_plot, "#66bb6a", 1.0),
                    self._create_curve(self.price_plot, "#ef5350", 1.0),
                    self._create_curve(self.price_plot, "#b39ddb", 1.0),
                ]
            else:
                items = [
                    self._create_curve(self.price_plot, "#42a5f5", 1.3),
                    self._create_curve(self.price_plot, "#ef5350", 1.3),
                    self._create_curve(self.price_plot, "#66bb6a", 1.3),
                ]
            self.indicator_items[key] = items
            self.indicators.append({"type": indicator, "period": period, "key": key})
            return key

        if indicator == "SAR":
            key = "SAR"
            if key in self.indicator_items:
                return key
            scatter = ScatterPlotItem()
            self.price_plot.addItem(scatter)
            self.indicator_items[key] = [scatter]
            self.indicators.append({"type": "SAR", "period": period, "key": key})
            return key

        if indicator == "FRACTAL":
            key = f"FRACTAL_{period}"
            if key in self.indicator_items:
                return key
            upper = ScatterPlotItem()
            lower = ScatterPlotItem()
            self.price_plot.addItem(upper)
            self.price_plot.addItem(lower)
            self.indicator_items[key] = [upper, lower]
            self.indicators.append({"type": "FRACTAL", "period": period, "key": key})
            return key

        if indicator == "ZIGZAG":
            key = f"ZIGZAG_{period}"
            if key in self.indicator_items:
                return key
            curve = self._create_curve(self.price_plot, "#f06292", 1.8)
            self.indicator_items[key] = [curve]
            self.indicators.append({"type": "ZIGZAG", "period": period, "key": key})
            return key

        if indicator == "FIBO":
            key = f"FIBO_{period}"
            if key in self.indicator_items:
                return key
            self.indicator_items[key] = self._build_fibonacci_overlay()
            self.indicators.append({"type": "FIBO", "period": period, "key": key})
            return key

        if indicator == "VOLUMES":
            key = "VOLUMES"
            if key not in self.indicator_items:
                self.indicator_items[key] = []
                self.indicators.append({"type": "VOLUMES", "period": period, "key": key})
            return key

        pane_label_map = {
            "ADX": "ADX",
            "ATR": "ATR",
            "STDDEV": "StdDev",
            "AC": "Accelerator",
            "AO": "Awesome",
            "CCI": "CCI",
            "DEMARKER": "DeMarker",
            "MACD": "MACD",
            "MOMENTUM": "Momentum",
            "OSMA": "OsMA",
            "RSI": "RSI",
            "RVI": "RVI",
            "STOCHASTIC": "Stochastic",
            "WPR": "Williams %R",
            "AD": "A/D",
            "MFI": "Money Flow",
            "OBV": "OBV",
            "BULLS POWER": "Bulls Power",
            "BEARS POWER": "Bears Power",
            "FORCE INDEX": "Force Index",
            "GATOR": "Gator",
            "BW_MFI": "Market Facilitation",
        }
        lower_indicator = indicator
        if lower_indicator in pane_label_map:
            key = f"{lower_indicator}_{period}" if lower_indicator in {
                "ADX",
                "ATR",
                "STDDEV",
                "CCI",
                "DEMARKER",
                "MOMENTUM",
                "RSI",
                "STOCHASTIC",
                "WPR",
                "MFI",
                "FORCE INDEX",
            } else lower_indicator.replace(" ", "_")
            if key in self.indicator_items:
                return key

            pane = self._create_indicator_pane(key, pane_label_map[lower_indicator])
            items = []

            if lower_indicator == "ADX":
                items = [
                    self._create_curve(pane, "#ffd54f", 1.4),
                    self._create_curve(pane, "#26a69a", 1.2),
                    self._create_curve(pane, "#ef5350", 1.2),
                ]
                self._add_reference_line(pane, 20.0)
            elif lower_indicator in {"ATR", "STDDEV", "AD", "MFI", "OBV", "MOMENTUM", "BULLS POWER", "BEARS POWER", "FORCE INDEX"}:
                items = [self._create_curve(pane, "#80deea", 1.5)]
                if lower_indicator in {"BULLS POWER", "BEARS POWER", "FORCE INDEX"}:
                    self._add_reference_line(pane, 0.0)
            elif lower_indicator in {"AC", "AO", "OSMA", "GATOR", "BW_MFI"}:
                items = [self._create_histogram(pane)]
                if lower_indicator == "GATOR":
                    items.append(self._create_histogram(pane))
                self._add_reference_line(pane, 0.0)
            elif lower_indicator == "CCI":
                items = [self._create_curve(pane, "#ffb74d", 1.5)]
                self._add_reference_line(pane, 100.0)
                self._add_reference_line(pane, -100.0)
            elif lower_indicator == "DEMARKER":
                items = [self._create_curve(pane, "#64b5f6", 1.5)]
                self._add_reference_line(pane, 0.3)
                self._add_reference_line(pane, 0.7)
            elif lower_indicator == "MACD":
                items = [
                    self._create_histogram(pane),
                    self._create_curve(pane, "#42a5f5", 1.3),
                    self._create_curve(pane, "#ffca28", 1.1),
                ]
                self._add_reference_line(pane, 0.0)
            elif lower_indicator == "RSI":
                items = [self._create_curve(pane, "#ab47bc", 1.5)]
                self._add_reference_line(pane, 30.0)
                self._add_reference_line(pane, 70.0)
            elif lower_indicator == "RVI":
                items = [
                    self._create_curve(pane, "#4fc3f7", 1.4),
                    self._create_curve(pane, "#ffb74d", 1.1),
                ]
                self._add_reference_line(pane, 0.0)
            elif lower_indicator == "STOCHASTIC":
                items = [
                    self._create_curve(pane, "#66bb6a", 1.4),
                    self._create_curve(pane, "#ef5350", 1.1),
                ]
                self._add_reference_line(pane, 20.0)
                self._add_reference_line(pane, 80.0)
            elif lower_indicator == "WPR":
                items = [self._create_curve(pane, "#90caf9", 1.4)]
                self._add_reference_line(pane, -20.0)
                self._add_reference_line(pane, -80.0)

            self.indicator_items[key] = items
            self.indicators.append({"type": lower_indicator, "period": period, "key": key})
            return key

        return None

    def _update_indicators(self, df, x, width):
        if not self.indicators:
            return

        open_ = df["open"].astype(float)
        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        volume = df["volume"].astype(float)

        for spec in self.indicators:
            ind_type = spec["type"]
            period = spec["period"]
            key = spec["key"]
            items = self.indicator_items.get(key, [])

            if ind_type == "SMA" and items:
                series = sma(close, period).to_numpy()
                items[0].setData(x, series)

            elif ind_type == "EMA" and items:
                series = ema(close, period).to_numpy()
                items[0].setData(x, series)

            elif ind_type == "SMMA" and items:
                series = smma(close, period).to_numpy()
                items[0].setData(x, series)

            elif ind_type == "LWMA" and items:
                series = lwma(close, period).to_numpy()
                items[0].setData(x, series)

            elif ind_type == "VWAP" and items:
                typical_price = (high + low + close) / 3.0
                pv = typical_price * volume
                vwap = pv.rolling(window=period, min_periods=1).sum() / volume.rolling(window=period, min_periods=1).sum().replace(0, np.nan)
                items[0].setData(x, vwap.bfill().fillna(close).to_numpy())

            elif ind_type == "BB" and len(items) == 3:
                mid = close.rolling(window=period, min_periods=1).mean()
                std = close.rolling(window=period, min_periods=1).std().fillna(0.0)
                upper = (mid + 2.0 * std).to_numpy()
                lower = (mid - 2.0 * std).to_numpy()
                items[0].setData(x, mid.to_numpy())
                items[1].setData(x, upper)
                items[2].setData(x, lower)

            elif ind_type == "ENVELOPES" and len(items) == 3:
                mid, upper, lower = envelopes(close, period)
                items[0].setData(x, mid.to_numpy())
                items[1].setData(x, upper.to_numpy())
                items[2].setData(x, lower.to_numpy())

            elif ind_type == "DONCHIAN" and len(items) == 3:
                upper = high.rolling(window=period, min_periods=1).max()
                lower = low.rolling(window=period, min_periods=1).min()
                mid = (upper + lower) / 2.0
                items[0].setData(x, upper.to_numpy())
                items[1].setData(x, mid.to_numpy())
                items[2].setData(x, lower.to_numpy())

            elif ind_type == "KELTNER" and len(items) == 3:
                atr_series = atr(high, low, close, period)
                mid = close.ewm(span=period, adjust=False).mean()
                upper = mid + (2.0 * atr_series)
                lower = mid - (2.0 * atr_series)
                items[0].setData(x, mid.to_numpy())
                items[1].setData(x, upper.to_numpy())
                items[2].setData(x, lower.to_numpy())

            elif ind_type == "ICHIMOKU" and len(items) == 5:
                tenkan, kijun, span_a, span_b, chikou = ichimoku(high, low, close)
                items[0].setData(x, tenkan.to_numpy())
                items[1].setData(x, kijun.to_numpy())
                items[2].setData(x, span_a.to_numpy())
                items[3].setData(x, span_b.to_numpy())
                items[4].setData(x, chikou.to_numpy())

            elif ind_type == "ALLIGATOR" and len(items) == 3:
                jaw, teeth, lips = alligator(high, low)
                items[0].setData(x, jaw.to_numpy())
                items[1].setData(x, teeth.to_numpy())
                items[2].setData(x, lips.to_numpy())

            elif ind_type == "SAR" and items:
                sar = parabolic_sar(high, low)
                items[0].setData(
                    x=np.asarray(x, dtype=float),
                    y=sar.to_numpy(),
                    symbol="o",
                    size=5,
                    brush=pg.mkBrush("#90caf9"),
                    pen=mkPen("#90caf9"),
                )

            elif ind_type == "FRACTAL" and len(items) == 2:
                (upper_x, upper_y), (lower_x, lower_y) = self._build_fractal_points(high, low, x, period)
                items[0].setData(
                    x=upper_x,
                    y=upper_y,
                    symbol="t",
                    size=10,
                    brush="#ef5350",
                    pen=mkPen("#ef5350"),
                )
                items[1].setData(
                    x=lower_x,
                    y=lower_y,
                    symbol="t1",
                    size=10,
                    brush="#26a69a",
                    pen=mkPen("#26a69a"),
                )

            elif ind_type == "ZIGZAG" and items:
                zz_x, zz_y = self._build_zigzag_points(high, low, x, period)
                items[0].setData(zz_x, zz_y)

            elif ind_type == "FIBO" and isinstance(items, dict):
                curves = items.get("curves", [])
                labels = items.get("labels", [])
                levels = items.get("levels", [])
                lookback = min(len(df), max(2, int(period)))
                window_high = high.iloc[-lookback:]
                window_low = low.iloc[-lookback:]
                if len(window_high) == 0 or len(window_low) == 0 or len(x) == 0:
                    continue

                high_value = float(window_high.max())
                low_value = float(window_low.min())
                span = high_value - low_value
                if not np.isfinite(span) or span <= 0:
                    span = max(abs(high_value) * 0.001, 1e-9)

                x_start = float(x[max(0, len(x) - lookback)])
                x_end = float(x[-1])
                label_x = x_end + max(width * 2.0, 1.0)

                for index, (ratio, _color) in enumerate(levels):
                    level_price = high_value - (span * float(ratio))
                    curves[index].setData(
                        np.array([x_start, x_end], dtype=float),
                        np.array([level_price, level_price], dtype=float),
                    )
                    labels[index].setHtml(
                        f"<span style='color:#d7dfeb;font-size:11px;'>"
                        f"{ratio * 100:.1f}%  {level_price:.6f}</span>"
                    )
                    labels[index].setPos(label_x, level_price)

            elif ind_type == "ADX" and len(items) == 3:
                adx_line, plus_di, minus_di = adx(high, low, close, period)
                items[0].setData(x, adx_line.to_numpy())
                items[1].setData(x, plus_di.to_numpy())
                items[2].setData(x, minus_di.to_numpy())

            elif ind_type == "ATR" and items:
                items[0].setData(x, atr(high, low, close, period).to_numpy())

            elif ind_type == "STDDEV" and items:
                items[0].setData(x, standard_deviation(close, period).to_numpy())

            elif ind_type == "AC" and items:
                values = accelerator(high, low).to_numpy()
                brushes = [pg.mkBrush("#26a69a" if index == 0 or values[index] >= values[index - 1] else "#ef5350") for index in range(len(values))]
                self._set_histogram_data(items[0], x, values, width, brushes)

            elif ind_type == "AO" and items:
                values = awesome(high, low).to_numpy()
                brushes = [pg.mkBrush("#26a69a" if index == 0 or values[index] >= values[index - 1] else "#ef5350") for index in range(len(values))]
                self._set_histogram_data(items[0], x, values, width, brushes)

            elif ind_type == "CCI" and items:
                items[0].setData(x, cci(high, low, close, period).to_numpy())

            elif ind_type == "DEMARKER" and items:
                items[0].setData(x, demarker(high, low, period).to_numpy())

            elif ind_type == "MACD" and len(items) == 3:
                macd_line, signal_line, histogram = macd(close)
                brushes = [pg.mkBrush("#26a69a" if value >= 0 else "#ef5350") for value in histogram.to_numpy()]
                self._set_histogram_data(items[0], x, histogram.to_numpy(), width, brushes)
                items[1].setData(x, macd_line.to_numpy())
                items[2].setData(x, signal_line.to_numpy())

            elif ind_type == "MOMENTUM" and items:
                items[0].setData(x, momentum(close, period).to_numpy())

            elif ind_type == "OSMA" and items:
                _macd_line, _signal_line, histogram = macd(close)
                brushes = [pg.mkBrush("#26a69a" if value >= 0 else "#ef5350") for value in histogram.to_numpy()]
                self._set_histogram_data(items[0], x, histogram.to_numpy(), width, brushes)

            elif ind_type == "RSI" and items:
                items[0].setData(x, rsi(close, period).to_numpy())

            elif ind_type == "RVI" and len(items) == 2:
                rvi_line, signal_line = rvi(open_, high, low, close, period)
                items[0].setData(x, rvi_line.to_numpy())
                items[1].setData(x, signal_line.to_numpy())

            elif ind_type == "STOCHASTIC" and len(items) == 2:
                percent_k, percent_d = stochastic(high, low, close, period)
                items[0].setData(x, percent_k.to_numpy())
                items[1].setData(x, percent_d.to_numpy())

            elif ind_type == "WPR" and items:
                items[0].setData(x, williams_r(high, low, close, period).to_numpy())

            elif ind_type == "AD" and items:
                items[0].setData(x, accumulation_distribution(high, low, close, volume).to_numpy())

            elif ind_type == "MFI" and items:
                items[0].setData(x, money_flow_index(high, low, close, volume, period).to_numpy())

            elif ind_type == "OBV" and items:
                items[0].setData(x, obv(close, volume).to_numpy())

            elif ind_type == "BULLS POWER" and items:
                items[0].setData(x, bulls_power(high, close).to_numpy())

            elif ind_type == "BEARS POWER" and items:
                items[0].setData(x, bears_power(low, close).to_numpy())

            elif ind_type == "FORCE INDEX" and items:
                items[0].setData(x, force_index(close, volume, period).to_numpy())

            elif ind_type == "GATOR" and len(items) == 2:
                upper, lower = gator(high, low)
                upper_brushes = [pg.mkBrush("#26a69a" if value >= 0 else "#ef5350") for value in upper.to_numpy()]
                lower_brushes = [pg.mkBrush("#ef5350" if value < 0 else "#26a69a") for value in lower.to_numpy()]
                self._set_histogram_data(items[0], x, upper.to_numpy(), width, upper_brushes)
                self._set_histogram_data(items[1], x, lower.to_numpy(), width, lower_brushes)

            elif ind_type == "BW_MFI" and items:
                values, colors = market_facilitation_index(high, low, volume)
                self._set_histogram_data(items[0], x, values.to_numpy(), width, [pg.mkBrush(color) for color in colors])

            elif ind_type == "VOLUMES":
                continue

    def update_candles(self, df):
        if df is None or len(df) == 0:
            return

        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(set(df.columns)):
            return

        x = self._extract_time_axis(df)
        width = self._infer_candle_width(x)
        self._sync_view_context()
        self._last_df = df.copy()
        self._last_x = np.array(x, dtype=float)
        self._last_candle_stats = self._build_candle_stats(self._last_df, self._last_x)

        candles = np.column_stack(
            [
                x,
                df["open"].astype(float).to_numpy(),
                df["close"].astype(float).to_numpy(),
                df["low"].astype(float).to_numpy(),
                df["high"].astype(float).to_numpy(),
            ]
        )

        self._last_candles = candles
        self.candle_item.set_body_width(width)
        self.candle_item.setData(candles)
        self.ema_curve.setData([], [])

        volume = df["volume"].astype(float).to_numpy()
        colors = [self.candle_up_color if c >= o else self.candle_down_color for o, c in zip(df["open"], df["close"])]
        brushes = [pg.mkBrush(c) for c in colors]
        self.volume_bars.setOpts(x=x, height=volume, width=width, brushes=brushes)

        self._update_indicators(df, x, width)

        if self._should_fit_chart_view(self._last_x):
            self._fit_chart_view(self._last_candle_stats, width)
        self.refresh_context_display()
        self._update_ohlcv_for_x(self._last_x[-1] if len(self._last_x) else 0.0)

        try:
            last_close = float(df["close"].iloc[-1])
            prev_close = float(df["close"].iloc[-2]) if len(df) > 1 else last_close
            line_color = self.candle_up_color if last_close >= prev_close else self.candle_down_color
            self.last_line.setPen(mkPen(line_color, width=1.15))
            self.last_line.label.fill = pg.mkBrush(pg.mkColor(line_color))
            self.last_line.label.setColor(pg.mkColor("#ffffff"))
            self.last_line.setPos(last_close)
            self.last_line.setVisible(True)
        except Exception:
            pass

    def update_price_lines(self, bid: float, ask: float, last: float | None = None):
        try:
            bid_f = float(bid)
            ask_f = float(ask)
        except Exception:
            return

        self._last_bid = bid_f
        self._last_ask = ask_f

        if bid_f > 0:
            self.bid_line.setPos(bid_f)
            self.bid_line.setVisible(self.show_bid_ask_lines)

        if ask_f > 0:
            self.ask_line.setPos(ask_f)
            self.ask_line.setVisible(self.show_bid_ask_lines)

        if last is None:
            last_f = (bid_f + ask_f) / 2.0 if (bid_f > 0 and ask_f > 0) else 0.0
        else:
            try:
                last_f = float(last)
            except Exception:
                last_f = 0.0

        if last_f > 0:
            self.last_line.setPos(last_f)
            self.last_line.setVisible(True)

    def set_bid_ask_lines_visible(self, visible: bool):
        self.show_bid_ask_lines = bool(visible)
        self.bid_line.setVisible(self.show_bid_ask_lines and self._last_bid is not None and self._last_bid > 0)
        self.ask_line.setVisible(self.show_bid_ask_lines and self._last_ask is not None and self._last_ask > 0)

    def set_candle_colors(self, up_color: str, down_color: str):
        self.candle_up_color = up_color
        self.candle_down_color = down_color
        self.candle_item.set_colors(up_color, down_color)
        if self._last_candles is not None:
            self.candle_item.setData(self._last_candles)

    def link_all_charts(self, _count):
        return
