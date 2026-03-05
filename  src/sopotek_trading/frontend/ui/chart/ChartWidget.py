import numpy as np
import pyqtgraph as pg

from PySide6 import QtCore
from PySide6.QtWidgets import QTabWidget
from pyqtgraph import (
    GraphicsLayoutWidget,
    ScatterPlotItem,
    mkPen,
    InfiniteLine,
    TextItem,
    SignalProxy,
    ImageItem
)

from sopotek_trading.frontend.ui.chart.chart_items import CandlestickItem


class ChartWidget(GraphicsLayoutWidget):

    sigMouseMoved = QtCore.Signal(object)

    def __init__(self, symbol: str, time_frame: str, controller):

        super().__init__()

        self.controller = controller
        self.symbol = symbol
        self.time_frame = time_frame
        self.chart_tabs=QTabWidget()

        self.setBackground("k")

        # ===============================
        # INTERNAL BUFFERS
        # ===============================

        self.heatmap_buffer = []
        self.max_heatmap_rows = 200

        # ===============================
        # PRICE CHART
        # ===============================

        self.price_plot = self.addPlot(row=0, col=0)
        self.price_plot.showGrid(x=True, y=True)

        self.price_plot.setLabel("left", "Price")
        self.price_plot.setLabel("bottom", "Time")

        # Candles
        self.candle_item = CandlestickItem()
        self.price_plot.addItem(self.candle_item)

        # EMA
        self.ema_curve = self.price_plot.plot(
            pen=mkPen("yellow", width=2)
        )

        # Strategy signals
        self.signal_markers = ScatterPlotItem()
        self.price_plot.addItem(self.signal_markers)

        # Trades
        self.trade_scatter = ScatterPlotItem()
        self.price_plot.addItem(self.trade_scatter)

        # ===============================
        # VOLUME CHART
        # ===============================

        self.volume_plot = self.addPlot(row=1, col=0)

        self.volume_plot.setXLink(self.price_plot)
        self.volume_plot.showGrid(x=True, y=True)

        self.volume_plot.setLabel("left", "Volume")

        self.volume_bars = pg.BarGraphItem(
            x=[],
            height=[],
            width=0.6,
            brush="b"
        )

        self.volume_plot.addItem(self.volume_bars)

        # ===============================
        # ORDERBOOK HEATMAP
        # ===============================

        self.heatmap_plot = self.addPlot(row=2, col=0)

        self.heatmap_plot.setXLink(self.price_plot)
        self.heatmap_plot.showGrid(x=True, y=True)

        self.heatmap_image = ImageItem()

        colormap = pg.colormap.get("inferno")
        self.heatmap_image.setLookupTable(
            colormap.getLookupTable()
        )

        self.heatmap_plot.addItem(self.heatmap_image)

        # ===============================
        # CROSSHAIR
        # ===============================

        self.v_line = InfiniteLine(angle=90, movable=False, pen=mkPen("y"))
        self.h_line = InfiniteLine(angle=0, movable=False, pen=mkPen("y"))

        self.price_plot.addItem(self.v_line, ignoreBounds=True)
        self.price_plot.addItem(self.h_line, ignoreBounds=True)

        self.text_item = TextItem(color="w")
        self.price_plot.addItem(self.text_item)

        self.proxy = SignalProxy(
            self.scene().sigMouseMoved,
            rateLimit=60,
            slot=self._mouse_moved
        )

        # Layout proportions
        self.ci.layout.setRowStretchFactor(0, 25)
        self.ci.layout.setRowStretchFactor(1, 4)
        self.ci.layout.setRowStretchFactor(2, 4)

    # ======================================================
    # CROSSHAIR
    # ======================================================

    def _mouse_moved(self, evt):

        pos = evt[0]

        if self.price_plot.sceneBoundingRect().contains(pos):

            mouse_point = self.price_plot.vb.mapSceneToView(pos)

            x = mouse_point.x()
            y = mouse_point.y()

            self.v_line.setPos(x)
            self.h_line.setPos(y)

            self.text_item.setHtml(
                f"<span style='color:white'>Price: {y:.4f}</span>"
            )

            self.text_item.setPos(x, y)

    # ======================================================
    # ORDERBOOK HEATMAP
    # ======================================================

    def update_orderbook_heatmap(self, bids, asks):

        if not bids or not asks:
            return

        volumes = []

        for price, volume in bids + asks:
            volumes.append(float(volume))

        if not volumes:
            return

        max_vol = max(volumes)

        normalized = [v / max_vol for v in volumes]

        self.heatmap_buffer.append(normalized)

        if len(self.heatmap_buffer) > self.max_heatmap_rows:
            self.heatmap_buffer.pop(0)

        heatmap_array = np.array(self.heatmap_buffer).T

        self.heatmap_image.setImage(
            heatmap_array,
            autoLevels=False
        )

    # ======================================================
    # STRATEGY SIGNAL
    # ======================================================

    def add_strategy_signal(self, index, price, signal):

        if signal == "BUY":
            symbol = "t1"
            color = "green"

        elif signal == "SELL":
            symbol = "t"
            color = "red"

        else:
            return

        self.signal_markers.addPoints(
            x=[index],
            y=[price],
            symbol=symbol,
            brush=color,
            size=14
        )

    # ======================================================
    # UPDATE CANDLES (FAST)
    # ======================================================

    def update_candles(self, df):

        if df is None or len(df) == 0:
            return

        x = np.arange(len(df))

        candles = np.column_stack([
            x,
            df["open"].values,
            df["close"].values,
            df["low"].values,
            df["high"].values
        ])

        # Update candles
        for i, candle in enumerate(candles):
         self.candle_item.setData(i,candles.item(i))

        # ===============================
        # EMA
        # ===============================

        ema = df["close"].ewm(span=21).mean()

        self.ema_curve.setData(x, ema)

        # ===============================
        # VOLUME
        # ===============================

        volume = df["volume"].values

        self.volume_bars.setOpts(
            x=x,
            height=volume
        )

        # Auto scale
        self.price_plot.enableAutoRange()

    def link_all_charts(self,count):
        charts = []
        for i in range(count):
            widget = self.chart_tabs.widget(i)
            if isinstance(widget, ChartWidget): charts.append(widget)
            if len(charts) < 2:
                return
            base = charts[0]
            for chart in charts[1:]: chart.link_to(base)
