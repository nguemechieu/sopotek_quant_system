import json

import pandas as pd
import pyqtgraph as pg
import websockets


class CandleChart(pg.GraphicsLayoutWidget):
    def __init__(self, ohclv_):
        super().__init__()

        self.ohclv = ohclv_
        self.setWindowTitle("Sopotek MT4 Chart")
        self.resize(1000, 600)

        self.plot = self.addPlot()
        self.plot.showGrid(x=True, y=True)

        self.data = []

    def update_chart(self, candle):

        self.data.append(candle)

        df = pd.DataFrame(self.data)

        if len(df) < 2:
            return

        self.plot.clear()

        for i in range(len(df)):
            open_ = df["open"][i]
            close_ = df["close"][i]
            high_ = df["high"][i]
            low_ = df["low"][i]

            color = "g" if close_ >= open_ else "r"

            self.plot.plot(
                [i, i],
                [low_, high_],
                pen=pg.mkPen(color)
            )

            self.plot.plot(
                [i - 0.2, i + 0.2],
                [open_, open_],
                pen=pg.mkPen(color)
            )

            self.plot.plot(
                [i - 0.2, i + 0.2],
                [close_, close_],
                pen=pg.mkPen(color)
            )

    def addPlot(self):
        self.plot.clear()
        self.data = self.ohclv
        self.update_chart(self.data)



async def websocket_stream(chart):

    url = "wss://stream.binance.us:9443/ws/btcusdt@kline_1m"

    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            k = data["k"]

            if not k["x"]:  # only closed candle
                continue

            candle = {
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
            }

            chart.update_chart(candle)


