import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer


class ReportGenerator:

    def __init__(self, trades: list[dict], equity_history: list[float]):
        self.trades = trades
        self.equity_history = equity_history


    # ======================================================
    # PERFORMANCE METRICS
    # ======================================================

    def calculate_metrics(self):

        df = pd.DataFrame(self.trades)

        if df.empty:
            return {}

        total_trades = len(df)
        wins = len(df[df["pnl"] > 0])
        losses = len(df[df["pnl"] <= 0])
        win_rate = wins/(total_trades * 100)
        net_profit = df["pnl"].sum()
        max_drawdown = self._max_drawdown()

        return {
            "Total Trades": total_trades,
            "Win Rate (%)": round(win_rate, 2),
            "Net Profit": round(net_profit, 2),
            "Max Drawdown": round(max_drawdown, 2),
            "Losses": round(losses, 2)
        }

    # ======================================================
    # MAX DRAWDOWN
    # ======================================================

    def _max_drawdown(self):

        peak = self.equity_history[0]
        max_dd = 0

        for value in self.equity_history:
            if value > peak:
                peak = value

            drawdown = (peak - value) / peak
            max_dd = max(max_dd, drawdown)

        return max_dd * 100

    # ======================================================
    # EXPORT EQUITY CURVE IMAGE
    # ======================================================

    def save_equity_curve(self, filename="equity_curve.png"):

        plt.figure()
        plt.plot(self.equity_history)
        plt.title("Equity Curve")
        plt.xlabel("Trades")
        plt.ylabel("Equity")
        plt.grid(True)
        plt.savefig(filename)
        plt.close()

    # ======================================================
    # EXPORT PDF REPORT
    # ======================================================

    def export_pdf(self, filename="Trading_Report.pdf"):

        doc = SimpleDocTemplate(filename)
        styles = getSampleStyleSheet()
        elements = []

        metrics = self.calculate_metrics()

        elements.append(Paragraph("Sopotek Trading Report", styles["Heading1"]))
        elements.append(Spacer(1, 12))

        for key, value in metrics.items():
            elements.append(
                Paragraph(f"{key}: {value}", styles["Normal"])
            )
            elements.append(Spacer(1, 6))

        doc.build(elements)

    # ======================================================
    # EXPORT EXCEL
    # ======================================================

    def export_excel(self, filename="Trading_Report.xlsx"):

        df = pd.DataFrame(self.trades)
        df.to_excel(filename, index=False)