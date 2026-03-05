import logging

import pandas as pd
import numpy as np
from datetime import datetime


class PerformanceEngine:

    def __init__(self, controller):
        self.logger = logging.getLogger(__name__)
        self.controller = controller
        self.trades = []
        self.equity_history = []
        self.timestamps = []
        self.starting_equity = 0.0
        self.current_timeframe = 0
        self.auto_trading_enabled = False



    # ==========================================
    # INITIALIZE
    # ==========================================

    def set_starting_equity(self, equity: float):
        self.starting_equity = equity
        self.equity_history = [equity]
        self.timestamps = [datetime.now()]

    # ==========================================
    # REGISTER TRADE
    # ==========================================

    def record_trade(self, trade: dict):
        """
        trade format:
        {
            "symbol": "BTC/USDT",
            "side": "BUY",
            "entry": 50000,
            "exit": 50500,
            "size": 0.01,
            "pnl": 5.0,
            "strategy": "LSTM"
        }
        """

        self.trades.append(trade)
        last_equity = self.equity_history[-1]
        self.equity_history.append(last_equity)
        self.timestamps.append(datetime.now())
        
        self.logger.info(self.equity_history)
        new_equity = last_equity + trade["pnl"]

        self.equity_history.append(new_equity)
        self.timestamps.append(datetime.now())

    # ==========================================
    # METRICS
    # ==========================================

    def metrics(self):

        if not self.trades:
            return {}

        df = pd.DataFrame(self.trades)

        total_trades = len(df)
        wins = len(df[df.pnl > 0])
        losses = len(df[df.pnl <= 0])

        win_rate = wins / total_trades * 100
        net_profit = df.pnl.sum()
        avg_win = df[df.pnl > 0].pnl.mean() if wins else 0
        avg_loss = df[df.pnl <= 0].pnl.mean() if losses else 0

        sharpe = self._sharpe_ratio()
        max_dd = self._max_drawdown()

        return {
            "Total Trades": total_trades,
            "Win Rate (%)": round(win_rate, 2),
            "Net Profit": round(net_profit, 2),
            "Average Win": round(avg_win, 2),
            "Average Loss": round(avg_loss, 2),
            "Sharpe Ratio": round(sharpe, 3),
            "Max Drawdown (%)": round(max_dd, 2)
        }

    # ==========================================
    # SHARPE
    # ==========================================

    def _sharpe_ratio(self):
        returns = np.diff(self.equity_history)/self.equity_history[:-1]
        if len(returns) < 2:
            return 0
        return np.mean(returns)/np.std(returns)*np.sqrt(252)

    # ==========================================
    # MAX DRAWDOWN
    # ==========================================

    def _max_drawdown(self):

        peak = self.equity_history[0]
        max_dd = 0

        for value in self.equity_history:
            if value > peak:
                peak = value

            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)

        return max_dd * 100