import numpy as np

from quant.analytics.metrics import Metrics
from quant.analytics.risk_metrics import RiskMetrics


class PerformanceEngine:

    def __init__(self):
        self.equity_curve = []
        self.equity_history = self.equity_curve
        self.trades = []

    # =====================================
    # UPDATE EQUITY
    # =====================================

    def update_equity(self, equity):
        self.equity_curve.append(equity)

    def record_trade(self, trade):
        if trade is None:
            return
        self.trades.append(dict(trade))

    # =====================================
    # REPORT
    # =====================================

    def report(self):
        if len(self.equity_curve) < 2:
            return {}

        equity = np.array(self.equity_curve)

        returns = Metrics.returns(equity)

        report = {

            "cumulative_return":
                Metrics.cumulative_return(equity),

            "volatility":
                Metrics.volatility(returns),

            "sharpe_ratio":
                Metrics.sharpe_ratio(returns),

            "sortino_ratio":
                Metrics.sortino_ratio(returns),

            "max_drawdown":
                RiskMetrics.max_drawdown(equity),

            "value_at_risk":
                RiskMetrics.var(returns),

            "conditional_var":
                RiskMetrics.cvar(returns),
        }

        return report
