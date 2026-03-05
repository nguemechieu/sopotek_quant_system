import logging
import numpy as np

logger = logging.getLogger(__name__)


class InstitutionalRiskEngine:

    def __init__(
            self,
            account_equity: float,
            max_risk_per_trade: float = 0.01,
            max_portfolio_risk: float = 0.05,
            max_daily_drawdown: float = 0.03,
            max_position_size_pct: float = 0.20,
            max_gross_exposure_pct: float = 1.5,
    ):

        self.account_equity = account_equity

        self.max_risk_per_trade = max_risk_per_trade
        self.max_portfolio_risk = max_portfolio_risk
        self.max_daily_drawdown = max_daily_drawdown
        self.max_position_size_pct = max_position_size_pct
        self.max_gross_exposure_pct = max_gross_exposure_pct

        self.daily_loss = 0

    # -------------------------------------------------
    # EQUITY UPDATE
    # -------------------------------------------------

    def update_equity(self, equity):
        self.account_equity = equity

    # -------------------------------------------------
    # POSITION SIZE
    # -------------------------------------------------

    def position_size(self, entry_price, stop_price, confidence=1.0, volatility=None):

        risk_capital = self.account_equity * self.max_risk_per_trade

        stop_distance = abs(entry_price - stop_price)

        if stop_distance <= 0:
            return 0

        base_size = risk_capital / stop_distance

        if volatility:
            base_size = base_size / max(volatility, 1e-8)

        adjusted_size = base_size * confidence

        max_position_value = self.account_equity * self.max_position_size_pct
        max_size_allowed = max_position_value / entry_price


        final_size = min(adjusted_size, max_size_allowed)

        return max(final_size, 0)

    # -------------------------------------------------
    # TRADE VALIDATION
    # -------------------------------------------------

    def validate_trade(self, signal, portfolio):

        entry = signal["entry_price"]
        stop = signal["stop_price"]

        size = self.position_size(entry, stop)

        stop_distance = abs(entry - stop)

        potential_loss = size * stop_distance

        max_allowed_loss = self.account_equity * self.max_risk_per_trade

        if potential_loss > max_allowed_loss:
            return False, "Per trade risk exceeded"

        gross_exposure = self._gross_exposure(portfolio)

        if gross_exposure > self.account_equity * self.max_gross_exposure_pct:
            return False, "Gross exposure exceeded"

        if self.daily_loss < -self.account_equity * self.max_daily_drawdown:
            return False, "Daily drawdown exceeded"


        return True, "Approved"

    # -------------------------------------------------
    # PORTFOLIO EXPOSURE
    # -------------------------------------------------

    def _gross_exposure(self, portfolio):

        exposure = 0

        for p in portfolio:
            exposure += abs(p["quantity"] * p["entry_price"])

        return exposure

    # -------------------------------------------------
    # VALUE AT RISK (VaR)
    # -------------------------------------------------

    def value_at_risk(self, returns, confidence_level=0.95):

        """
        Historical VaR
        """

        returns = np.array(returns)

        percentile = (1 - confidence_level) * 100

        var = np.percentile(returns, percentile)

        return abs(var) * self.account_equity

    # -------------------------------------------------
    # CONDITIONAL VAR (Expected Shortfall)
    # -------------------------------------------------

    def conditional_var(self, returns, confidence_level=0.95):

        returns = np.array(returns)

        var_threshold = np.percentile(returns, (1 - confidence_level) * 100)

        losses = returns[returns <= var_threshold]

        if len(losses) == 0:
            return 0

        cvar = losses.mean()

        return abs(cvar) * self.account_equity

    # -------------------------------------------------
    # MONTE CARLO RISK SIMULATION
    # -------------------------------------------------

    def monte_carlo_var(self, returns, simulations=5000, horizon=1):

        returns = np.array(returns)

        mean = returns.mean()
        std = returns.std()

        simulated_returns = np.random.normal(
            mean,
            std,
            (simulations, horizon)
        )

        simulated_paths = simulated_returns.sum(axis=1)

        var = np.percentile(simulated_paths, 5)

        return abs(var) * self.account_equity

    # -------------------------------------------------
    # DAILY PNL CONTROL
    # -------------------------------------------------

    def update_daily_pnl(self, pnl):

        self.daily_loss += pnl

        if self.daily_loss < -self.account_equity * self.max_daily_drawdown:

            logger.critical("Daily drawdown breached")

            return False

        return True

    # -------------------------------------------------
    # FRACTIONAL KELLY
    # -------------------------------------------------

    def kelly_fraction(self, win_rate, win_loss_ratio, fraction=0.25):

        b = win_loss_ratio
        p = win_rate
        q = 1 - p

        kelly = (b * p - q) / b

        return max(min(kelly * fraction, 1), 0)