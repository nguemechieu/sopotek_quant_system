import numpy as np


class RiskEngine:

    def __init__(
            self,
            account_equity: float=0.0,
            max_portfolio_risk: float = 0.2,        # 20% total exposure
            max_risk_per_trade: float = 0.02,       # 2% per trade
            max_daily_drawdown: float = 0.1,        # 10%
            max_position_size_pct: float = 0.1,     # 10% of equity
    ):

        self.account_equity = account_equity
        self.initial_equity = account_equity

        self.max_portfolio_risk = max_portfolio_risk
        self.max_risk_per_trade = max_risk_per_trade
        self.max_daily_drawdown = max_daily_drawdown
        self.max_position_size_pct = max_position_size_pct

        self.current_drawdown = 0.0

    # =========================================================
    # EQUITY UPDATE
    # =========================================================

    async def update_equity(self, equity: float):

        self.account_equity = equity

        drawdown =0.01# (self.initial_equity - equity) / self.initial_equity

        self.current_drawdown = max(0.0, drawdown)

    # =========================================================
    # TRADE VALIDATION
    # =========================================================

    def validate_trade(self, signal: dict, positions: list) -> bool:

        if self.current_drawdown >= self.max_daily_drawdown:
            return False

        total_exposure = sum(
            abs(p.get("amount", 0) * p.get("entry_price", 0))
            for p in positions
        )

        if total_exposure > self.account_equity * self.max_portfolio_risk:
            return False

        return True

    # =========================================================
    # POSITION SIZING
    # =========================================================

    def position_size(
            self,
            entry_price: float,
            stop_price: float,
            confidence: float,
            volatility: float
    ) -> float:

        if entry_price <= 0 or stop_price <= 0:
            return 0.0

        risk_per_unit = abs(entry_price - stop_price)

        if risk_per_unit == 0:
            return 0.0

        # Base risk capital
        capital_at_risk = self.account_equity * self.max_risk_per_trade

        # Confidence weighting (0–1)
        capital_at_risk *= confidence

        # Volatility scaling (inverse)
        if volatility > 0:
            capital_at_risk /= (1 + volatility)

        raw_size = capital_at_risk / risk_per_unit

        # Hard cap on position size
        max_position_value = self.account_equity * self.max_position_size_pct
        capped_size = min(raw_size, max_position_value / entry_price)

        return max(0.0, capped_size)

    # =========================================================
    # VALUE AT RISK
    # =========================================================

    def value_at_risk(self, returns, confidence_level=0.95):

        if len(returns) < 10:
            return 0.0

        return np.percentile(returns, (1 - confidence_level) * 100)

    def conditional_var(self, returns, confidence_level=0.95):

        if len(returns) < 10:
            return 0.0

        var = self.value_at_risk(returns, confidence_level)

        losses = [r for r in returns if r <= var]

        if not losses:
            return 0.0

        return np.mean(losses)

    def monte_carlo_var(self, returns, simulations=1000, confidence_level=0.95):

        if len(returns) < 10:
            return 0.0

        simulated = np.random.choice(returns, size=(simulations, len(returns)))

        simulated_means = simulated.mean(axis=1)

        return np.percentile(simulated_means, (1 - confidence_level) * 100)