class RiskEngine:

    def __init__(
            self,
            account_equity,
            max_portfolio_risk=0.1,
            max_risk_per_trade=0.02,
            max_position_size_pct=0.1,
            max_gross_exposure_pct=2.0
    ):

        self.account_equity = max(1.0, self._safe_float(account_equity, 10000.0))

        self.max_portfolio_risk = max(0.001, self._safe_float(max_portfolio_risk, 0.1))
        self.max_risk_per_trade = max(0.001, self._safe_float(max_risk_per_trade, 0.02))
        self.max_position_size_pct = max(0.001, self._safe_float(max_position_size_pct, 0.1))
        self.max_gross_exposure_pct = max(0.01, self._safe_float(max_gross_exposure_pct, 2.0))

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def sync_equity(self, equity):
        value = self._safe_float(equity, self.account_equity)
        if value > 0:
            self.account_equity = value

    def max_position_notional(self):
        return max(0.0, self.account_equity * self.max_position_size_pct)

    def max_position_quantity(self, price):
        trade_price = self._safe_float(price, 0.0)
        if trade_price <= 0:
            return 0.0
        return self.max_position_notional() / trade_price

    def adjust_trade(self, price, quantity):
        trade_price = self._safe_float(price, 0.0)
        requested_quantity = abs(self._safe_float(quantity, 0.0))
        if trade_price <= 0 or requested_quantity <= 0:
            return False, 0.0, "Invalid trade payload"

        requested_notional = trade_price * requested_quantity
        max_notional = self.max_position_notional()
        if max_notional <= 0:
            return False, 0.0, "Position size cap is zero"

        if requested_notional <= max_notional:
            return True, requested_quantity, "Approved"

        adjusted_quantity = max_notional / trade_price
        if adjusted_quantity <= 0:
            return False, 0.0, "Position size cap reduced trade to zero"

        return (
            True,
            adjusted_quantity,
            f"Position size reduced to fit {self.max_position_size_pct:.1%} max position cap",
        )

    # =====================================
    # VALIDATE TRADE
    # =====================================

    def validate_trade(self, price, quantity):
        approved, adjusted_quantity, reason = self.adjust_trade(price, quantity)
        if not approved:
            return False, reason
        if adjusted_quantity + 1e-12 < abs(self._safe_float(quantity, 0.0)):
            return False, "Position size too large"
        return True, reason

    # =====================================
    # POSITION SIZE
    # =====================================

    def position_size(self, entry_price, stop_price):

        risk_amount = self.account_equity * self.max_risk_per_trade

        risk_per_unit = abs(entry_price - stop_price)

        if risk_per_unit == 0:
            return 0

        size = risk_amount / risk_per_unit
        max_size = self.max_position_quantity(entry_price)
        if max_size > 0:
            size = min(size, max_size)

        return size
