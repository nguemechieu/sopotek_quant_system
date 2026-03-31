class RiskEngine:

    def __init__(
            self,
            account_equity,
            max_portfolio_risk=0.1,
            max_risk_per_trade=0.02,
            max_position_size_pct=0.1,
            max_gross_exposure_pct=2.0
    ):

        self.account_equity = max(0.0, self._safe_float(account_equity, 10000.0))

        self.max_portfolio_risk = max(0.0, self._safe_float(max_portfolio_risk, 0.1))
        self.max_risk_per_trade = max(0.0, self._safe_float(max_risk_per_trade, 0.02))
        self.max_position_size_pct = max(0.0, self._safe_float(max_position_size_pct, 0.1))
        self.max_gross_exposure_pct = max(0.0, self._safe_float(max_gross_exposure_pct, 2.0))

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def sync_equity(self, equity):
        value = self._safe_float(equity, self.account_equity)
        if value >= 0:
            self.account_equity = value

    def max_position_notional(self):
        return max(0.0, self.account_equity * self.max_position_size_pct)

    def max_position_quantity(self, price):
        trade_price = self._safe_float(price, 0.0)
        if trade_price <= 0:
            return 0.0
        return self.max_position_notional() / trade_price

    def _normalize_quote_to_account_rate(self, value):
        rate = self._safe_float(value, 1.0)
        return rate if rate > 0 else 1.0

    def risk_per_unit(self, entry_price, stop_price, quote_to_account_rate=1.0):
        entry = self._safe_float(entry_price, 0.0)
        stop = self._safe_float(stop_price, 0.0)
        if entry <= 0 or stop <= 0:
            return 0.0
        return abs(entry - stop) * self._normalize_quote_to_account_rate(quote_to_account_rate)

    def max_risk_quantity(self, entry_price, stop_price, quote_to_account_rate=1.0):
        risk_amount = self.account_equity * self.max_risk_per_trade
        if risk_amount <= 0:
            return 0.0
        risk_per_unit = self.risk_per_unit(
            entry_price,
            stop_price,
            quote_to_account_rate=quote_to_account_rate,
        )
        if risk_per_unit <= 0:
            return 0.0
        return risk_amount / risk_per_unit

    def stop_distance_pips(self, entry_price, stop_price, pip_size=None):
        pip_value = self._safe_float(pip_size, 0.0)
        if pip_value <= 0:
            return None
        distance = abs(self._safe_float(entry_price, 0.0) - self._safe_float(stop_price, 0.0))
        if distance <= 0:
            return None
        return distance / pip_value

    def adjust_trade(self, price, quantity, *, stop_price=None, quote_to_account_rate=1.0, pip_size=None, symbol=None):
        trade_price = self._safe_float(price, 0.0)
        requested_quantity = abs(self._safe_float(quantity, 0.0))
        if trade_price <= 0 or requested_quantity <= 0:
            return False, 0.0, "Invalid trade payload"

        requested_notional = trade_price * requested_quantity
        max_notional = self.max_position_notional()
        if max_notional <= 0:
            return False, 0.0, "Position size cap is zero"

        max_quantity = max_notional / trade_price
        limiting_reason = None
        limiting_quantity = max_quantity

        stop_value = self._safe_float(stop_price, 0.0)
        if stop_value > 0 and abs(stop_value - trade_price) > 1e-12:
            max_risk_quantity = self.max_risk_quantity(
                trade_price,
                stop_value,
                quote_to_account_rate=quote_to_account_rate,
            )
            if max_risk_quantity > 0 and max_risk_quantity < limiting_quantity:
                limiting_quantity = max_risk_quantity
                stop_distance_pips = self.stop_distance_pips(trade_price, stop_value, pip_size=pip_size)
                if stop_distance_pips is not None:
                    limiting_reason = (
                        f"Position size reduced to fit {self.max_risk_per_trade:.1%} max risk "
                        f"at {stop_distance_pips:.1f} pip stop"
                    )
                else:
                    limiting_reason = (
                        f"Position size reduced to fit {self.max_risk_per_trade:.1%} max risk "
                        "at the current stop distance"
                    )

        if requested_quantity <= limiting_quantity:
            return True, requested_quantity, "Approved"

        adjusted_quantity = limiting_quantity
        if adjusted_quantity <= 0:
            return False, 0.0, "Position size cap reduced trade to zero"

        return (
            True,
            adjusted_quantity,
            limiting_reason or f"Position size reduced to fit {self.max_position_size_pct:.1%} max position cap",
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

    def position_size(self, entry_price, stop_price, *, quote_to_account_rate=1.0, pip_size=None, symbol=None):
        risk_per_unit = self.risk_per_unit(
            entry_price,
            stop_price,
            quote_to_account_rate=quote_to_account_rate,
        )
        if risk_per_unit <= 0:
            return 0

        size = self.max_risk_quantity(
            entry_price,
            stop_price,
            quote_to_account_rate=quote_to_account_rate,
        )
        max_size = self.max_position_quantity(entry_price)
        if max_size > 0:
            size = min(size, max_size)

        return size
