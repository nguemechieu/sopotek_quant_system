import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BacktestEngine:

    def __init__(
            self,
            strategy,
            risk_engine,
            initial_balance=10000
    ):

        self.strategy = strategy
        self.risk_engine = risk_engine

        self.balance = initial_balance
        self.equity = initial_balance

        self.position = None

        self.trades = []

    async def run(self, df: pd.DataFrame, symbol="BTC/USDT"):

        for i in range(200, len(df)):

            window = df.iloc[:i]

            signal = await self.strategy.generate_signal(symbol, window)

            price = window.iloc[-1]["close"]

            # -------------------------------------------------
            # OPEN POSITION
            # -------------------------------------------------

            if signal and not self.position:

                entry =float( signal["entry_price"])

                stop = signal["stop_price"]

                size = self.risk_engine.position_size(
                    entry,
                    stop,
                    signal.get("confidence", 1),
                    signal.get("volatility")
                )

                if size <= 0:
                    continue

                self.position = {
                    "side": signal["signal"],
                    "entry": entry,
                    "size": size,
                    "stop": stop
                }

                logger.info(f"Open {signal['signal']} at {entry}")

            # -------------------------------------------------
            # POSITION MANAGEMENT
            # -------------------------------------------------

            if self.position:

                side = self.position["side"]
                entry = self.position["entry"]
                size = self.position["size"]
                stop = self.position["stop"]



                if side == "BUY":

                    if price <= stop:
                        pnl = (stop - entry) * size
                        self.close_trade(pnl)

                if side == "SELL":

                    if price >= stop:
                        pnl = (entry - stop) * size
                        self.close_trade(pnl)

        return self.performance()

    def close_trade(self, pnl):

        self.balance += pnl

        self.trades.append(pnl)

        self.position = None

    # -------------------------------------------------
    # PERFORMANCE METRICS
    # -------------------------------------------------

    def performance(self):

        if len(self.trades) == 0:
            return {}

        wins = [t for t in self.trades if t > 0]
        losses = [t for t in self.trades if t <= 0]

        win_rate = len(wins) / len(self.trades)

        total_profit = sum(self.trades)

        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0

        return {
            "total_trades": len(self.trades),
            "win_rate": win_rate,
            "profit": total_profit,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "balance": self.balance
        }