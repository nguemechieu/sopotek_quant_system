class StrategyMetrics:

    def __init__(self):

        self.stats = {}

    def register(self, name):

        if name not in self.stats:
            self.stats[name] = {
                "trades": 0,
                "wins": 0,
                "pnl": 0,
                "sharpe": 0
            }

    def update(self, name, pnl):

        s = self.stats[name]

        s["trades"] += 1
        s["pnl"] += pnl

        if pnl > 0:
            s["wins"] += 1

    def win_rate(self, name):

        s = self.stats[name]

        if s["trades"] == 0:
            return 0

        return s["wins"] / s["trades"]