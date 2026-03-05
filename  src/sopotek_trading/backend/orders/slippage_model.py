class SlippageModel:

    def __init__(self, slippage_rate=0.001):
        self.slippage_rate = slippage_rate

    def adjust_price(self, price, side):
        if side == "BUY":
            return price * (1 + self.slippage_rate)
        elif side == "SELL":
            return price * (1 - self.slippage_rate)
        return price