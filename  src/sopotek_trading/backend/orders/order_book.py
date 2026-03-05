class OrderBookSimulator:

    def __init__(self, bids, asks):
        self.bids = bids
        self.asks = asks

    def execute_market_buy(self, quantity):
        total_cost = 0
        remaining = quantity

        for price, volume in self.asks:
            fill = min(volume, remaining)
            total_cost += fill * price
            remaining -= fill

            if remaining <= 0:
                break

        return total_cost / quantity