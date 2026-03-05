class OrderbookAnalyzer:

    def __init__(self, depth=10):

        self.depth = depth

    # ========================================
    # ORDERBOOK IMBALANCE
    # ========================================

    def imbalance(self, bids, asks):

        bid_volume = sum(b[1] for b in bids[:self.depth])
        ask_volume = sum(a[1] for a in asks[:self.depth])

        if bid_volume + ask_volume == 0:
            return 0

        return (bid_volume - ask_volume) / (bid_volume + ask_volume)

    # ========================================
    # SPREAD
    # ========================================

    def spread(self, bids, asks):

        if not bids or not asks:
            return 0

        best_bid = bids[0][0]
        best_ask = asks[0][0]

        return best_ask - best_bid

    # ========================================
    # LIQUIDITY WALL DETECTION
    # ========================================

    def detect_walls(self, levels, threshold=10):

        walls = []

        for price, volume in levels:

            if volume >= threshold:
                walls.append((price, volume))

        return walls

    # ========================================
    # MARKET PRESSURE SCORE
    # ========================================

    def pressure(self, bids, asks):

        imbalance = self.imbalance(bids, asks)

        bid_volume = sum(b[1] for b in bids[:self.depth])
        ask_volume = sum(a[1] for a in asks[:self.depth])

        volume_ratio = bid_volume / (ask_volume + 1e-9)

        return imbalance * volume_ratio