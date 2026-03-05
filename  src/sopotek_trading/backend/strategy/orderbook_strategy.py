from sopotek_trading.backend.analytics.orderbook_analyzer import OrderbookAnalyzer


class OrderbookStrategy:

    def __init__(self, imbalance_threshold=0.25, wall_threshold=20):

        self.analyzer = OrderbookAnalyzer()

        self.imbalance_threshold = imbalance_threshold
        self.wall_threshold = wall_threshold

    # ========================================
    # GENERATE SIGNAL
    # ========================================

    async def generate_signal(self, symbol, orderbook):

        bids = orderbook["bids"]
        asks = orderbook["asks"]

        if not bids or not asks:
            return None

        imbalance = self.analyzer.imbalance(bids, asks)
        spread = self.analyzer.spread(bids, asks)

        bid_walls = self.analyzer.detect_walls(
            bids, self.wall_threshold
        )

        ask_walls = self.analyzer.detect_walls(
            asks, self.wall_threshold
        )


        price = (bids[0][0] + asks[0][0]) / 2

        # ========================================
        # BUY SIGNAL
        # ========================================

        if imbalance > self.imbalance_threshold:

            return {
                "symbol": symbol,
                "signal": "BUY",
                "entry_price": price,
                "confidence": min(1.0, imbalance),
                "spread": spread
            }

        # ========================================
        # SELL SIGNAL
        # ========================================

        if imbalance < -self.imbalance_threshold:

            return {
                "symbol": symbol,
                "signal": "SELL",
                "entry_price": price,
                "confidence": min(1.0, abs(imbalance)),
                "spread": spread
            }

        return None