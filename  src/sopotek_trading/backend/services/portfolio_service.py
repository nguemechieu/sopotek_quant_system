class PortfolioService:

    def __init__(self, event_bus, portfolio):

        self.event_bus = event_bus
        self.portfolio = portfolio

        event_bus.subscribe("order.executed", self.update)

    async def update(self, order):

        self.portfolio.update_position(
            symbol=order["symbol"],
            side=order["side"],
            quantity=order["amount"],
            price=order["price"]
        )