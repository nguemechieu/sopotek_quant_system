class SmartOrderRouter:

    def __init__(self, event_bus, broker_manager):

        self.event_bus = event_bus
        self.broker_manager = broker_manager

        event_bus.subscribe("signal.approved", self.execute)

    async def execute(self, signal):

        symbol = signal["symbol"]
        side = signal["signal"]
        amount = signal["amount"]

        best_broker = None
        best_price = None

        for broker in self.broker_manager.brokers.values():

            ticker = await broker.fetch_ticker(symbol)

            price = ticker["last"]

            if best_price is None:
                best_price = price
                best_broker = broker

            elif side == "BUY" and price < best_price:
                best_price = price
                best_broker = broker

            elif side == "SELL" and price > best_price:
                best_price = price
                best_broker = broker

        order = await best_broker.create_order(
            symbol=symbol,
            side=side.lower(),
            order_type="market",
            amount=amount
        )

        await self.event_bus.publish(
            "order.executed",
            order
        )