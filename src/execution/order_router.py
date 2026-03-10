class OrderRouter:

    def __init__(self, broker):

        self.broker = broker

    async def route(self, order):

        symbol = order["symbol"]
        side = order["side"]
        amount = order["amount"]

        order_type = order.get("type", "market")
        params = dict(order.get("params") or {})
        stop_loss = order.get("stop_loss")
        take_profit = order.get("take_profit")

        if order_type == "market":

            execution = await self.broker.create_order(
                symbol=symbol,
                side=side,
                amount=amount,
                type="market",
                params=params,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )

        else:

            price = order.get("price")

            execution = await self.broker.create_order(
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                type="limit",
                params=params,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )

        return execution
