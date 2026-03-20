from execution.smart_execution import SmartExecution


class OrderRouter:

    def __init__(self, broker):

        self.broker = broker
        self.smart_execution = SmartExecution(broker)

    async def route(self, order):
        return await self.smart_execution.execute(order)
