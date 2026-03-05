import asyncio
from typing import Any


class ExecutionManager:

    def __init__(self, controller):

        self.controller = controller
        self.task = None
        self.num_workers = 0
        self.tasks = None
        self.broker = controller.broker
        self.logger = controller.logger
        self.queue = asyncio.Queue()
        
        self.running = False
        self.worker_task = None

    # -------------------------------------------------
    # START
    # -------------------------------------------------

    def start(self):

     self.running = True

     for _ in range(self.num_workers):

        self.task = asyncio.create_task(self._worker())

        self.tasks.append(self.task)
     while not self.queue.empty():
      self.queue.get_nowait()
      self.queue.task_done()
    # -------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------

    async def execute_trade(
            self,

            symbol: str,
            side: str,
            amount: float,
            order_type: str,
            price: float,
            stop_loss: float = 0.0,
            take_profit: float = 0.0,
            slippage: float = 0.0
    ) -> Any:

        loop = asyncio.get_running_loop()
        future = loop.create_future()


        order_request = {

            "symbol": symbol,
            "side": side,
            "amount": amount,
            "order_type": order_type,
            "price": price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "slippage": slippage,
            "future": future,
        }

        await self.queue.put(order_request)

        return await future

    # -------------------------------------------------
    # WORKER
    # -------------------------------------------------

    async def _worker(self):

     while self.running:

        try:

            order = await self.queue.get()

        except asyncio.CancelledError:
            break

        try:

            await self._execute(order)

        except Exception as e:

            self.logger.error(f"Execution failure: {e}")

        finally:

            self.queue.task_done()
    # -------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------

    async def stop(self):

     self.running = False

     for task in self.tasks:
        task.cancel()

     await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _execute(self, order):

      try:

        symbol = order.get("symbol")
        side = order.get("side")
        amount = order.get("amount")
        price = order.get("price")
        order_type = order.get("order_type", "market")
        self.user_id = order.get("user_id", "system")

        if not symbol or not side or not amount:
            raise ValueError("Invalid order format")

        self.logger.info(
            f"Executing order | {symbol} {side} {amount} @ {price}"
        )

        result = await self.broker.create_order(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            order_type=order_type
        )

        if result:

            self.logger.info(f"Order executed: {result}")

            # emit event to GUI
            if hasattr(self.controller, "trade_signal"):

                self.controller.trade_signal.emit({
                    "symbol": symbol,
                    "side": side,
                    "price": price,
                    "size": amount
                })

        return result

      except Exception as e:

        self.logger.error(f"Execution error: {e}")
        raise