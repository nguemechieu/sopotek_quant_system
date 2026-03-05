
from sopotek_trading.backend.engine.trading_engine import TradingEngine


class Orchestrator:

    def __init__(self, controller):

        self.controller = controller
        self.logger = controller.logger
        self.candles_buffer = controller.candles_buffer

        self.trading_engine = TradingEngine(controller=controller)

        self.running = False
        self.symbol_tasks = {}

    # ======================================================
    # START
    # ======================================================

    async def start(self):

        if self.running:
            self.logger.info("Orchestrator already running.")
            return

        self.running = True
        self.logger.info("Orchestrator started.")

        await self.trading_engine.start()





    # ======================================================
    # STOP
    # ======================================================

    async def shutdown(self):

        self.running = False

        for task in self.symbol_tasks.values():
            task.cancel()

        self.symbol_tasks.clear()

        await self.trading_engine.stop()

        self.logger.info("Orchestrator stopped.")