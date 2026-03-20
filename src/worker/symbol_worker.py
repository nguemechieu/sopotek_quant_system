# core/symbol_worker.py

import asyncio
import logging


class SymbolWorker:

    def __init__(
        self,
        symbol,
        broker,
        strategy,
        execution_manager,
        timeframe,
        limit,
        controller=None,
        startup_delay=0.0,
        poll_interval=2.0,
        signal_processor=None,
    ):
        self.logger=logging.getLogger("SymbolWorker")

        self.symbol = symbol
        self.broker = broker
        self.strategy = strategy
        self.execution_manager = execution_manager
        self.timeframe = timeframe
        self.limit = limit
        self.controller = controller
        self.running = True
        self.startup_delay = max(0.0, float(startup_delay))
        self.poll_interval = max(2.0, float(poll_interval))
        self.signal_processor = signal_processor

    async def _run_centralized_signal_pipeline(self):
        processor = self.signal_processor
        if processor is None and self.controller is not None:
            trading_system = getattr(self.controller, "trading_system", None)
            candidate = getattr(trading_system, "process_symbol", None)
            if callable(candidate):
                processor = candidate

        if not callable(processor):
            return False

        await processor(
            self.symbol,
            timeframe=self.timeframe,
            limit=self.limit,
            publish_debug=True,
        )
        return True


    async def run(self):
        if self.startup_delay > 0:
            await asyncio.sleep(self.startup_delay)

        while self.running:

            try:
                if self.controller and hasattr(self.controller, "is_symbol_enabled_for_autotrade"):
                    try:
                        enabled = self.controller.is_symbol_enabled_for_autotrade(self.symbol)
                    except Exception:
                        enabled = True
                    if not enabled:
                        await asyncio.sleep(self.poll_interval)
                        continue

                if await self._run_centralized_signal_pipeline():
                    await asyncio.sleep(self.poll_interval)
                    continue

                if self.controller and hasattr(self.controller, "_safe_fetch_ohlcv"):
                    candles = await self.controller._safe_fetch_ohlcv(
                        self.symbol,
                        timeframe=self.timeframe,
                        limit=self.limit,
                    )
                else:
                    candles = await self.broker.fetch_ohlcv(
                        self.symbol,
                        timeframe=self.timeframe,
                        limit=self.limit
                    )

                signal = self.strategy.generate_signal(candles)
                features = None
                if hasattr(self.strategy, "compute_features"):
                    try:
                        features = self.strategy.compute_features(candles)
                    except Exception:
                        features = None

                display_signal = signal or {
                    "side": "hold",
                    "amount": 0.0,
                    "confidence": 0.0,
                    "reason": "No entry signal on the latest scan.",
                }
                if self.controller and hasattr(self.controller, "publish_ai_signal"):
                    self.controller.publish_ai_signal(self.symbol, display_signal, candles=candles)
                if self.controller and hasattr(self.controller, "publish_strategy_debug"):
                    self.controller.publish_strategy_debug(
                        self.symbol,
                        display_signal,
                        candles=candles,
                        features=features,
                    )

                if signal:
                    trading_system = getattr(self.controller, "trading_system", None) if self.controller is not None else None
                    process_signal = getattr(trading_system, "process_signal", None)
                    if callable(process_signal):
                        try:
                            await process_signal(self.symbol, signal, timeframe=self.timeframe)
                        except TypeError:
                            await process_signal(self.symbol, signal)
                    else:
                        await self.execution_manager.execute(
                            symbol=self.symbol,
                            side=signal["side"],
                            amount=signal["amount"],
                            price=signal.get("price")
                        )

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                self.logger.error(f"Worker error {self.symbol}: {e}")
                retry_delay = self.poll_interval
                if "429" in str(e):
                    retry_delay = max(self.poll_interval, 20.0)
                await asyncio.sleep(retry_delay)
