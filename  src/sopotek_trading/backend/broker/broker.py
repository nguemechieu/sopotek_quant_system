import asyncio


class Broker:

    def __init__(self, adapter, logger=None, max_retries=5, controller=None):

        if adapter is None:
            raise ValueError("Broker adapter cannot be None")

        self.adapter = adapter
        self.logger = logger
        self.max_retries = max_retries
        self.orderbook_signal = getattr(controller, "orderbook_signal", None)

    async def _safe_call(self, func, *args, **kwargs):

        for attempt in range(self.max_retries):

            try:
                return await func(*args, **kwargs)

            except Exception as e:

                error_str = str(e)

                if "Timestamp" in error_str or "InvalidNonce" in error_str:
                    if hasattr(self.adapter, "resync_time"):
                        await self.adapter.resync_time()

                if self.logger:
                    self.logger.warning(
                        f"Broker error (attempt {attempt + 1}): {e}"
                    )

                if attempt == self.max_retries - 1:
                    raise

                await asyncio.sleep(2 ** attempt)

        return None