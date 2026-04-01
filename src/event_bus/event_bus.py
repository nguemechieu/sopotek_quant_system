import asyncio
from typing import Any

from sopotek.core.event_bus.bus import AsyncEventBus


class _LegacyQueueView:
    """Expose event objects on `.queue` like the pre-v2 bus."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()

    async def put(self, item: Any) -> None:
        await self._queue.put(item)

    async def get(self) -> Any:
        return await self._queue.get()

    def empty(self) -> bool:
        return self._queue.empty()

    def qsize(self) -> int:
        return self._queue.qsize()

    def task_done(self) -> None:
        self._queue.task_done()


class EventBus(AsyncEventBus):
    """Backward-compatible import path for the upgraded async event bus."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._dispatch_queue = self.queue
        self.queue = _LegacyQueueView()

    async def publish(self, event_or_type, data=None, **kwargs):
        event = self._coerce_event(event_or_type, data=data, **kwargs)
        event.sequence = self._sequence
        self._sequence += 1
        if self._enable_persistence and self._store is not None:
            await self._store.append(event)
        await self._dispatch_queue.put((int(event.priority), int(event.sequence), event))
        await self.queue.put(event)
        self.logger.debug(
            "Published legacy event type=%s priority=%s sequence=%s",
            event.type,
            event.priority,
            event.sequence,
        )
        return event

    async def dispatch_once(self, event=None):
        came_from_queue = event is None
        if event is None:
            _, _, event = await self._dispatch_queue.get()
        try:
            if getattr(event, "type", None) == self.SHUTDOWN_EVENT:
                self._running = False
                return event
            await self._deliver(event)
            return event
        finally:
            if came_from_queue:
                self._dispatch_queue.task_done()


__all__ = ["EventBus"]
