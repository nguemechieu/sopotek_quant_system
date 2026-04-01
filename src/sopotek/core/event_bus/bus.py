from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable

from sopotek.core.event_bus.event import Event
from sopotek.core.event_bus.store import EventStore

EventHandler = Callable[[Event], Any]


class AsyncEventBus:
    SHUTDOWN_EVENT = "__event_bus_shutdown__"
    ALL_EVENTS = "*"

    def __init__(
        self,
        *,
        store: EventStore | None = None,
        enable_persistence: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self.queue: asyncio.PriorityQueue[tuple[int, int, Event]] = asyncio.PriorityQueue()
        self.subscribers: dict[str, list[EventHandler]] = defaultdict(list)
        self._dispatcher_task: asyncio.Task[Any] | None = None
        self._running = False
        self._sequence = 0
        self._store = store
        self._enable_persistence = bool(enable_persistence and store is not None)
        self.logger = logger or logging.getLogger("SopotekEventBus")

    def subscribe(self, event_type: str, handler: EventHandler) -> EventHandler:
        self.subscribers[event_type].append(handler)
        return handler

    def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        handlers = self.subscribers.get(event_type)
        if not handlers:
            return
        self.subscribers[event_type] = [registered for registered in handlers if registered is not handler]

    @property
    def is_running(self) -> bool:
        return bool(self._running and self._dispatcher_task is not None and not self._dispatcher_task.done())

    async def publish(self, event_or_type: Event | str, data: Any = None, **kwargs: Any) -> Event:
        event = self._coerce_event(event_or_type, data=data, **kwargs)
        event.sequence = self._sequence
        self._sequence += 1
        if self._enable_persistence and self._store is not None:
            await self._store.append(event)
        await self.queue.put((int(event.priority), int(event.sequence), event))
        self.logger.debug(
            "Published event type=%s priority=%s sequence=%s",
            event.type,
            event.priority,
            event.sequence,
        )
        return event

    async def dispatch_once(self, event: Event | None = None) -> Event:
        came_from_queue = event is None
        if event is None:
            _, _, event = await self.queue.get()
        try:
            if getattr(event, "type", None) == self.SHUTDOWN_EVENT:
                self._running = False
                return event
            await self._deliver(event)
            return event
        finally:
            if came_from_queue:
                self.queue.task_done()

    async def _deliver(self, event: Event) -> None:
        handlers = list(self.subscribers.get(getattr(event, "type", None), []))
        handlers.extend(self.subscribers.get(self.ALL_EVENTS, []))
        if not handlers:
            return

        tasks: list[Awaitable[Any]] = []
        for handler in handlers:
            try:
                result = handler(event)
            except Exception:
                self.logger.exception("Event handler crashed for %s", event.type)
                continue
            if inspect.isawaitable(result):
                tasks.append(result)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _coerce_event(self, event_or_type: Event | str, data: Any = None, **kwargs: Any) -> Event:
        if isinstance(event_or_type, Event) and data is None:
            return event_or_type
        return Event(event_or_type, data, **kwargs)

    async def start(self) -> None:
        self._running = True
        try:
            while self._running:
                await self.dispatch_once()
        finally:
            self._running = False
            self._dispatcher_task = None

    def run_in_background(self) -> asyncio.Task[Any]:
        if self.is_running:
            return self._dispatcher_task  # type: ignore[return-value]
        self._dispatcher_task = asyncio.create_task(self.start(), name="sopotek_event_bus")
        return self._dispatcher_task

    async def shutdown(self) -> None:
        task = self._dispatcher_task if self._dispatcher_task is not None and not self._dispatcher_task.done() else None
        if task is None:
            self._running = False
            self._dispatcher_task = None
            return
        self._running = False
        await self.publish(self.SHUTDOWN_EVENT, {})
        await asyncio.gather(task, return_exceptions=True)
        self._dispatcher_task = None

    async def replay(
        self,
        *,
        event_types: list[str] | None = None,
        limit: int | None = None,
        handler: EventHandler | None = None,
    ) -> list[Event]:
        if self._store is None:
            return []
        replay_events = await self._store.read(event_types=event_types, limit=limit)
        delivered = []
        for stored_event in replay_events:
            replay_event = stored_event.copy(replayed=True)
            if handler is not None:
                result = handler(replay_event)
                if inspect.isawaitable(result):
                    await result
            else:
                await self._deliver(replay_event)
            delivered.append(replay_event)
        return delivered
