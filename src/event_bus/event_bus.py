import asyncio
import inspect
from collections import defaultdict

from event_bus.event import Event


class EventBus:
    SHUTDOWN_EVENT = "__event_bus_shutdown__"

    def __init__(self):
        self.queue = asyncio.Queue()
        self.subscribers = defaultdict(list)
        self._dispatcher_task = None
        self._running = False

    async def publish(self, event_or_type, data=None):
        if isinstance(event_or_type, Event) and data is None:
            event = event_or_type
        else:
            event = Event(event_or_type, data)
        await self.queue.put(event)
        return event

    def subscribe(self, event_type, handler):
        self.subscribers[event_type].append(handler)
        return handler

    @property
    def is_running(self):
        return bool(self._running and self._dispatcher_task is not None and not self._dispatcher_task.done())

    async def dispatch_once(self, event=None):
        event = event if event is not None else await self.queue.get()
        try:
            if getattr(event, "type", None) == self.SHUTDOWN_EVENT:
                self._running = False
                return event

            handlers = list(self.subscribers.get(getattr(event, "type", None), []) or [])
            tasks = []
            for handler in handlers:
                try:
                    result = handler(event)
                except Exception:
                    continue
                if inspect.isawaitable(result):
                    tasks.append(asyncio.create_task(result))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            return event
        finally:
            self.queue.task_done()

    async def start(self):
        self._running = True
        try:
            while self._running:
                event = await self.queue.get()
                await self.dispatch_once(event)
        finally:
            self._running = False
            self._dispatcher_task = None

    def run_in_background(self):
        if self.is_running:
            return self._dispatcher_task
        self._dispatcher_task = asyncio.create_task(self.start(), name="event_bus_dispatcher")
        return self._dispatcher_task

    async def shutdown(self):
        task = self._dispatcher_task if self._dispatcher_task is not None and not self._dispatcher_task.done() else None
        if task is None:
            self._running = False
            self._dispatcher_task = None
            return
        self._running = False
        await self.publish(self.SHUTDOWN_EVENT, {})
        await asyncio.gather(task, return_exceptions=True)
        self._dispatcher_task = None
