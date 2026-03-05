import asyncio
from collections import defaultdict


class EventBus:

    def __init__(self):
        self.subscribers = defaultdict(list)

    def subscribe(self, event_type, handler):
        self.subscribers[event_type].append(handler)

    async def publish(self, event_type, data):

        handlers = self.subscribers.get(event_type, [])

        tasks = []

        for handler in handlers:
            tasks.append(asyncio.create_task(handler(data)))

        if tasks:
            await asyncio.gather(*tasks)