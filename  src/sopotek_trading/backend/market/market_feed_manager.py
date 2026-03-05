import asyncio


class MarketFeedManager:

    def __init__(self):
        self.feeds = []

    def register(self, feed):
        self.feeds.append(feed)

    async def start(self):

        tasks = []

        for feed in self.feeds:
            tasks.append(asyncio.create_task(feed.start()))

        await asyncio.gather(*tasks)