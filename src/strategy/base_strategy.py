from event_bus.event import Event
from event_bus.event_types import EventType


class BaseStrategy:
    """Initialize a base strategy with an event bus for emitting trading events.
        This sets up core infrastructure for derived strategies to publish orders.

        Args:
            event_bus: An event bus instance used to publish trading-related events.
        """

    def __init__(self, event_bus):
        self.bus = event_bus

    # ===============================
    # EMIT SIGNAL
    # ===============================

    async def signal(self, symbol, side, amount):
        order = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "type": "market"
        }

        event = Event(EventType.ORDER, order)

        await self.bus.publish(event)
