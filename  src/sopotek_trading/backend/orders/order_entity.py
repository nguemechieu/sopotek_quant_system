from datetime import datetime

from sopotek_trading.backend.models.order_states import OrderState


class ManagedOrder:

    def __init__(self, trade_id, symbol, side, amount):
        self.trade_id = trade_id
        self.symbol = symbol
        self.side = side
        self.amount = float(amount)

        self.exchange_order_id = None
        self.filled = 0.0
        self.avg_price = None

        self.state = OrderState.CREATED
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    def transition(self, new_state: OrderState):
        self.state = new_state
        self.updated_at = datetime.now()