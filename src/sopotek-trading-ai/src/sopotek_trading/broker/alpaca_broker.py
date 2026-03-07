import alpaca_trade_api as tradeapi

from sopotek_trading.broker.base_broker import BaseBroker


class AlpacaBroker(BaseBroker):

    def __init__(self, api_key, secret, paper=True):

        base_url = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"

        self.api = tradeapi.REST(
            api_key,
            secret,
            base_url,
            api_version="v2"
        )

    # =================================
    # CONNECT
    # =================================

    async def connect(self):

        account = self.api.get_account()

        print("Connected to Alpaca")
        print("Account status:", account.status)

    async def close(self):

        pass

    # =================================
    # MARKET DATA
    # =================================

    async def fetch_ticker(self, symbol):

        bar = self.api.get_latest_trade(symbol)

        return {
            "symbol": symbol,
            "price": bar.price
        }

    async def fetch_order_book(self, symbol, limit=10):

        quote = self.api.get_latest_quote(symbol)

        return {
            "bid": quote.bid_price,
            "ask": quote.ask_price
        }

    # =================================
    # ORDERS
    # =================================

    async def create_order(
            self,
            symbol,
            side,
            amount,
            type="market",
            price=None
    ):

        order = self.api.submit_order(
            symbol=symbol,
            qty=amount,
            side=side.lower(),
            type=type,
            time_in_force="gtc"
        )

        return order

    async def cancel_order(self, order_id, symbol):

        return self.api.cancel_order(order_id)

    # =================================
    # ACCOUNT
    # =================================

    async def fetch_balance(self):

        account = self.api.get_account()

        return {
            "equity": float(account.equity),
            "cash": float(account.cash)
        }