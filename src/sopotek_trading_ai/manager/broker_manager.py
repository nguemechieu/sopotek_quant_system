import asyncio


class BrokerManager:

    def __init__(self):

        # Asset class brokers
        self.crypto = None
        self.forex = None
        self.stocks = None
        self.paper = None

        # Registry
        self.brokers = {}

    # ======================================================
    # REGISTER METHODS
    # ======================================================

    def register_crypto(self, broker):

        self.crypto = broker
        self.brokers["crypto"] = broker

    def register_forex(self, broker):

        self.forex = broker
        self.brokers["forex"] = broker

    def register_stocks(self, broker):

        self.stocks = broker
        self.brokers["stocks"] = broker

    def register_paper(self, broker):

        self.paper = broker
        self.brokers["paper"] = broker

    # ======================================================
    # AUTO REGISTER FROM CONFIG
    # ======================================================

    def register(self, config: dict):

        """
        Example config:

        {
            "exchange_type": "crypto",
            "crypto": BinanceBroker(...)
        }

        or

        {
            "exchange_type": "forex",
            "forex": OandaBroker(...)
        }
        """

        if not config:
            raise RuntimeError("No broker config provided")

        exchange_type = config.get("exchange_type")

        if exchange_type == "crypto":

            broker = config.get("crypto")
            if broker is None:
                raise RuntimeError("Crypto  broker missing in config")

            self.register_crypto(broker)

        elif exchange_type == "forex":
            broker = config.get("forex")
            if broker is None:
                raise RuntimeError("Forex  broker missing in config")

            self.register_forex(broker)

        elif exchange_type == "stocks":
            broker = config.get("stocks")
            if broker is None:
                raise RuntimeError("Stocks  broker missing in config")

            self.register_stocks(broker)

        elif exchange_type == "paper":

            broker = config.get("paper")
            if broker is None:
                raise RuntimeError("Paper broker missing in config")
            self.register_paper(broker)

        else:
            raise RuntimeError(f"Unsupported exchange type: {exchange_type}")

    # ======================================================
    # BROKER ROUTING
    # ======================================================

    def get_broker(self, symbol: str):

        """
        Determines which broker to use based on symbol format
        """

        if not symbol:
            raise ValueError("Symbol cannot be empty")

        # Crypto (BTC/USDT)
        if "/" in symbol:
            if self.crypto:
                return self.crypto

        # Forex (EUR_USD)
        if "_" in symbol:
            if self.forex:
                return self.forex

        # Stocks (AAPL)
        if symbol.isalpha():
            if self.stocks:
                return self.stocks

        # Fallback
        if self.paper:
            return self.paper

        raise RuntimeError(f"No broker available for symbol {symbol}")

    # ======================================================
    # CONNECT ALL BROKERS
    # ======================================================

    async def connect_all(self):

        tasks = []

        for broker in self.brokers.values():

            if broker:
                tasks.append(broker.connect())

        if tasks:
            await asyncio.gather(*tasks)

    # ======================================================
    # CLOSE ALL BROKERS
    # ======================================================

    async def close_all(self):

        tasks = []

        for broker in self.brokers.values():

            if broker:
                tasks.append(broker.close())

        if tasks:
            await asyncio.gather(*tasks)