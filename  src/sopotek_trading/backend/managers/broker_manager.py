class BrokerManager:

    def __init__(self):

        self.crypto = None
        self.forex = None

    def register_crypto(self, broker):
        self.crypto = broker

    def register_forex(self, broker):
        self.forex = broker

    def get_broker(self, symbol):

        if "/" in symbol:
            return self.crypto

        if "_" in symbol:
            return self.forex

        raise ValueError("Unknown asset type")

    def register(self, config):
        pass

    async def connect_all(self):
        pass