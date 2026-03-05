# brokers/factory.py

from .ccxt_broker import CCXTBroker
from .oanda_broker import OandaBroker
from .alpaca_broker import AlpacaBroker
from .paper_broker import PaperBroker

BROKER_REGISTRY = {
    "crypto": CCXTBroker,
    "forex": OandaBroker,
    "stocks": AlpacaBroker,
    "paper":PaperBroker
}


class BrokerFactory:

    @staticmethod
    def create(controller):

        broker_type = getattr(controller, "type", None)

        if not broker_type:
            raise ValueError("Broker type not specified in controller")

        broker_class = BROKER_REGISTRY.get(broker_type)

        if broker_class is None:
            raise ValueError(f"Unsupported broker type: {broker_type}")




        # =====================================
        # CRYPTO (CCXT)
        # =====================================

        if broker_type == "crypto":

            exchange = getattr(controller, "exchange_name", None)
            api_key = getattr(controller, "api_key", None)
            secret = getattr(controller, "secret", None)

            if not exchange:
                raise ValueError("Missing exchange name for crypto broker")

            if not api_key or not secret:
                raise ValueError("Missing API credentials for crypto broker")



            return broker_class(controller)

        # =====================================
        # FOREX (OANDA)
        # =====================================

        if broker_type == "forex":

            api_key = getattr(controller, "api_key", None)
            account_id = getattr(controller, "account_id", None)

            if not api_key or not account_id:
                raise ValueError("Missing OANDA credentials")


            return broker_class(controller)

        # =====================================
        # STOCKS (ALPACA)
        # =====================================

        if broker_type == "stocks":




            return broker_class(controller)

        raise ValueError(f"Invalid broker type: {broker_type}")