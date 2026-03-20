from broker.ccxt_broker import CCXTBroker
from broker.oanda_broker import OandaBroker
from broker.alpaca_broker import AlpacaBroker
from broker.paper_broker import PaperBroker
from broker.stellar_broker import StellarBroker
from config.config_validator import ConfigValidator


BROKER_REGISTRY = {
    "crypto": CCXTBroker,
    "forex": OandaBroker,
    "stocks": AlpacaBroker,
    "paper": PaperBroker
}

US_REGION_CODES = {"us", "usa", "united_states", "united states"}


def _normalized_customer_region(broker_cfg):
    if broker_cfg is None:
        return ""

    raw = getattr(broker_cfg, "customer_region", None)
    if raw is None:
        options = getattr(broker_cfg, "options", None) or {}
        raw = options.get("customer_region")

    return str(raw or "").strip().lower()


def _validate_exchange_jurisdiction(broker_cfg):
    if broker_cfg is None:
        return

    broker_type = str(getattr(broker_cfg, "type", "") or "").strip().lower()
    exchange = str(getattr(broker_cfg, "exchange", "") or "").strip().lower()
    region = _normalized_customer_region(broker_cfg)

    if broker_type != "crypto" or exchange not in {"binance", "binanceus"}:
        return

    is_us_customer = region in US_REGION_CODES
    if exchange == "binance" and is_us_customer:
        raise ValueError("Binance.com is not available for US customers. Use Binance US instead.")
    if exchange == "binanceus" and region and not is_us_customer:
        raise ValueError("Binance US is only available for US customers. Use Binance instead.")


class BrokerFactory:

    @staticmethod
    def create(config):

        # Validate configuration
        ConfigValidator.validate(config)

        broker_cfg = config.broker
        _validate_exchange_jurisdiction(broker_cfg)

        # Special handling for paper trading
        if broker_cfg.exchange == "paper":
            return PaperBroker(broker_cfg)
        if broker_cfg.exchange == "stellar":
            return StellarBroker(broker_cfg)

        broker_class = BROKER_REGISTRY.get(broker_cfg.type)

        if broker_class is None:
            raise ValueError(
                f"Unsupported broker type: {broker_cfg.type}"
            )

        # Create broker instance
        broker = broker_class(broker_cfg)

        return broker
