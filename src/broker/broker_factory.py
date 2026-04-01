from __future__ import annotations

import importlib

from config.config_validator import ConfigValidator


BROKER_REGISTRY = {
    "crypto": "broker.ccxt_broker:CCXTBroker",
    "forex": "broker.oanda_broker:OandaBroker",
    "stocks": "broker.alpaca_broker:AlpacaBroker",
    "options": "broker.tdameritrade_broker:TDAmeritradeBroker",
    "futures": "broker.ibkr_broker:IBKRBroker",
    "derivatives": "broker.ibkr_broker:IBKRBroker",
    "paper": "broker.paper_broker:PaperBroker",
}

EXCHANGE_REGISTRY = {
    "alpaca": "broker.alpaca_broker:AlpacaBroker",
    "amp": "broker.amp_broker:AMPFuturesBroker",
    "ampfutures": "broker.amp_broker:AMPFuturesBroker",
    "ib": "broker.ibkr_broker:IBKRBroker",
    "ibkr": "broker.ibkr_broker:IBKRBroker",
    "interactivebrokers": "broker.ibkr_broker:IBKRBroker",
    "interactive_brokers": "broker.ibkr_broker:IBKRBroker",
    "oanda": "broker.oanda_broker:OandaBroker",
    "paper": "broker.paper_broker:PaperBroker",
    "schwab": "broker.tdameritrade_broker:TDAmeritradeBroker",
    "stellar": "broker.stellar_broker:StellarBroker",
    "tdameritrade": "broker.tdameritrade_broker:TDAmeritradeBroker",
    "tradovate": "broker.tradovate_broker:TradovateBroker",
}

US_REGION_CODES = {"us", "usa", "united_states", "united states"}


def _load_broker_class(target: str):
    module_name, class_name = target.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


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
        ConfigValidator.validate(config)

        broker_cfg = config.broker
        _validate_exchange_jurisdiction(broker_cfg)

        normalized_exchange = str(getattr(broker_cfg, "exchange", "") or "").strip().lower()
        normalized_type = str(getattr(broker_cfg, "type", "") or "").strip().lower()

        target = EXCHANGE_REGISTRY.get(normalized_exchange) or BROKER_REGISTRY.get(normalized_type)
        if target is None:
            raise ValueError(
                f"Unsupported broker configuration: type={broker_cfg.type} exchange={broker_cfg.exchange}"
            )

        broker_class = _load_broker_class(target)
        return broker_class(broker_cfg)
