SPOT_ONLY_EXCHANGES = {"binanceus"}

MARKET_VENUE_CHOICES = [
    ("Auto", "auto"),
    ("Spot", "spot"),
    ("Derivative", "derivative"),
    ("Options", "option"),
    ("OTC", "otc"),
]

VALID_MARKET_VENUES = {value for _label, value in MARKET_VENUE_CHOICES}


def normalize_market_venue(value, default="auto"):
    normalized = str(value or default).strip().lower() or default
    if normalized not in VALID_MARKET_VENUES:
        return default
    return normalized


def supported_market_venues_for_profile(broker_type=None, exchange=None):
    normalized_type = str(broker_type or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().lower()

    if normalized_exchange == "coinbase":
        return ["auto", "spot", "derivative"]

    if normalized_exchange in SPOT_ONLY_EXCHANGES:
        return ["auto", "spot"]

    if normalized_exchange == "stellar":
        return ["auto", "spot"]

    if normalized_type == "forex" or normalized_exchange == "oanda":
        return ["auto", "otc"]

    if normalized_type == "stocks" or normalized_exchange == "alpaca":
        return ["auto", "spot"]

    if normalized_type == "paper" or normalized_exchange == "paper":
        return ["auto", "spot", "derivative", "option", "otc"]

    if normalized_type == "crypto":
        return ["auto", "spot", "derivative", "option"]

    return ["auto", "spot"]
