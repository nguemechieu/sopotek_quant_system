import asyncio
import copy
from datetime import datetime, timezone
import logging
import re
import socket
import time

import aiohttp
import ccxt.async_support as ccxt

from broker.base_broker import BaseBroker
from broker.coinbase_credentials import normalize_coinbase_credentials
from broker.coinbase_jwt_auth import build_coinbase_rest_jwt, resolve_coinbase_rest_host, uses_coinbase_jwt_auth
from broker.market_venues import (
    SPOT_ONLY_EXCHANGES,
    normalize_market_venue,
    supported_market_venues_for_profile,
)


_COINBASE_EXCHANGE_CLASS_CACHE = {}


class _CoinbaseJWTAuthMixin:
    def sign(self, path, api=None, method="GET", params=None, headers=None, body=None):
        api = api or []
        params = params or {}
        version = api[0] if len(api) > 0 else None
        signed = len(api) > 1 and api[1] == "private"

        if not signed or not uses_coinbase_jwt_auth(getattr(self, "apiKey", None), getattr(self, "secret", None)):
            return super().sign(path, api=api, method=method, params=params, headers=headers, body=body)

        path_part = "api/v3" if version == "v3" else "v2"
        full_path = "/" + path_part + "/" + self.implode_params(path, params)
        query = self.omit(params, self.extract_params(path))
        request_path = full_path
        if method == "GET" and query:
            full_path += "?" + self.urlencode(query)

        url = self.urls["api"]["rest"] + full_path
        if method != "GET" and query:
            body = self.json(query)

        jwt_token = build_coinbase_rest_jwt(
            request_method=method,
            request_host=resolve_coinbase_rest_host(self.urls["api"]["rest"]),
            request_path=request_path,
            api_key=self.apiKey,
            api_secret=self.secret,
        )
        return {
            "url": url,
            "method": method,
            "body": body,
            "headers": {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
            },
        }

    async def create_order(self, symbol, type, side, amount, price=None, params=None):
        original_id = getattr(self, "id", None)
        try:
            return await super().create_order(symbol, type, side, amount, price, params)
        finally:
            if original_id is not None:
                self.id = original_id


def _coinbase_exchange_class(base_class):
    if not isinstance(base_class, type):
        return base_class
    cached = _COINBASE_EXCHANGE_CLASS_CACHE.get(base_class)
    if cached is not None:
        return cached

    derived = type(f"Sopotek{base_class.__name__}JWT", (_CoinbaseJWTAuthMixin, base_class), {})
    _COINBASE_EXCHANGE_CLASS_CACHE[base_class] = derived
    return derived


class CCXTBroker(BaseBroker):
    DEFAULT_TIMEOUT_MS = 30000
    COINBASE_MAX_OHLCV_CANDLES = 300
    GENERIC_MAX_OHLCV_CANDLES = 1000
    COINBASE_OHLCV_CACHE_SECONDS = 8.0
    COINBASE_OHLCV_SHORTFALL_CACHE_SECONDS = 20.0
    COINBASE_OHLCV_MAX_CONCURRENCY = 2
    SPOT_ACCOUNT_CACHE_SECONDS = 3.0
    COINBASE_SPOT_ACCOUNT_CACHE_SECONDS = 8.0
    COINBASE_OPEN_ORDERS_CACHE_SECONDS = 5.0
    SPOT_PRICE_CACHE_SECONDS = 15.0
    SPOT_DUST_THRESHOLD = 1e-10
    SPOT_CASH_EQUIVALENTS = {"USD", "USDC", "USDT", "BUSD", "EUR", "GBP"}
    SPOT_VALUE_QUOTE_PRIORITY = ("USD", "USDC", "USDT", "BUSD", "EUR", "GBP", "BTC", "ETH")
    CAPABILITY_MAP = {
        "fetch_ticker": "fetchTicker",
        "fetch_tickers": "fetchTickers",
        "fetch_order_book": "fetchOrderBook",
        "fetch_ohlcv": "fetchOHLCV",
        "fetch_trades": "fetchTrades",
        "fetch_my_trades": "fetchMyTrades",
        "fetch_markets": "fetchMarkets",
        "fetch_currencies": "fetchCurrencies",
        "fetch_status": "fetchStatus",
        "create_order": "createOrder",
        "cancel_order": "cancelOrder",
        "cancel_all_orders": "cancelAllOrders",
        "fetch_balance": "fetchBalance",
        "fetch_positions": "fetchPositions",
        "fetch_order": "fetchOrder",
        "fetch_orders": "fetchOrders",
        "fetch_open_orders": "fetchOpenOrders",
        "fetch_closed_orders": "fetchClosedOrders",
        "withdraw": "withdraw",
        "fetch_deposit_address": "fetchDepositAddress",
    }

    def __init__(self, config):
        super().__init__()

        self.logger = logging.getLogger("CCXTBroker")

        self.config = config
        self.exchange_name = getattr(config, "exchange", None)
        self.api_key = getattr(config, "api_key", None)
        self.secret = getattr(config, "secret", None)
        self.password = getattr(config, "password", None) or getattr(config, "passphrase", None)
        self.uid = getattr(config, "uid", None)
        self.account_id = getattr(config, "account_id", None)
        self.wallet = getattr(config, "wallet", None)
        self.mode = (getattr(config, "mode", "live") or "live").lower()
        self.sandbox = bool(getattr(config, "sandbox", False) or self.mode in {"paper", "sandbox", "testnet"})
        self.timeout = int(getattr(config, "timeout", self.DEFAULT_TIMEOUT_MS) or self.DEFAULT_TIMEOUT_MS)
        self.extra_options = dict(getattr(config, "options", None) or {})
        self.extra_params = dict(getattr(config, "params", None) or {})
        self.market_preference = normalize_market_venue(self.extra_options.get("market_type", "auto"))
        self.resolved_market_preference = self.market_preference

        self.exchange = None
        self.session = None
        self.symbols = []
        self._connected = False
        self._open_orders_snapshot_cache = {}
        self._spot_account_snapshot_cache = None
        self._spot_account_snapshot_cache_until = 0.0
        self._spot_account_snapshot_inflight = None
        self._ticker_price_cache = {}
        self._ticker_price_cache_until = {}
        self._ticker_price_inflight = {}
        self._ohlcv_cache = {}
        self._ohlcv_cache_until = {}
        self._ohlcv_inflight = {}
        self._coinbase_ohlcv_semaphore = None
        self._account_asset_codes = []
        self._market_symbol_lookup = set()

        if not self.exchange_name:
            raise ValueError("CCXT exchange name is required")

        self.logger.info("Initializing broker %s", self.exchange_name)

    @staticmethod
    def _normalized_credential(value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    # ==========================================================
    # INTERNALS
    # ==========================================================

    def _exchange_class(self):
        try:
            exchange_class = getattr(ccxt, self.exchange_name)
        except AttributeError as exc:
            raise ValueError(f"Unsupported CCXT exchange: {self.exchange_name}") from exc
        if self._exchange_code() == "coinbase":
            return _coinbase_exchange_class(exchange_class)
        return exchange_class

    def _build_exchange_options(self):
        options = {"adjustForTimeDifference": True}
        default_type = self._default_type_for_market_preference()
        if default_type:
            options.setdefault("defaultType", default_type)
        options.update(self.extra_options)
        if self._exchange_code() == "binanceus":
            options["defaultType"] = "spot"
            options.pop("defaultSubType", None)
            options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        return options

    def _exchange_code(self):
        return str(self.exchange_name or "").strip().lower()

    @staticmethod
    def _normalize_market_symbol(symbol):
        return str(symbol or "").strip().upper().replace("_", "/").replace("-", "/")

    def _refresh_market_symbol_lookup(self, markets=None):
        lookup = set()

        for symbol in self.symbols or []:
            normalized = self._normalize_market_symbol(symbol)
            if normalized:
                lookup.add(normalized)

        if isinstance(markets, dict):
            for market_symbol, market in markets.items():
                normalized_market_symbol = self._normalize_market_symbol(market_symbol)
                if normalized_market_symbol:
                    lookup.add(normalized_market_symbol)

                if isinstance(market, dict):
                    normalized_declared_symbol = self._normalize_market_symbol(market.get("symbol"))
                    if normalized_declared_symbol:
                        lookup.add(normalized_declared_symbol)

        self._market_symbol_lookup = lookup
        return lookup

    def _supports_attached_protection_prices(self):
        exchange_code = self._exchange_code()
        return exchange_code in set()

    def supports_symbol(self, symbol):
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not normalized_symbol:
            return False

        lookup = getattr(self, "_market_symbol_lookup", None)
        if not lookup:
            markets = getattr(self.exchange, "markets", {}) if self.exchange is not None else {}
            lookup = self._refresh_market_symbol_lookup(markets)

        return normalized_symbol in lookup

    def _normalize_credentials(self):
        self.exchange_name = self._normalized_credential(self.exchange_name)
        self.api_key = self._normalized_credential(self.api_key)
        self.secret = self._normalized_credential(self.secret)
        self.password = self._normalized_credential(self.password)
        self.uid = self._normalized_credential(self.uid)
        self.account_id = self._normalized_credential(self.account_id)
        self.wallet = self._normalized_credential(self.wallet)
        if self._exchange_code() == "coinbase" and (self.api_key or self.secret or self.password):
            self.api_key, self.secret, self.password = normalize_coinbase_credentials(
                self.api_key,
                self.secret,
                self.password,
            )

    @staticmethod
    def _strip_wrapped_quotes(value):
        text = str(value or "").strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            return text[1:-1].strip()
        return text

    @classmethod
    def _normalize_coinbase_api_key(cls, value):
        normalized = cls._strip_wrapped_quotes(value)
        return normalized or None

    @classmethod
    def _normalize_coinbase_secret(cls, value):
        secret = cls._strip_wrapped_quotes(value)
        if not secret:
            return None
        if "\\n" in secret:
            secret = secret.replace("\\r\\n", "\n").replace("\\n", "\n")

        if "-----BEGIN" in secret and "-----END" in secret:
            header_match = re.search(r"-----BEGIN [A-Z ]+-----", secret)
            footer_match = re.search(r"-----END [A-Z ]+-----", secret)
            if header_match and footer_match and header_match.start() < footer_match.start():
                header = header_match.group(0)
                footer = footer_match.group(0)
                middle = secret[header_match.end():footer_match.start()]
                if "\n" not in secret and "\r" not in secret:
                    middle = re.sub(r"\s+", "", middle)
                    secret = f"{header}\n{middle}\n{footer}\n" if middle else f"{header}\n{footer}\n"

        return secret

    def _validate_credentials(self):
        exchange_code = self._exchange_code()
        if exchange_code in {"binance", "binanceus"}:
            if self.api_key and any(ch.isspace() for ch in self.api_key):
                raise ValueError(
                    f"{exchange_code.upper()} API key contains whitespace. Paste the key exactly as issued by the exchange."
                )
            if self.secret and any(ch.isspace() for ch in self.secret):
                raise ValueError(
                    f"{exchange_code.upper()} API secret contains whitespace. Paste the secret exactly as issued by the exchange."
                )

    def _default_open_orders_symbol(self, symbol=None):
        if symbol:
            return symbol

        configured = (
            getattr(self.config, "symbol", None)
            or self.extra_options.get("open_orders_symbol")
            or self.extra_options.get("symbol")
        )
        if configured:
            return str(configured).strip() or None

        if len(self.symbols) == 1:
            return self.symbols[0]

        return None

    @staticmethod
    def _dedupe_orders_snapshot(orders):
        unique = []
        seen = set()
        for order in orders or []:
            if isinstance(order, dict):
                key = (
                    str(order.get("id") or ""),
                    str(order.get("clientOrderId") or ""),
                    str(order.get("symbol") or ""),
                    str(order.get("status") or ""),
                )
            else:
                key = (str(order), "", "", "")
            if key in seen:
                continue
            seen.add(key)
            unique.append(order)
        return unique

    def _monitored_symbols(self, symbols=None):
        normalized = []
        default_symbol = self._default_open_orders_symbol()
        if default_symbol:
            normalized.append(default_symbol)

        for symbol in symbols or self.symbols or []:
            candidate = str(symbol or "").strip()
            if candidate:
                normalized.append(candidate)

        return list(dict.fromkeys(normalized))

    async def _fetch_open_orders_without_symbol(self, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_open_orders", None, default=[], **kwargs)

    async def _fetch_open_orders_by_symbols(self, symbols, limit=None):
        snapshot = []
        for symbol in symbols or []:
            try:
                orders = await self.fetch_open_orders(symbol=symbol, limit=limit)
            except TypeError:
                orders = await self.fetch_open_orders(symbol)
            snapshot.extend(orders or [])
        return self._dedupe_orders_snapshot(snapshot)

    def _market_matches_preference(self, market):
        if self.market_preference == "auto":
            return True
        if not isinstance(market, dict):
            return False
        if self.market_preference == "spot":
            return bool(market.get("spot"))
        if self.market_preference == "derivative":
            return self._market_is_derivative(market)
        if self.market_preference == "option":
            return bool(market.get("option"))
        if self.market_preference == "otc":
            return self._market_is_otc(market)
        return True

    def _default_type_for_market_preference(self):
        if self.market_preference == "spot":
            return "spot"
        if self.market_preference == "option":
            return "option"
        if self.market_preference == "derivative":
            subtype = str(self.extra_options.get("defaultSubType", "") or "").strip().lower()
            if self._exchange_code() == "coinbase":
                return subtype if subtype in {"future", "swap"} else None
            return "future" if subtype == "future" else "swap"
        return None

    @staticmethod
    def _market_is_derivative(market):
        if not isinstance(market, dict):
            return False
        if bool(market.get("option")):
            return False
        return any(bool(market.get(key)) for key in ("contract", "swap", "future"))

    @staticmethod
    def _market_is_otc(market):
        if not isinstance(market, dict):
            return False
        if bool(market.get("otc")):
            return True
        for key in ("type", "marketType", "subType", "category"):
            if str(market.get(key) or "").strip().lower() == "otc":
                return True
        return False

    def _market_flag_summary(self, markets):
        summary = {
            "flags_detected": False,
            "spot": False,
            "derivative": False,
            "option": False,
            "otc": False,
        }
        if not isinstance(markets, dict):
            return summary

        for market in markets.values():
            if not isinstance(market, dict):
                continue
            if any(key in market for key in ("spot", "contract", "swap", "future", "option", "otc")):
                summary["flags_detected"] = True
            if bool(market.get("spot")):
                summary["spot"] = True
            if self._market_is_derivative(market):
                summary["derivative"] = True
            if bool(market.get("option")):
                summary["option"] = True
            if self._market_is_otc(market):
                summary["otc"] = True

        return summary

    def _supports_coinbase_positions_endpoint(self, markets):
        effective_preference = normalize_market_venue(getattr(self, "resolved_market_preference", None), default="auto")
        if effective_preference == "spot":
            return False
        if effective_preference == "option":
            return False

        if not isinstance(markets, dict) or not markets:
            return effective_preference == "derivative"

        summary = self._market_flag_summary(markets)
        if effective_preference == "derivative":
            return summary["derivative"]
        if summary["derivative"] and not summary["spot"]:
            return True
        return False

    def _filtered_symbols_from_markets(self, markets):
        if not isinstance(markets, dict):
            return []

        matched = []
        fallback = []
        for symbol, market in markets.items():
            candidate = str((market or {}).get("symbol") or symbol or "").strip()
            if not candidate:
                continue
            fallback.append(candidate)
            if self._market_matches_preference(market):
                matched.append(candidate)

        if self.market_preference == "auto" or matched:
            self.resolved_market_preference = self.market_preference if self.market_preference != "auto" else "auto"
            return sorted(dict.fromkeys(matched or fallback))

        self.resolved_market_preference = "auto"
        self.logger.warning(
            "Requested market preference %s was not available on %s; falling back to auto symbols",
            self.market_preference,
            self.exchange_name,
        )
        return sorted(dict.fromkeys(fallback))

    def _supports_positions_endpoint(self):
        exchange_code = self._exchange_code()
        if exchange_code in SPOT_ONLY_EXCHANGES:
            return False

        markets = getattr(self.exchange, "markets", None)
        if exchange_code == "coinbase":
            return self._supports_coinbase_positions_endpoint(markets)

        if not isinstance(markets, dict) or not markets:
            return True

        summary = self._market_flag_summary(markets)
        if summary["derivative"] or summary["option"]:
            return True

        if summary["flags_detected"]:
            return False

        return True

    def apply_market_preference(self, preference=None):
        if preference is not None:
            normalized = normalize_market_venue(preference)
            self.market_preference = normalized
            self.extra_options["market_type"] = normalized
        markets = getattr(self.exchange, "markets", {}) if self.exchange is not None else {}
        self.symbols = self._filtered_symbols_from_markets(markets)
        self._refresh_market_symbol_lookup(markets)
        return list(self.symbols)

    def supported_market_venues(self):
        exchange_code = self._exchange_code()
        if exchange_code in SPOT_ONLY_EXCHANGES:
            return ["auto", "spot"]

        profile_venues = supported_market_venues_for_profile("crypto", exchange_code)
        markets = getattr(getattr(self, "exchange", None), "markets", None)
        if isinstance(markets, dict) and markets:
            venues = ["auto"]
            if any(bool((market or {}).get("spot")) for market in markets.values()):
                venues.append("spot")
            if any(self._market_is_derivative(market) for market in markets.values()):
                venues.append("derivative")
            if any(bool((market or {}).get("option")) for market in markets.values()):
                venues.append("option")
            if any(self._market_is_otc(market) for market in markets.values()):
                venues.append("otc")
            if exchange_code == "coinbase":
                venues = [venue for venue in venues if venue != "option"]
                return list(dict.fromkeys(profile_venues + venues))
            if len(venues) > 1:
                return list(dict.fromkeys(venues))

        return profile_venues

    def _build_exchange_config(self):
        cfg = {
            "enableRateLimit": True,
            "timeout": self.timeout,
            "options": self._build_exchange_options(),
        }

        if self.session is not None:
            cfg["session"] = self.session

        if self.api_key:
            cfg["apiKey"] = self.api_key
        if self.secret:
            cfg["secret"] = self.secret
        if self.password:
            cfg["password"] = self.password
        if self.uid:
            cfg["uid"] = self.uid
        if self.wallet:
            cfg["walletAddress"] = self.wallet

        if self.exchange_name.startswith("binance"):
            cfg["recvWindow"] = int(self.extra_options.get("recvWindow", 10000))

        return cfg

    def _timeframe_seconds(self, timeframe):
        if self.exchange is not None:
            parser = getattr(self.exchange, "parse_timeframe", None)
            if callable(parser):
                try:
                    return int(parser(timeframe))
                except Exception:
                    pass

        fallback = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "2h": 7200,
            "4h": 14400,
            "6h": 21600,
            "1d": 86400,
        }
        return int(fallback.get(str(timeframe or "").strip().lower(), 3600))

    def _coinbase_ohlcv_limiter(self):
        limiter = getattr(self, "_coinbase_ohlcv_semaphore", None)
        if limiter is None:
            limiter = asyncio.Semaphore(self.COINBASE_OHLCV_MAX_CONCURRENCY)
            self._coinbase_ohlcv_semaphore = limiter
        return limiter

    @staticmethod
    def _ticker_mid_price(ticker):
        if not isinstance(ticker, dict):
            return None

        bid = ticker.get("bid")
        ask = ticker.get("ask")
        try:
            bid_value = float(bid) if bid not in (None, "") else None
        except Exception:
            bid_value = None
        try:
            ask_value = float(ask) if ask not in (None, "") else None
        except Exception:
            ask_value = None
        if bid_value is not None and ask_value is not None and bid_value > 0 and ask_value > 0:
            return (bid_value + ask_value) / 2.0

        for key in ("last", "close", "price", "mark", "index"):
            value = ticker.get(key)
            try:
                numeric = float(value) if value not in (None, "") else None
            except Exception:
                numeric = None
            if numeric is not None and numeric > 0:
                return numeric
        return None

    async def _fetch_raw_balance(self):
        return await self._call_unified("fetch_balance")

    def _spot_balance_totals(self, balance):
        if not isinstance(balance, dict):
            return {}

        source = {}
        total_bucket = balance.get("total")
        if isinstance(total_bucket, dict) and total_bucket:
            source = dict(total_bucket)
        else:
            for bucket_name in ("free", "used"):
                bucket = balance.get(bucket_name)
                if not isinstance(bucket, dict):
                    continue
                for asset, value in bucket.items():
                    normalized_asset = str(asset or "").upper().strip()
                    if not normalized_asset:
                        continue
                    try:
                        source[normalized_asset] = float(source.get(normalized_asset, 0.0) or 0.0) + float(value or 0.0)
                    except Exception:
                        continue

        if not source:
            skip = {
                "free",
                "used",
                "total",
                "info",
                "raw",
                "equity",
                "cash",
                "balance",
                "account_value",
                "total_account_value",
                "net_liquidation",
                "position_value",
                "positions_value",
            }
            for asset, value in balance.items():
                if asset in skip:
                    continue
                normalized_asset = str(asset or "").upper().strip()
                if not normalized_asset:
                    continue
                try:
                    source[normalized_asset] = float(value or 0.0)
                except Exception:
                    continue

        cleaned = {}
        for asset, value in source.items():
            try:
                numeric = float(value or 0.0)
            except Exception:
                continue
            if abs(numeric) <= self.SPOT_DUST_THRESHOLD:
                continue
            cleaned[str(asset).upper()] = numeric
        return cleaned

    @staticmethod
    def _market_base_quote(symbol, market=None):
        if isinstance(market, dict):
            base = str(market.get("base") or "").upper().strip()
            quote = str(market.get("quote") or "").upper().strip()
            if base and quote:
                return base, quote

        normalized = str((market or {}).get("symbol") if isinstance(market, dict) else symbol or "").upper().strip()
        normalized = normalized.replace("_", "/").replace("-", "/")
        if "/" not in normalized:
            return "", ""
        base, quote = normalized.split("/", 1)
        return base.strip(), quote.strip()

    def _spot_symbol_candidates(self, base_asset, quote_candidates=None):
        normalized_base = str(base_asset or "").upper().strip()
        if not normalized_base:
            return []

        preferred_quotes = [str(code or "").upper().strip() for code in (quote_candidates or self.SPOT_VALUE_QUOTE_PRIORITY)]
        quote_rank = {quote: index for index, quote in enumerate(preferred_quotes)}
        markets = getattr(self.exchange, "markets", None)
        if not isinstance(markets, dict) or not markets:
            return []

        ranked = []
        seen = set()
        for market_symbol, market in markets.items():
            if isinstance(market, dict) and (self._market_is_derivative(market) or bool(market.get("option"))):
                continue

            base, quote = self._market_base_quote(market_symbol, market)
            if base != normalized_base or not quote:
                continue
            if preferred_quotes and quote not in quote_rank:
                continue

            symbol_text = str((market or {}).get("symbol") if isinstance(market, dict) else market_symbol or "").upper().strip()
            if not symbol_text or symbol_text in seen:
                continue
            seen.add(symbol_text)
            ranked.append((quote_rank.get(quote, len(preferred_quotes)), symbol_text, quote))

        ranked.sort(key=lambda item: (item[0], item[1]))
        return [(symbol, quote) for _, symbol, quote in ranked]

    def _spot_inverse_symbol_candidates(self, quote_asset, base_candidates=None):
        normalized_quote = str(quote_asset or "").upper().strip()
        if not normalized_quote:
            return []

        preferred_bases = [str(code or "").upper().strip() for code in (base_candidates or self.SPOT_VALUE_QUOTE_PRIORITY)]
        base_rank = {base: index for index, base in enumerate(preferred_bases)}
        markets = getattr(self.exchange, "markets", None)
        if not isinstance(markets, dict) or not markets:
            return []

        ranked = []
        seen = set()
        for market_symbol, market in markets.items():
            if isinstance(market, dict) and (self._market_is_derivative(market) or bool(market.get("option"))):
                continue

            base, quote = self._market_base_quote(market_symbol, market)
            if quote != normalized_quote or not base:
                continue
            if preferred_bases and base not in base_rank:
                continue

            symbol_text = str((market or {}).get("symbol") if isinstance(market, dict) else market_symbol or "").upper().strip()
            if not symbol_text or symbol_text in seen:
                continue
            seen.add(symbol_text)
            ranked.append((base_rank.get(base, len(preferred_bases)), symbol_text, base))

        ranked.sort(key=lambda item: (item[0], item[1]))
        return [(symbol, base) for _, symbol, base in ranked]

    async def _spot_symbol_mid_price(self, symbol):
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not normalized_symbol:
            return None
        if not self.supports_symbol(normalized_symbol):
            return None

        now = time.monotonic()
        cached_price = self._ticker_price_cache.get(normalized_symbol)
        if cached_price is not None and now < float(self._ticker_price_cache_until.get(normalized_symbol, 0.0) or 0.0):
            return float(cached_price)

        inflight = self._ticker_price_inflight.get(normalized_symbol)
        if inflight is not None and not inflight.done():
            return await inflight

        async def runner():
            ticker = await self._call_unified("fetch_ticker", normalized_symbol, default=None)
            price = self._ticker_mid_price(ticker)
            if price is not None and price > 0:
                self._ticker_price_cache[normalized_symbol] = float(price)
                self._ticker_price_cache_until[normalized_symbol] = time.monotonic() + self.SPOT_PRICE_CACHE_SECONDS
                return float(price)
            return None

        task = asyncio.create_task(runner())
        self._ticker_price_inflight[normalized_symbol] = task
        try:
            return await task
        finally:
            current = self._ticker_price_inflight.get(normalized_symbol)
            if current is task:
                self._ticker_price_inflight.pop(normalized_symbol, None)

    async def _spot_asset_valuation_usd(self, asset_code, visited=None):
        normalized_asset = str(asset_code or "").upper().strip()
        if not normalized_asset:
            return (None, "")
        if normalized_asset in {"USD", "USDC", "USDT", "BUSD"}:
            return (1.0, normalized_asset)

        visited_assets = set(visited or set())
        if normalized_asset in visited_assets:
            return (None, "")
        visited_assets.add(normalized_asset)

        for symbol, quote in self._spot_symbol_candidates(normalized_asset):
            price = await self._spot_symbol_mid_price(symbol)
            if price is None or price <= 0:
                continue
            if quote in {"USD", "USDC", "USDT", "BUSD"}:
                return (float(price), symbol)

            quote_price, _quote_symbol = await self._spot_asset_valuation_usd(quote, visited=visited_assets)
            if quote_price is None or quote_price <= 0:
                continue
            return (float(price) * float(quote_price), symbol)

        for symbol, base in self._spot_inverse_symbol_candidates(normalized_asset):
            price = await self._spot_symbol_mid_price(symbol)
            if price is None or price <= 0:
                continue
            if base in {"USD", "USDC", "USDT", "BUSD"}:
                return (1.0 / float(price), symbol)

            base_price, _base_symbol = await self._spot_asset_valuation_usd(base, visited=visited_assets)
            if base_price is None or base_price <= 0:
                continue
            return (float(base_price) / float(price), symbol)

        return (None, "")

    async def _spot_account_snapshot_from_balance(self, raw_balance):
        totals = self._spot_balance_totals(raw_balance)
        self._account_asset_codes = [asset for asset in sorted(totals) if asset]
        positions = []
        cash_value = 0.0

        for asset_code, amount in sorted(totals.items()):
            try:
                amount_value = float(amount or 0.0)
            except Exception:
                continue
            if amount_value <= self.SPOT_DUST_THRESHOLD:
                continue

            price_usd, reference_symbol = await self._spot_asset_valuation_usd(asset_code)
            if price_usd is None or price_usd <= 0:
                if asset_code in {"USD", "USDC", "USDT", "BUSD"}:
                    cash_value += amount_value
                continue

            if asset_code in self.SPOT_CASH_EQUIVALENTS:
                cash_value += amount_value * float(price_usd)
                continue

            positions.append(
                {
                    "symbol": reference_symbol or f"{asset_code}/USD",
                    "asset_code": asset_code,
                    "side": "long",
                    "position_side": "long",
                    "amount": amount_value,
                    "units": amount_value,
                    "entry_price": float(price_usd),
                    "mark_price": float(price_usd),
                    "market_price": float(price_usd),
                    "value": amount_value * float(price_usd),
                    "pnl": 0.0,
                    "realized_pnl": 0.0,
                    "financing": 0.0,
                    "margin_used": 0.0,
                    "resettable_pl": 0.0,
                    "quote_currency": "USD",
                    "position_key": asset_code,
                }
            )

        return {
            "positions": positions,
            "cash_value": float(cash_value),
        }

    @staticmethod
    def _spot_position_matches_symbols(position, symbols):
        if not symbols:
            return True

        requested_symbols = set()
        requested_assets = set()
        for symbol in symbols or []:
            normalized = str(symbol or "").upper().strip()
            if not normalized:
                continue
            compact = normalized.replace("_", "/").replace("-", "/")
            requested_symbols.add(compact)
            requested_assets.add(compact.split("/", 1)[0] if "/" in compact else compact)

        position_symbol = str(position.get("symbol") or "").upper().strip().replace("_", "/").replace("-", "/")
        asset_code = str(position.get("asset_code") or "").upper().strip()
        return position_symbol in requested_symbols or asset_code in requested_assets

    async def _augment_spot_balance_snapshot(self, raw_balance):
        if not isinstance(raw_balance, dict):
            return raw_balance

        snapshot = await self._spot_account_snapshot_from_balance(raw_balance)
        return self._compose_spot_balance_snapshot(raw_balance, snapshot)

    def _compose_spot_balance_snapshot(self, raw_balance, snapshot):
        normalized = dict(raw_balance)
        totals = self._spot_balance_totals(raw_balance)
        if totals:
            normalized.setdefault("asset_balances", dict(totals))
        cash_value = float(snapshot.get("cash_value", 0.0) or 0.0)
        position_value = sum(
            float((position or {}).get("value", 0.0) or 0.0)
            for position in snapshot.get("positions", [])
            if isinstance(position, dict)
        )
        account_value = cash_value + position_value

        if cash_value > 0:
            normalized.setdefault("cash", cash_value)
        if position_value > 0:
            normalized.setdefault("position_value", position_value)
            normalized.setdefault("positions_value", position_value)
        if account_value > 0:
            normalized.setdefault("equity", account_value)
            normalized.setdefault("account_value", account_value)
            normalized.setdefault("total_account_value", account_value)
            normalized.setdefault("net_liquidation", account_value)
        return normalized

    def _spot_account_cache_seconds(self):
        if self._exchange_code() == "coinbase":
            return self.COINBASE_SPOT_ACCOUNT_CACHE_SECONDS
        return self.SPOT_ACCOUNT_CACHE_SECONDS

    def _invalidate_account_state_cache(self):
        self._spot_account_snapshot_cache = None
        self._spot_account_snapshot_cache_until = 0.0
        self._open_orders_snapshot_cache.clear()

    async def _get_spot_account_snapshot_cached(self):
        now = time.monotonic()
        cached_snapshot = self._spot_account_snapshot_cache
        if cached_snapshot is not None and now < float(self._spot_account_snapshot_cache_until or 0.0):
            return copy.deepcopy(cached_snapshot)

        inflight = self._spot_account_snapshot_inflight
        if inflight is not None and not inflight.done():
            return copy.deepcopy(await inflight)

        async def runner():
            raw_balance = await self._fetch_raw_balance()
            if not isinstance(raw_balance, dict):
                payload = {
                    "raw_balance": raw_balance,
                    "balance": raw_balance,
                    "positions": [],
                    "cash_value": 0.0,
                }
            else:
                snapshot = await self._spot_account_snapshot_from_balance(raw_balance)
                payload = {
                    "raw_balance": raw_balance,
                    "balance": self._compose_spot_balance_snapshot(raw_balance, snapshot),
                    "positions": list(snapshot.get("positions", [])),
                    "cash_value": float(snapshot.get("cash_value", 0.0) or 0.0),
                }

            self._spot_account_snapshot_cache = copy.deepcopy(payload)
            self._spot_account_snapshot_cache_until = time.monotonic() + self._spot_account_cache_seconds()
            return copy.deepcopy(payload)

        task = asyncio.create_task(runner())
        self._spot_account_snapshot_inflight = task
        try:
            return await task
        finally:
            current = self._spot_account_snapshot_inflight
            if current is task:
                self._spot_account_snapshot_inflight = None

    def _coinbase_ohlcv_cache_key(self, symbol, timeframe, limit):
        return f"{str(symbol or '').upper().strip()}|{str(timeframe or '1h').lower().strip()}|{max(int(limit or 0), 1)}"

    def _normalize_ohlcv_boundary_ms(self, value, *, end_of_day=False):
        if value is None:
            return None

        if isinstance(value, datetime):
            timestamp = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            if end_of_day:
                timestamp = timestamp.replace(hour=23, minute=59, second=59, microsecond=999999)
            return int(timestamp.timestamp() * 1000)

        try:
            numeric = float(value)
            if abs(numeric) > 1e11:
                return int(numeric)
            return int(numeric * 1000)
        except Exception:
            pass

        text_value = str(value or "").strip()
        if not text_value:
            return None
        if "T" not in text_value and len(text_value) <= 10:
            text_value = (
                f"{text_value}T23:59:59.999999+00:00"
                if end_of_day
                else f"{text_value}T00:00:00+00:00"
            )
        if text_value.endswith("Z"):
            text_value = text_value[:-1] + "+00:00"
        try:
            timestamp = datetime.fromisoformat(text_value)
        except ValueError:
            return None
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        return int(timestamp.timestamp() * 1000)

    async def _fetch_coinbase_ohlcv_cached(self, symbol, timeframe="1h", limit=100):
        cache_key = self._coinbase_ohlcv_cache_key(symbol, timeframe, limit)
        now = time.monotonic()
        cached_rows = self._ohlcv_cache.get(cache_key)
        if isinstance(cached_rows, list) and now < float(self._ohlcv_cache_until.get(cache_key, 0.0) or 0.0):
            return [list(row) if isinstance(row, (list, tuple)) else row for row in cached_rows]

        inflight = self._ohlcv_inflight.get(cache_key)
        if inflight is not None and not inflight.done():
            return await inflight

        async def runner():
            self.logger.debug("Fetching OHLCV for %s", symbol)
            async with self._coinbase_ohlcv_limiter():
                data = await self._fetch_coinbase_ohlcv(symbol, timeframe=timeframe, limit=limit)
            rows = [list(row) if isinstance(row, (list, tuple)) else row for row in (data or [])]
            ttl_seconds = (
                self.COINBASE_OHLCV_SHORTFALL_CACHE_SECONDS
                if len(rows) < max(int(limit or 0), 1)
                else self.COINBASE_OHLCV_CACHE_SECONDS
            )
            self._ohlcv_cache[cache_key] = rows
            self._ohlcv_cache_until[cache_key] = time.monotonic() + ttl_seconds
            return [list(row) if isinstance(row, (list, tuple)) else row for row in rows]

        task = asyncio.create_task(runner())
        self._ohlcv_inflight[cache_key] = task
        try:
            return await task
        finally:
            current = self._ohlcv_inflight.get(cache_key)
            if current is task:
                self._ohlcv_inflight.pop(cache_key, None)

    async def _ensure_connected(self):
        if not self._connected:
            await self.connect()

    def _exchange_has(self, capability):
        exchange = self.exchange
        if exchange is None:
            return False

        if capability == "fetch_positions" and not self._supports_positions_endpoint():
            return False

        has_key = self.CAPABILITY_MAP.get(capability, capability)
        has_map = getattr(exchange, "has", None)
        if isinstance(has_map, dict):
            supported = has_map.get(has_key)
            if supported in (True, "emulated"):
                return True
            if supported is False:
                return False

        return callable(getattr(exchange, capability, None))

    def _maybe_precision_amount(self, symbol, amount):
        if self.exchange is None or amount is None:
            return amount

        converter = getattr(self.exchange, "amount_to_precision", None)
        if callable(converter):
            try:
                return float(converter(symbol, amount))
            except Exception:
                return amount
        return amount

    def _maybe_precision_price(self, symbol, price):
        if self.exchange is None or price is None:
            return price

        converter = getattr(self.exchange, "price_to_precision", None)
        if callable(converter):
            try:
                return float(converter(symbol, price))
            except Exception:
                return price
        return price

    async def _call_unified(self, method_name, *args, default=None, **kwargs):
        await self._ensure_connected()

        method = getattr(self.exchange, method_name, None)
        if not callable(method):
            if default is not None:
                return default
            raise NotImplementedError(
                f"{self.exchange_name} does not expose {method_name}"
            )

        if not self._exchange_has(method_name):
            if default is not None:
                return default
            raise NotImplementedError(
                f"{self.exchange_name} does not support {method_name}"
            )

        return await method(*args, **kwargs)

    # ==========================================================
    # CONNECT
    # ==========================================================

    async def connect(self):
        if self._connected:
            return

        self._normalize_credentials()
        self._validate_credentials()

        exchange_class = self._exchange_class()

        resolver = aiohttp.ThreadedResolver()
        connector = aiohttp.TCPConnector(
            resolver=resolver,
            family=socket.AF_INET,
            ttl_dns_cache=300,
        )
        self.session = aiohttp.ClientSession(connector=connector)
        self.exchange = exchange_class(self._build_exchange_config())

        try:
            if hasattr(self.exchange, "set_sandbox_mode"):
                self.exchange.set_sandbox_mode(self.sandbox)

            if callable(getattr(self.exchange, "load_time_difference", None)):
                await self.exchange.load_time_difference()

            await self.exchange.load_markets()
            markets = getattr(self.exchange, "markets", {}) or {}
            self.symbols = self._filtered_symbols_from_markets(markets)
            self._refresh_market_symbol_lookup(markets)
            self._connected = True
        except Exception:
            await self.close()
            raise

    async def close(self):
        errors = []

        if self.exchange is not None:
            try:
                await self.exchange.close()
            except Exception as exc:
                errors.append(exc)

        if self.session is not None:
            try:
                await self.session.close()
            except Exception as exc:
                errors.append(exc)

        self.exchange = None
        self.session = None
        self.symbols = []
        self._market_symbol_lookup = set()
        self._connected = False

        if errors:
            self.logger.warning("Broker close encountered %s issue(s)", len(errors))

    # ==========================================================
    # DISCOVERY
    # ==========================================================

    async def fetch_symbol(self):
        await self._ensure_connected()
        return list(self.symbols)

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    async def fetch_markets(self):
        await self._ensure_connected()
        markets = getattr(self.exchange, "markets", None)
        if isinstance(markets, dict) and markets:
            return markets
        return await self._call_unified("fetch_markets", default={})

    async def fetch_currencies(self):
        await self._ensure_connected()
        currencies = getattr(self.exchange, "currencies", None)
        if isinstance(currencies, dict) and currencies:
            return currencies
        return await self._call_unified("fetch_currencies", default={})

    async def fetch_status(self):
        if not self._connected:
            return {"status": "disconnected"}

        if self._exchange_has("fetchStatus"):
            return await self._call_unified("fetch_status")

        return {"status": "ok", "exchange": self.exchange_name}

    # ==========================================================
    # MARKET DATA
    # ==========================================================

    async def fetch_ticker(self, symbol):
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not self.supports_symbol(normalized_symbol):
            return None
        return await self._call_unified("fetch_ticker", normalized_symbol, default=None)

    async def fetch_tickers(self, symbols=None):
        return await self._call_unified("fetch_tickers", symbols, default={})

    async def fetch_orderbook(self, symbol, limit=100):
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not self.supports_symbol(normalized_symbol):
            return {"bids": [], "asks": []}
        return await self._call_unified(
            "fetch_order_book",
            normalized_symbol,
            limit,
            default={"bids": [], "asks": []},
        )

    async def fetch_order_book(self, symbol, limit=100):
        return await self.fetch_orderbook(symbol, limit=limit)

    async def _fetch_coinbase_ohlcv(self, symbol, timeframe="1h", limit=100, start_time=None, end_time=None):
        await self._ensure_connected()

        # Respect the user-requested limit while capping at Coinbase's maximum.
        requested_limit = max(min(int(limit or self.COINBASE_MAX_OHLCV_CANDLES), self.COINBASE_MAX_OHLCV_CANDLES), 1)
        timeframe_seconds = max(self._timeframe_seconds(timeframe), 1)
        remaining = requested_limit
        end_ms = self._normalize_ohlcv_boundary_ms(end_time, end_of_day=True)
        start_ms = self._normalize_ohlcv_boundary_ms(start_time, end_of_day=False)
        end_seconds = int((end_ms / 1000.0) if end_ms is not None else time.time())
        start_seconds_boundary = int(start_ms / 1000.0) if start_ms is not None else 0
        seen_timestamps = set()
        candles = []

        while remaining > 0 and end_seconds >= start_seconds_boundary:
            batch_size = min(remaining, self.COINBASE_MAX_OHLCV_CANDLES)
            start_seconds = max(end_seconds - (batch_size * timeframe_seconds), start_seconds_boundary, 0)
            params = {
                "start": str(start_seconds),
                "end": str(end_seconds),
            }
            batch = await self._call_unified(
                "fetch_ohlcv",
                symbol,
                timeframe=timeframe,
                since=start_seconds * 1000,
                limit=batch_size,
                params=params,
                default=[],
            )
            if not batch:
                break

            new_rows = 0
            for candle in batch:
                if not isinstance(candle, (list, tuple)) or not candle:
                    continue
                timestamp = candle[0]
                if start_ms is not None and int(timestamp) < int(start_ms):
                    continue
                if end_ms is not None and int(timestamp) > int(end_ms):
                    continue
                if timestamp in seen_timestamps:
                    continue
                seen_timestamps.add(timestamp)
                candles.append(list(candle))
                new_rows += 1

            candles.sort(key=lambda row: row[0])
            if len(candles) >= requested_limit:
                break

            earliest_timestamp = batch[0][0] if isinstance(batch[0], (list, tuple)) and batch[0] else None
            if earliest_timestamp is None or new_rows == 0 or len(batch) < batch_size:
                break
            if start_ms is not None and int(earliest_timestamp) <= int(start_ms):
                break

            next_end_seconds = int(earliest_timestamp / 1000) - timeframe_seconds
            if next_end_seconds <= 0 or next_end_seconds >= end_seconds:
                break

            end_seconds = next_end_seconds
            remaining = requested_limit - len(candles)

        return candles[-requested_limit:]

    async def _fetch_generic_ohlcv_range(self, symbol, timeframe="1h", limit=100, start_time=None, end_time=None):
        await self._ensure_connected()

        # Respect the user-requested limit while capping at the generic maximum.
        requested_limit = max(min(int(limit or self.GENERIC_MAX_OHLCV_CANDLES), self.GENERIC_MAX_OHLCV_CANDLES), 1)
        timeframe_seconds = max(self._timeframe_seconds(timeframe), 1)
        step_ms = timeframe_seconds * 1000
        start_ms = self._normalize_ohlcv_boundary_ms(start_time, end_of_day=False)
        end_ms = self._normalize_ohlcv_boundary_ms(end_time, end_of_day=True)
        if end_ms is None and start_ms is not None:
            end_ms = start_ms + (requested_limit * step_ms)
        if start_ms is None and end_ms is not None:
            start_ms = max(0, end_ms - (requested_limit * step_ms))
        if start_ms is None:
            start_ms = max(0, int(time.time() * 1000) - (requested_limit * step_ms))
        if end_ms is None:
            end_ms = start_ms + (requested_limit * step_ms)
        if start_ms is not None and end_ms is not None:
            newest_window_start = max(0, int(end_ms) - (max(requested_limit - 1, 0) * step_ms))
            start_ms = max(int(start_ms), newest_window_start)

        candles = []
        seen_timestamps = set()
        current_since = int(start_ms)

        while current_since <= int(end_ms) and len(candles) < requested_limit:
            batch_limit = min(self.GENERIC_MAX_OHLCV_CANDLES, requested_limit - len(candles))
            batch = await self._call_unified(
                "fetch_ohlcv",
                symbol,
                timeframe=timeframe,
                since=current_since,
                limit=batch_limit,
                params={},
                default=[],
            )
            if not batch:
                break

            new_rows = 0
            last_timestamp = None
            for candle in batch:
                if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                    continue
                timestamp = int(candle[0])
                last_timestamp = timestamp
                if timestamp < int(start_ms) or timestamp > int(end_ms):
                    continue
                if timestamp in seen_timestamps:
                    continue
                seen_timestamps.add(timestamp)
                candles.append(list(candle[:6]))
                new_rows += 1

            candles.sort(key=lambda row: row[0])
            if len(candles) >= requested_limit or last_timestamp is None or new_rows == 0 or len(batch) < batch_limit:
                break

            next_since = int(last_timestamp) + step_ms
            if next_since <= current_since:
                break
            current_since = next_since

        return candles[-requested_limit:]

    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100, start_time=None, end_time=None):
        await self._ensure_connected()
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not self.supports_symbol(normalized_symbol):
            return []
        if start_time is not None or end_time is not None:
            if self._exchange_code() == "coinbase":
                return await self._fetch_coinbase_ohlcv(
                    normalized_symbol,
                    timeframe=timeframe,
                    limit=limit,
                    start_time=start_time,
                    end_time=end_time,
                )
            return await self._fetch_generic_ohlcv_range(
                normalized_symbol,
                timeframe=timeframe,
                limit=limit,
                start_time=start_time,
                end_time=end_time,
            )
        if self._exchange_code() == "coinbase":
            return await self._fetch_coinbase_ohlcv_cached(normalized_symbol, timeframe=timeframe, limit=limit)
        self.logger.debug("Fetching OHLCV for %s", normalized_symbol)
        return await self._call_unified(
            "fetch_ohlcv",
            normalized_symbol,
            timeframe=timeframe,
            limit=limit,
            default=[],
        )

    async def fetch_trades(self, symbol, limit=None):
        normalized_symbol = self._normalize_market_symbol(symbol)
        if not self.supports_symbol(normalized_symbol):
            return []
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_trades", normalized_symbol, default=[], **kwargs)

    async def fetch_my_trades(self, symbol=None, limit=None):
        normalized_symbol = self._normalize_market_symbol(symbol) if symbol else None
        if normalized_symbol and not self.supports_symbol(normalized_symbol):
            return []
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_my_trades", normalized_symbol, default=[], **kwargs)

    # ==========================================================
    # TRADING
    # ==========================================================

    async def create_order(
        self,
        symbol,
        side,
        amount,
        type="market",
        price=None,
        stop_price=None,
        params=None,
        stop_loss=None,
        take_profit=None,
    ):
        await self._ensure_connected()

        normalized_type = str(type or "market").strip().lower() or "market"
        trigger_price = stop_price
        if trigger_price is None and isinstance(params, dict):
            trigger_price = params.get("stop_price", params.get("stopPrice"))
        if normalized_type == "stop_limit":
            if price is None or float(price) <= 0:
                raise ValueError("stop_limit orders require a positive limit price")
            if trigger_price is None or float(trigger_price) <= 0:
                raise ValueError("stop_limit orders require a positive stop_price trigger")

        normalized_amount = self._maybe_precision_amount(symbol, float(amount))
        normalized_price = self._maybe_precision_price(symbol, price)
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        if trigger_price is not None:
            order_params.setdefault("stopPrice", float(trigger_price))
            order_params.setdefault("stop_price", float(trigger_price))
        if stop_loss is not None and self._supports_attached_protection_prices():
            order_params.setdefault("stopLossPrice", stop_loss)
        if take_profit is not None and self._supports_attached_protection_prices():
            order_params.setdefault("takeProfitPrice", take_profit)

        if not self._exchange_has("create_order"):
            raise NotImplementedError(f"{self.exchange_name} does not support create_order")

        created = await self.exchange.create_order(
            symbol,
            normalized_type,
            str(side).lower(),
            normalized_amount,
            normalized_price,
            order_params,
        )
        self._invalidate_account_state_cache()
        if isinstance(created, dict) and trigger_price is not None:
            created.setdefault("stop_price", float(trigger_price))
        return created

    async def cancel_order(self, order_id, symbol=None):
        self._invalidate_account_state_cache()
        if symbol is None:
            return await self._call_unified("cancel_order", order_id)
        return await self._call_unified("cancel_order", order_id, symbol)

    async def cancel_all_orders(self, symbol=None):
        self._invalidate_account_state_cache()
        if symbol is None:
            return await self._call_unified("cancel_all_orders", default=[])
        return await self._call_unified("cancel_all_orders", symbol, default=[])

    # ==========================================================
    # ACCOUNT
    # ==========================================================

    async def fetch_balance(self):
        if not self._supports_positions_endpoint():
            try:
                snapshot = await self._get_spot_account_snapshot_cached()
                return snapshot.get("balance")
            except Exception as exc:
                self.logger.debug("Unable to derive spot account valuation on %s: %s", self.exchange_name, exc)
        raw_balance = await self._fetch_raw_balance()
        if not isinstance(raw_balance, dict):
            return raw_balance
        return raw_balance

    async def fetch_positions(self, symbols=None):
        if not self._supports_positions_endpoint():
            try:
                snapshot = await self._get_spot_account_snapshot_cached()
                return [
                    position
                    for position in (snapshot or {}).get("positions", [])
                    if isinstance(position, dict) and self._spot_position_matches_symbols(position, symbols)
                ]
            except Exception as exc:
                self.logger.debug("Unable to derive spot positions on %s: %s", self.exchange_name, exc)
                return []
        return await self._call_unified("fetch_positions", symbols, default=[])

    async def fetch_order(self, order_id, symbol=None):
        if symbol is None:
            return await self._call_unified("fetch_order", order_id)
        return await self._call_unified("fetch_order", order_id, symbol)

    async def fetch_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_orders", symbol, default=[], **kwargs)

    async def fetch_open_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        target_symbol = self._default_open_orders_symbol(symbol)
        return await self._call_unified("fetch_open_orders", target_symbol, default=[], **kwargs)

    async def fetch_open_orders_snapshot(self, symbols=None, limit=None):
        monitored_symbols = self._monitored_symbols(symbols)
        exchange_code = str(self.exchange_name or "").strip().lower()
        cache_key = ("snapshot", tuple(monitored_symbols), limit)
        now = time.monotonic()

        if exchange_code == "binanceus":
            if len(monitored_symbols) <= 8 and monitored_symbols:
                ttl_seconds = 15.0
                fetcher = lambda: self._fetch_open_orders_by_symbols(monitored_symbols, limit=limit)
            else:
                cache_key = ("snapshot-global", limit)
                ttl_seconds = 310.0
                fetcher = lambda: self._fetch_open_orders_without_symbol(limit=limit)

            cached = self._open_orders_snapshot_cache.get(cache_key)
            if cached and now < cached["expires_at"]:
                return list(cached["orders"])

            orders = self._dedupe_orders_snapshot(await fetcher())
            self._open_orders_snapshot_cache[cache_key] = {
                "orders": list(orders),
                "expires_at": now + ttl_seconds,
            }
            return orders

        if exchange_code == "coinbase":
            cache_key = ("coinbase-snapshot", tuple(monitored_symbols), limit)
            cached = self._open_orders_snapshot_cache.get(cache_key)
            if cached and now < cached["expires_at"]:
                return list(cached["orders"])

            try:
                orders = self._dedupe_orders_snapshot(await self._fetch_open_orders_without_symbol(limit=limit))
            except Exception:
                if monitored_symbols:
                    orders = await self._fetch_open_orders_by_symbols(monitored_symbols, limit=limit)
                else:
                    raise

            self._open_orders_snapshot_cache[cache_key] = {
                "orders": list(orders),
                "expires_at": now + self.COINBASE_OPEN_ORDERS_CACHE_SECONDS,
            }
            return orders

        try:
            return self._dedupe_orders_snapshot(await self._fetch_open_orders_without_symbol(limit=limit))
        except Exception:
            if monitored_symbols:
                return await self._fetch_open_orders_by_symbols(monitored_symbols, limit=limit)
            raise

    async def fetch_closed_orders(self, symbol=None, limit=None):
        kwargs = {}
        if limit is not None:
            kwargs["limit"] = limit
        return await self._call_unified("fetch_closed_orders", symbol, default=[], **kwargs)

    async def withdraw(self, code, amount, address, tag=None, params=None):
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        if tag is not None:
            order_params.setdefault("tag", tag)
        self._invalidate_account_state_cache()
        return await self._call_unified(
            "withdraw",
            code,
            amount,
            address,
            tag,
            order_params,
        )

    async def fetch_deposit_address(self, code, params=None):
        order_params = dict(self.extra_params)
        if params:
            order_params.update(params)
        return await self._call_unified(
            "fetch_deposit_address",
            code,
            order_params,
        )
