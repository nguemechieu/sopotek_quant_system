from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - optional dependency in stripped test environments
    aiohttp = None

from broker.base_broker import BaseDerivativeBroker
from models.instrument import Instrument, InstrumentType, OptionRight
from models.order import Order, OrderLeg, OrderSide, OrderType
from models.position import Position


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00"), f"{text}T00:00:00+00:00"):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


class TDAmeritradeBroker(BaseDerivativeBroker):
    supported_instrument_types = {
        InstrumentType.STOCK.value,
        InstrumentType.OPTION.value,
    }

    def __init__(self, config, event_bus=None):
        super().__init__(config, event_bus=event_bus)
        self.account_hash = self.options.get("account_hash") or self.params.get("account_hash")
        self.market_data_url = str(
            self.options.get("market_data_base_url")
            or self.params.get("market_data_base_url")
            or "https://api.schwabapi.com/marketdata/v1"
        ).rstrip("/")
        self.token_url = str(
            self.options.get("token_url")
            or self.params.get("token_url")
            or "https://api.schwabapi.com/v1/oauth/token"
        ).rstrip("/")
        self.default_contract_size = int(self.options.get("default_contract_size") or 100)

    def default_base_url(self):
        return "https://api.schwabapi.com/trader/v1"

    def _auth_headers(self):
        headers = super()._auth_headers()
        if self.api_key and not headers.get("X-Api-Key"):
            headers["X-Api-Key"] = self.api_key
        return headers

    async def _authenticate(self):
        if self.access_token:
            return
        if not self.refresh_token or not self.client_id:
            return
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for Schwab OAuth authentication")

        session = await self._ensure_session()
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
        }
        auth = aiohttp.BasicAuth(self.client_id, self.client_secret or "")
        async with session.post(
            self.token_url,
            data=payload,
            auth=auth,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as response:
            if response.status not in (200, 201):
                body = await response.text()
                raise RuntimeError(f"schwab auth failed: {response.status} {body}")
            tokens = await response.json()
        self.access_token = tokens.get("access_token") or self.access_token
        self.refresh_token = tokens.get("refresh_token") or self.refresh_token

    async def _resolve_account_hash(self) -> str:
        if self.account_hash:
            return str(self.account_hash)
        if self.account_id and str(self.account_id).strip():
            self.account_hash = str(self.account_id).strip()
            return self.account_hash

        payload = await self._request_json(
            "GET",
            self.options.get("account_numbers_path") or "/accounts/accountNumbers",
        )
        accounts = payload if isinstance(payload, list) else payload.get("accounts") or payload.get("accountNumbers") or []
        if not accounts:
            raise RuntimeError("No Schwab accounts were returned")

        selected = accounts[0]
        self.account_id = str(selected.get("accountNumber") or selected.get("accountId") or "").strip() or self.account_id
        self.account_hash = str(selected.get("hashValue") or selected.get("accountHash") or self.account_id or "").strip()
        if not self.account_hash:
            raise RuntimeError("Schwab account hash could not be resolved")
        return self.account_hash

    def _normalize_account(self, account_payload: Mapping[str, Any]) -> dict[str, Any]:
        balance = dict(account_payload.get("currentBalances") or {})
        initial = dict(account_payload.get("initialBalances") or {})
        return {
            "broker": self.exchange_name,
            "account_id": self.account_id,
            "account_hash": self.account_hash,
            "currency": account_payload.get("roundTrips") and "USD" or "USD",
            "cash": _safe_float(balance.get("cashBalance", initial.get("cashAvailableForTrading", 0.0))),
            "buying_power": _safe_float(balance.get("buyingPower", initial.get("buyingPower", 0.0))),
            "equity": _safe_float(balance.get("equity", initial.get("accountValue", 0.0))),
            "liquidation_value": _safe_float(balance.get("liquidationValue", balance.get("equity", 0.0))),
            "margin_used": _safe_float(balance.get("marginBalance", 0.0)),
            "available_funds": _safe_float(balance.get("availableFunds", balance.get("cashAvailableForTrading", 0.0))),
            "maintenance_requirement": _safe_float(balance.get("maintenanceRequirement", 0.0)),
            "raw": dict(account_payload),
        }

    def _instrument_from_payload(self, raw_instrument: Mapping[str, Any]) -> Instrument:
        asset_type = str(raw_instrument.get("assetType") or raw_instrument.get("type") or "EQUITY").strip().upper()
        instrument_type = InstrumentType.OPTION if asset_type == "OPTION" else InstrumentType.STOCK
        option_right = raw_instrument.get("putCall") or raw_instrument.get("option_type")
        if option_right is not None:
            option_right = str(option_right).strip().lower()
            option_right = OptionRight.CALL if option_right.startswith("c") else OptionRight.PUT
        return Instrument(
            symbol=raw_instrument.get("symbol") or raw_instrument.get("description"),
            type=instrument_type,
            expiry=raw_instrument.get("expirationDate") or raw_instrument.get("expiration"),
            strike=raw_instrument.get("strikePrice"),
            option_type=option_right,
            contract_size=self.default_contract_size if instrument_type is InstrumentType.OPTION else 1,
            exchange=self.exchange_name,
            underlying=raw_instrument.get("underlyingSymbol"),
            multiplier=self.default_contract_size if instrument_type is InstrumentType.OPTION else 1.0,
            metadata=dict(raw_instrument),
        )

    def _normalize_position(self, raw_position: Mapping[str, Any]) -> dict[str, Any]:
        instrument = self._instrument_from_payload(dict(raw_position.get("instrument") or {}))
        long_qty = _safe_float(raw_position.get("longQuantity", 0.0))
        short_qty = _safe_float(raw_position.get("shortQuantity", 0.0))
        quantity = long_qty if long_qty > 0 else -short_qty
        position = Position(
            symbol=instrument.symbol,
            quantity=quantity,
            side="long" if quantity >= 0 else "short",
            instrument=instrument,
            avg_price=_safe_float(raw_position.get("averagePrice", 0.0)),
            mark_price=_safe_float(raw_position.get("marketValue", 0.0)) / max(abs(quantity) * max(instrument.multiplier, 1.0), 1.0)
            if quantity
            else None,
            unrealized_pnl=_safe_float(raw_position.get("currentDayProfitLoss", raw_position.get("longOpenProfitLoss", 0.0))),
            broker=self.exchange_name,
            account_id=self.account_id,
            metadata=dict(raw_position),
        )
        return position.to_dict()

    async def get_account_info(self):
        account_hash = await self._resolve_account_hash()
        payload = await self._request_json("GET", f"/accounts/{account_hash}", params={"fields": "positions"})
        account = payload.get("securitiesAccount") if isinstance(payload, Mapping) else {}
        normalized = self._normalize_account(account)
        await self._emit_account_event(normalized)
        return normalized

    async def get_positions(self):
        account_hash = await self._resolve_account_hash()
        payload = await self._request_json("GET", f"/accounts/{account_hash}", params={"fields": "positions"})
        account = payload.get("securitiesAccount") if isinstance(payload, Mapping) else {}
        positions = [self._normalize_position(item) for item in list(account.get("positions") or [])]
        for position in positions:
            await self._emit_position_event(position)
        return positions

    def _leg_instruction(self, leg: OrderLeg, *, closing: bool = False) -> str:
        if leg.instrument.type is InstrumentType.OPTION:
            if leg.side is OrderSide.BUY:
                return "BUY_TO_CLOSE" if closing else "BUY_TO_OPEN"
            return "SELL_TO_CLOSE" if closing else "SELL_TO_OPEN"
        return "BUY" if leg.side is OrderSide.BUY else "SELL"

    def _build_order_payload(self, order: Order) -> dict[str, Any]:
        duration = order.time_in_force if order.time_in_force in {"DAY", "GTC"} else "DAY"
        legs = list(order.legs)
        if not legs:
            instrument = order.instrument or Instrument(symbol=order.symbol, exchange=self.exchange_name)
            legs = [OrderLeg(instrument=instrument, side=order.side, quantity=order.quantity)]

        leg_payloads = []
        for leg in legs:
            leg_payloads.append(
                {
                    "instruction": self._leg_instruction(leg, closing=bool(order.params.get("closing"))),
                    "quantity": abs(leg.quantity),
                    "instrument": {
                        "assetType": "OPTION" if leg.instrument.type is InstrumentType.OPTION else "EQUITY",
                        "symbol": leg.instrument.symbol,
                    },
                }
            )

        payload: dict[str, Any] = {
            "session": str(order.params.get("session") or "NORMAL").upper(),
            "duration": duration,
            "orderType": "MARKET" if order.order_type is OrderType.BRACKET else order.order_type.value.replace("_", "").upper(),
            "orderStrategyType": "TRIGGER" if order.order_type is OrderType.BRACKET else "SINGLE",
            "orderLegCollection": leg_payloads,
        }
        if order.price is not None:
            payload["price"] = round(float(order.price), 4)
        if order.stop_price is not None:
            payload["stopPrice"] = round(float(order.stop_price), 4)
        if len(leg_payloads) > 1:
            payload["complexOrderStrategyType"] = str(order.params.get("complex_order_strategy") or "CUSTOM").upper()

        exit_side = OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY
        child_strategies = []
        if order.take_profit is not None:
            child_strategies.append(
                {
                    "orderType": "LIMIT",
                    "session": "NORMAL",
                    "duration": duration,
                    "orderStrategyType": "SINGLE",
                    "price": round(float(order.take_profit), 4),
                    "orderLegCollection": [
                        {
                            "instruction": "SELL" if exit_side is OrderSide.SELL else "BUY",
                            "quantity": abs(order.quantity),
                            "instrument": {"assetType": "EQUITY", "symbol": order.symbol},
                        }
                    ],
                }
            )
        if order.stop_loss is not None:
            child_strategies.append(
                {
                    "orderType": "STOP",
                    "session": "NORMAL",
                    "duration": duration,
                    "orderStrategyType": "SINGLE",
                    "stopPrice": round(float(order.stop_loss), 4),
                    "orderLegCollection": [
                        {
                            "instruction": "SELL" if exit_side is OrderSide.SELL else "BUY",
                            "quantity": abs(order.quantity),
                            "instrument": {"assetType": "EQUITY", "symbol": order.symbol},
                        }
                    ],
                }
            )
        if child_strategies:
            payload["childOrderStrategies"] = child_strategies
        return payload

    async def place_order(self, order):
        order = Order.from_mapping(order)
        account_hash = await self._resolve_account_hash()
        payload = self._build_order_payload(order)
        _, headers, _status = await self._request_json(
            "POST",
            f"/accounts/{account_hash}/orders",
            json_payload=payload,
            include_meta=True,
            expected_statuses=(200, 201, 202),
        )
        location = headers.get("Location") or headers.get("location") or ""
        order_id = location.rstrip("/").split("/")[-1] if location else order.client_order_id
        normalized = {
            "id": order_id,
            "clientOrderId": order.client_order_id,
            "broker": self.exchange_name,
            "account_id": self.account_id,
            "symbol": order.symbol,
            "side": order.side.value,
            "amount": order.quantity,
            "type": order.order_type.value,
            "price": order.price,
            "stop_price": order.stop_price,
            "status": "submitted",
            "raw": payload,
        }
        await self._emit_order_event(normalized)
        return normalized

    async def cancel_order(self, order_id, symbol=None):
        account_hash = await self._resolve_account_hash()
        await self._request_json(
            "DELETE",
            f"/accounts/{account_hash}/orders/{order_id}",
            expected_statuses=(200, 202, 204),
        )
        normalized = {"id": str(order_id), "symbol": symbol, "status": "canceled", "broker": self.exchange_name}
        await self._emit_order_event(normalized)
        return normalized

    async def _fetch_quotes(self, symbols):
        normalized_symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
        if not normalized_symbols:
            return []
        payload = await self._request_json(
            "GET",
            self.options.get("quotes_path") or "/quotes",
            params={"symbols": ",".join(normalized_symbols)},
            base_url=self.market_data_url,
        )
        quote_map = payload if isinstance(payload, Mapping) else {}
        quotes = []
        for symbol in normalized_symbols:
            raw = quote_map.get(symbol) or quote_map.get(symbol.upper()) or {}
            quote = {
                "broker": self.exchange_name,
                "symbol": symbol,
                "bid": _safe_float(raw.get("bidPrice", raw.get("bid", 0.0))),
                "ask": _safe_float(raw.get("askPrice", raw.get("ask", 0.0))),
                "last": _safe_float(raw.get("lastPrice", raw.get("last", 0.0))),
                "close": _safe_float(raw.get("closePrice", raw.get("close", 0.0))),
                "mark": _safe_float(raw.get("mark", raw.get("markPrice", raw.get("lastPrice", 0.0)))),
                "timestamp": raw.get("quoteTime") or datetime.now(timezone.utc).isoformat(),
                "raw": dict(raw),
            }
            quotes.append(quote)
        return quotes

    async def _list_orders(self, status=None, symbol=None, limit=None):
        account_hash = await self._resolve_account_hash()
        end_at = datetime.now(timezone.utc)
        start_at = end_at - timedelta(days=int(self.options.get("orders_lookback_days") or 7))
        payload = await self._request_json(
            "GET",
            f"/accounts/{account_hash}/orders",
            params={
                "fromEnteredTime": start_at.isoformat(),
                "toEnteredTime": end_at.isoformat(),
                "maxResults": limit or self.options.get("orders_limit") or 50,
            },
        )
        orders = payload if isinstance(payload, list) else payload.get("orders") or []
        normalized_orders = []
        for raw in orders:
            if symbol and str(raw.get("orderLegCollection", [{}])[0].get("instrument", {}).get("symbol") or "").upper() != str(symbol).upper():
                continue
            normalized_status = str(raw.get("status") or "unknown").strip().lower()
            if status == "open" and normalized_status in {"filled", "canceled", "expired", "rejected"}:
                continue
            if status == "closed" and normalized_status not in {"filled", "canceled", "expired", "rejected"}:
                continue
            leg = next(iter(raw.get("orderLegCollection") or [{}]), {})
            normalized_orders.append(
                {
                    "id": raw.get("orderId"),
                    "broker": self.exchange_name,
                    "symbol": leg.get("instrument", {}).get("symbol"),
                    "side": str(leg.get("instruction") or "").lower(),
                    "amount": _safe_float(leg.get("quantity", raw.get("quantity", 0.0))),
                    "type": str(raw.get("orderType") or "").lower(),
                    "price": _safe_float(raw.get("price", 0.0)) or None,
                    "stop_price": _safe_float(raw.get("stopPrice", 0.0)) or None,
                    "status": normalized_status,
                    "timestamp": raw.get("enteredTime"),
                    "raw": dict(raw),
                }
            )
        return normalized_orders

    async def _get_order(self, order_id, symbol=None):
        for order in await self._list_orders(symbol=symbol, limit=self.options.get("orders_limit") or 100):
            if str(order.get("id")) == str(order_id):
                return order
        return None

    def _normalize_chain_contract(self, symbol: str, expiry: str, strike: str, contract: Mapping[str, Any]) -> dict[str, Any]:
        put_call = str(contract.get("putCall") or contract.get("option_type") or "").strip().lower()
        option_type = OptionRight.CALL if put_call.startswith("c") else OptionRight.PUT
        instrument = Instrument(
            symbol=contract.get("symbol") or f"{symbol}_{expiry}_{strike}_{option_type.value.upper()}",
            type=InstrumentType.OPTION,
            expiry=_safe_datetime(expiry.split(":", 1)[0]),
            strike=_safe_float(strike),
            option_type=option_type,
            contract_size=self.default_contract_size,
            exchange=self.exchange_name,
            underlying=symbol,
            multiplier=self.default_contract_size,
            metadata=dict(contract),
        )
        return {
            "instrument": instrument.to_dict(),
            "symbol": instrument.symbol,
            "expiry": instrument.expiry.isoformat() if instrument.expiry is not None else None,
            "strike": instrument.strike,
            "option_type": instrument.option_type.value if instrument.option_type is not None else None,
            "bid": _safe_float(contract.get("bid", 0.0)),
            "ask": _safe_float(contract.get("ask", 0.0)),
            "last": _safe_float(contract.get("last", 0.0)),
            "mark": _safe_float(contract.get("mark", contract.get("last", 0.0))),
            "volume": int(_safe_float(contract.get("totalVolume", contract.get("volume", 0.0)))),
            "open_interest": int(_safe_float(contract.get("openInterest", 0.0))),
            "delta": _safe_float(contract.get("delta", 0.0)),
            "gamma": _safe_float(contract.get("gamma", 0.0)),
            "theta": _safe_float(contract.get("theta", 0.0)),
            "vega": _safe_float(contract.get("vega", 0.0)),
            "broker": self.exchange_name,
            "raw": dict(contract),
        }

    async def get_option_chain(self, symbol, **kwargs):
        payload = await self._request_json(
            "GET",
            self.options.get("option_chain_path") or "/chains",
            params={
                "symbol": str(symbol).strip().upper(),
                "contractType": kwargs.get("contract_type", "ALL"),
                "strikeCount": kwargs.get("strike_count", 20),
                "includeUnderlyingQuote": "TRUE",
            },
            base_url=self.market_data_url,
        )
        contracts = []
        for side_key in ("callExpDateMap", "putExpDateMap"):
            expiry_map = dict(payload.get(side_key) or {})
            for expiry, strike_map in expiry_map.items():
                for strike, raw_contracts in dict(strike_map or {}).items():
                    for contract in list(raw_contracts or []):
                        contracts.append(self._normalize_chain_contract(str(symbol).strip().upper(), expiry, strike, contract))
        return {
            "broker": self.exchange_name,
            "symbol": str(symbol).strip().upper(),
            "underlying_price": _safe_float((payload.get("underlying") or {}).get("last", 0.0)),
            "interest_rate": _safe_float(payload.get("interestRate", 0.0)),
            "volatility": _safe_float(payload.get("volatility", 0.0)),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "contracts": contracts,
            "raw": dict(payload),
        }
