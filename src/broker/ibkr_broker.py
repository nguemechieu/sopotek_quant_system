from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Optional

from broker.base_broker import BaseDerivativeBroker
from models.instrument import Instrument, InstrumentType, OptionRight
from models.order import Order, OrderSide
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
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


class IBKRBroker(BaseDerivativeBroker):
    supported_instrument_types = {
        InstrumentType.STOCK.value,
        InstrumentType.OPTION.value,
        InstrumentType.FUTURE.value,
    }

    def __init__(self, config, event_bus=None):
        super().__init__(config, event_bus=event_bus)
        self.conid_cache = dict(self.options.get("contract_map") or {})
        self.account_capabilities = {}

    def default_base_url(self):
        return "https://localhost:5000/v1/api"

    async def _authenticate(self):
        status_path = self.options.get("auth_status_path") or "/iserver/auth/status"
        try:
            payload = await self._request_json("POST", status_path, expected_statuses=(200,))
        except Exception:
            self.logger.debug("IBKR auth status request failed", exc_info=True)
            return
        if isinstance(payload, Mapping):
            self.account_capabilities = dict(payload)

    async def _resolve_account_id(self) -> str:
        if self.account_id:
            return str(self.account_id)
        payload = await self._request_json("GET", self.options.get("accounts_path") or "/portfolio/accounts")
        accounts = payload if isinstance(payload, list) else payload.get("accounts") or []
        if not accounts:
            raise RuntimeError("No IBKR accounts were returned")
        selected = accounts[0]
        self.account_id = str(selected.get("id") or selected.get("accountId") or selected.get("accountIdKey") or selected.get("account"))
        return self.account_id

    def _normalize_instrument_type(self, raw_type: Any) -> InstrumentType:
        normalized = str(raw_type or "").strip().upper()
        if normalized in {"OPT", "OPTION"}:
            return InstrumentType.OPTION
        if normalized in {"FUT", "FUTURE"}:
            return InstrumentType.FUTURE
        return InstrumentType.STOCK

    def _instrument_from_contract(self, raw: Mapping[str, Any]) -> Instrument:
        instrument_type = self._normalize_instrument_type(raw.get("assetClass") or raw.get("secType"))
        right_value = str(raw.get("right") or raw.get("putCall") or "").strip().lower()
        option_right = None
        if right_value:
            option_right = OptionRight.CALL if right_value.startswith("c") else OptionRight.PUT
        multiplier = _safe_float(raw.get("multiplier", 100.0 if instrument_type is InstrumentType.OPTION else 1.0), 1.0)
        return Instrument(
            symbol=raw.get("symbol") or raw.get("local_symbol") or raw.get("localSymbol") or raw.get("ticker"),
            type=instrument_type,
            expiry=raw.get("expiry") or raw.get("lastTradeDate") or raw.get("maturityDate"),
            strike=raw.get("strike"),
            option_type=option_right,
            contract_size=int(multiplier) if instrument_type in {InstrumentType.OPTION, InstrumentType.FUTURE} else 1,
            exchange=raw.get("exchange") or raw.get("listingExchange") or self.exchange_name,
            currency=raw.get("currency") or "USD",
            multiplier=multiplier,
            underlying=raw.get("underlyingSymbol"),
            metadata=dict(raw),
        )

    async def _resolve_conid(self, instrument: Instrument | Mapping[str, Any] | None, symbol: str) -> str:
        if isinstance(instrument, Mapping):
            instrument = Instrument.from_mapping(instrument)
        cache_key = ""
        if isinstance(instrument, Instrument):
            cache_key = instrument.symbol
            cached = self.conid_cache.get(cache_key) or instrument.metadata.get("conid") or instrument.metadata.get("contract_id")
            if cached:
                return str(cached)
        cached = self.conid_cache.get(symbol)
        if cached:
            return str(cached)

        params = {
            "symbol": (instrument.symbol if isinstance(instrument, Instrument) else symbol).split(" ", 1)[0],
        }
        if isinstance(instrument, Instrument) and instrument.type is InstrumentType.OPTION:
            params["name"] = instrument.underlying or instrument.root_symbol
            params["secType"] = "OPT"
        elif isinstance(instrument, Instrument) and instrument.type is InstrumentType.FUTURE:
            params["secType"] = "FUT"
        payload = await self._request_json("GET", self.options.get("contract_search_path") or "/iserver/secdef/search", params=params)
        results = payload if isinstance(payload, list) else payload.get("contracts") or payload.get("results") or []
        if not results:
            raise RuntimeError(f"IBKR contract id could not be resolved for {symbol}")
        selected = results[0]
        conid = str(selected.get("conid") or selected.get("conidex") or "")
        if not conid:
            raise RuntimeError(f"IBKR contract id could not be resolved for {symbol}")
        self.conid_cache[symbol] = conid
        if cache_key:
            self.conid_cache[cache_key] = conid
        return conid

    def _normalize_account(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "broker": self.exchange_name,
            "account_id": self.account_id,
            "currency": raw.get("currency") or raw.get("baseCurrency") or "USD",
            "cash": _safe_float(raw.get("cashbalance", raw.get("cash", 0.0))),
            "equity": _safe_float(raw.get("equitywithloanvalue", raw.get("netliq", raw.get("equity", 0.0)))),
            "buying_power": _safe_float(raw.get("buyingpower", raw.get("availablefunds", 0.0))),
            "margin_used": max(
                0.0,
                _safe_float(raw.get("initmarginreq", raw.get("initialMargin", 0.0)))
                or _safe_float(raw.get("marginreq", raw.get("margin", 0.0))),
            ),
            "maintenance_requirement": _safe_float(raw.get("maintmarginreq", raw.get("maintenanceMargin", 0.0))),
            "available_funds": _safe_float(raw.get("availablefunds", raw.get("availableFunds", 0.0))),
            "raw": dict(raw),
        }

    def _normalize_position(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        instrument = self._instrument_from_contract(raw)
        quantity = _safe_float(raw.get("position", raw.get("quantity", 0.0)))
        position = Position(
            symbol=instrument.symbol,
            quantity=quantity,
            side="long" if quantity >= 0 else "short",
            instrument=instrument,
            avg_price=_safe_float(raw.get("avgCost", raw.get("avgPrice", 0.0))),
            mark_price=_safe_float(raw.get("mktPrice", raw.get("marketPrice", 0.0))) or None,
            leverage=_safe_float(raw.get("leverage", 0.0)) or None,
            margin_used=_safe_float(raw.get("margin", raw.get("initialMargin", 0.0))),
            unrealized_pnl=_safe_float(raw.get("unrealizedPnl", raw.get("upl", 0.0))),
            realized_pnl=_safe_float(raw.get("realizedPnl", raw.get("rpl", 0.0))),
            broker=self.exchange_name,
            account_id=self.account_id,
            metadata=dict(raw),
        )
        return position.to_dict()

    async def get_account_info(self):
        account_id = await self._resolve_account_id()
        payload = await self._request_json("GET", f"/portfolio/{account_id}/summary")
        summary = payload if isinstance(payload, Mapping) else {}
        normalized = self._normalize_account(summary)
        await self._emit_account_event(normalized)
        return normalized

    async def get_positions(self):
        account_id = await self._resolve_account_id()
        payload = await self._request_json(
            "GET",
            self.options.get("positions_path") or f"/portfolio/{account_id}/positions/0",
        )
        positions = payload if isinstance(payload, list) else payload.get("positions") or []
        normalized = [self._normalize_position(item) for item in positions]
        for position in normalized:
            await self._emit_position_event(position)
        return normalized

    def _order_type_code(self, order: Order) -> str:
        mapping = {
            OrderType.MARKET: "MKT",
            OrderType.LIMIT: "LMT",
            OrderType.STOP: "STP",
            OrderType.STOP_LIMIT: "STP LMT",
            OrderType.BRACKET: "LMT" if order.price is not None else "MKT",
        }
        return mapping.get(order.order_type, "MKT")

    async def place_order(self, order):
        order = Order.from_mapping(order)
        account_id = await self._resolve_account_id()
        instrument = order.instrument or Instrument(symbol=order.symbol, exchange=self.exchange_name)
        conid = await self._resolve_conid(instrument, order.symbol)
        payload_order: dict[str, Any] = {
            "acctId": account_id,
            "conid": int(conid) if str(conid).isdigit() else conid,
            "secType": "OPT" if instrument.type is InstrumentType.OPTION else "FUT" if instrument.type is InstrumentType.FUTURE else "STK",
            "cOID": order.client_order_id or f"{order.symbol}-{int(datetime.now(timezone.utc).timestamp())}",
            "orderType": self._order_type_code(order),
            "side": "BUY" if order.side is OrderSide.BUY else "SELL",
            "quantity": abs(order.quantity),
            "tif": order.time_in_force,
        }
        if order.price is not None:
            payload_order["price"] = float(order.price)
        if order.stop_price is not None:
            payload_order["auxPrice"] = float(order.stop_price)
        if order.account_id:
            payload_order["acctId"] = order.account_id

        payload = {"orders": [payload_order]}
        if order.order_type is OrderType.BRACKET or order.stop_loss is not None or order.take_profit is not None:
            payload_order["isSingleGroup"] = True
            attachments = []
            exit_side = "SELL" if order.side is OrderSide.BUY else "BUY"
            if order.take_profit is not None:
                attachments.append(
                    {
                        "acctId": payload_order["acctId"],
                        "conid": payload_order["conid"],
                        "orderType": "LMT",
                        "side": exit_side,
                        "quantity": abs(order.quantity),
                        "price": float(order.take_profit),
                        "tif": order.time_in_force,
                        "parentId": payload_order["cOID"],
                    }
                )
            if order.stop_loss is not None:
                attachments.append(
                    {
                        "acctId": payload_order["acctId"],
                        "conid": payload_order["conid"],
                        "orderType": "STP",
                        "side": exit_side,
                        "quantity": abs(order.quantity),
                        "auxPrice": float(order.stop_loss),
                        "tif": order.time_in_force,
                        "parentId": payload_order["cOID"],
                    }
                )
            if attachments:
                payload["orders"].extend(attachments)

        response = await self._request_json(
            "POST",
            self.options.get("order_path") or f"/iserver/account/{account_id}/orders",
            json_payload=payload,
            expected_statuses=(200, 201),
        )
        result = response[0] if isinstance(response, list) and response else response
        normalized = {
            "id": result.get("order_id") or result.get("id") or result.get("local_order_id") or payload_order["cOID"],
            "clientOrderId": payload_order["cOID"],
            "broker": self.exchange_name,
            "account_id": payload_order["acctId"],
            "symbol": order.symbol,
            "side": order.side.value,
            "amount": order.quantity,
            "type": order.order_type.value,
            "price": order.price,
            "stop_price": order.stop_price,
            "status": str(result.get("order_status") or result.get("status") or "submitted").strip().lower(),
            "raw": result,
        }
        await self._emit_order_event(normalized)
        return normalized

    async def cancel_order(self, order_id, symbol=None):
        account_id = await self._resolve_account_id()
        payload = await self._request_json(
            "DELETE",
            self.options.get("cancel_order_path") or f"/iserver/account/{account_id}/order/{order_id}",
            expected_statuses=(200, 202, 204),
        )
        normalized = {
            "id": str(order_id),
            "symbol": symbol,
            "broker": self.exchange_name,
            "status": str((payload or {}).get("status") or "canceled").strip().lower(),
            "raw": payload or {},
        }
        await self._emit_order_event(normalized)
        return normalized

    async def _fetch_quotes(self, symbols):
        normalized_symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
        if not normalized_symbols:
            return []
        conids = [await self._resolve_conid(None, symbol) for symbol in normalized_symbols]
        payload = await self._request_json(
            "GET",
            self.options.get("market_snapshot_path") or "/iserver/marketdata/snapshot",
            params={
                "conids": ",".join(map(str, conids)),
                "fields": self.options.get("market_snapshot_fields") or "31,55,84,86,88,6008",
            },
        )
        snapshots = payload if isinstance(payload, list) else payload.get("data") or []
        snapshot_map = {}
        for item in snapshots:
            if isinstance(item, Mapping):
                snapshot_map[str(item.get("55") or item.get("symbol") or "").upper()] = dict(item)
        quotes = []
        for symbol in normalized_symbols:
            raw = snapshot_map.get(symbol, {})
            quotes.append(
                {
                    "broker": self.exchange_name,
                    "symbol": symbol,
                    "bid": _safe_float(raw.get("84", raw.get("bid", 0.0))),
                    "ask": _safe_float(raw.get("86", raw.get("ask", 0.0))),
                    "last": _safe_float(raw.get("31", raw.get("last", 0.0))),
                    "close": _safe_float(raw.get("88", raw.get("close", 0.0))),
                    "mark": _safe_float(raw.get("6008", raw.get("last", 0.0))),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "raw": raw,
                }
            )
        return quotes

    async def _list_orders(self, status=None, symbol=None, limit=None):
        account_id = await self._resolve_account_id()
        payload = await self._request_json("GET", self.options.get("live_orders_path") or f"/iserver/account/orders")
        orders = payload if isinstance(payload, list) else payload.get("orders") or []
        normalized = []
        for raw in orders:
            raw_symbol = raw.get("ticker") or raw.get("symbol")
            raw_status = str(raw.get("status") or raw.get("order_status") or "unknown").strip().lower()
            if symbol and str(raw_symbol).upper() != str(symbol).upper():
                continue
            if status == "open" and raw_status in {"filled", "cancelled", "canceled", "inactive", "rejected"}:
                continue
            if status == "closed" and raw_status not in {"filled", "cancelled", "canceled", "inactive", "rejected"}:
                continue
            normalized.append(
                {
                    "id": raw.get("orderId") or raw.get("order_id"),
                    "broker": self.exchange_name,
                    "account_id": account_id,
                    "symbol": raw_symbol,
                    "side": str(raw.get("side") or "").lower(),
                    "amount": _safe_float(raw.get("size", raw.get("quantity", 0.0))),
                    "filled": _safe_float(raw.get("filledQuantity", raw.get("filled", 0.0))),
                    "price": _safe_float(raw.get("price", 0.0)) or None,
                    "status": raw_status,
                    "timestamp": raw.get("lastExecutionTime") or raw.get("created"),
                    "raw": dict(raw),
                }
            )
        return normalized[: int(limit or len(normalized))]

    async def _get_order(self, order_id, symbol=None):
        for order in await self._list_orders(symbol=symbol):
            if str(order.get("id")) == str(order_id):
                return order
        return None

    async def get_option_chain(self, symbol, **kwargs):
        if self.options.get("option_chain_path"):
            payload = await self._request_json(
                "GET",
                self.options["option_chain_path"],
                params={"symbol": str(symbol).strip().upper(), **kwargs},
            )
            contracts = payload.get("contracts") if isinstance(payload, Mapping) else payload
        else:
            search_results = await self._request_json(
                "GET",
                self.options.get("contract_search_path") or "/iserver/secdef/search",
                params={"symbol": str(symbol).strip().upper(), "secType": "OPT"},
            )
            contracts = search_results if isinstance(search_results, list) else search_results.get("results") or []
        normalized_contracts = []
        for contract in list(contracts or []):
            instrument = self._instrument_from_contract(contract)
            if instrument.type is not InstrumentType.OPTION:
                continue
            normalized_contracts.append(
                {
                    "instrument": instrument.to_dict(),
                    "symbol": instrument.symbol,
                    "expiry": instrument.expiry.isoformat() if instrument.expiry else None,
                    "strike": instrument.strike,
                    "option_type": instrument.option_type.value if instrument.option_type else None,
                    "broker": self.exchange_name,
                    "bid": _safe_float(contract.get("bid", 0.0)),
                    "ask": _safe_float(contract.get("ask", 0.0)),
                    "last": _safe_float(contract.get("last", 0.0)),
                    "volume": int(_safe_float(contract.get("volume", 0.0))),
                    "open_interest": int(_safe_float(contract.get("openInterest", 0.0))),
                    "raw": dict(contract),
                }
            )
        return {
            "broker": self.exchange_name,
            "symbol": str(symbol).strip().upper(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "contracts": normalized_contracts,
        }

    async def get_contract_metadata(self, symbol, **kwargs):
        instrument = kwargs.get("instrument")
        conid = await self._resolve_conid(instrument, str(symbol).strip().upper())
        payload = await self._request_json(
            "GET",
            self.options.get("contract_metadata_path") or "/trsrv/secdef",
            params={"conids": conid},
        )
        records = payload if isinstance(payload, list) else payload.get("secdef") or payload.get("records") or []
        raw = records[0] if records else {}
        instrument_obj = self._instrument_from_contract(raw or {"symbol": symbol})
        return {
            "broker": self.exchange_name,
            "symbol": instrument_obj.symbol,
            "conid": conid,
            "tick_size": _safe_float(raw.get("minTick", raw.get("tick", 0.0))),
            "multiplier": _safe_float(raw.get("multiplier", instrument_obj.multiplier)),
            "currency": raw.get("currency") or instrument_obj.currency,
            "exchange": raw.get("exchange") or instrument_obj.exchange,
            "expiry": instrument_obj.expiry.isoformat() if instrument_obj.expiry else None,
            "raw": raw,
        }
