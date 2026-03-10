import logging

import aiohttp

from broker.base_broker import BaseBroker


class OandaBroker(BaseBroker):
    MAX_OHLCV_COUNT = 5000
    GRANULARITY_MAP = {
        "1m": "M1",
        "5m": "M5",
        "15m": "M15",
        "30m": "M30",
        "1h": "H1",
        "4h": "H4",
        "1d": "D",
        "1w": "W",
    }

    def __init__(self, config):
        super().__init__()

        self.logger = logging.getLogger("OandaBroker")
        self.config = config

        self.token = getattr(config, "api_key", None) or getattr(config, "token", None)
        self.account_id = getattr(config, "account_id", None)
        self.mode = (getattr(config, "mode", "paper") or "paper").lower()
        self.base_url = (
            "https://api-fxpractice.oanda.com"
            if self.mode in {"paper", "practice", "sandbox"}
            else "https://api-fxtrade.oanda.com"
        )

        self.session = None
        self._connected = False
        self._instrument_details = {}

        if not self.token:
            raise ValueError("Oanda API token is required")
        if not self.account_id:
            raise ValueError("Oanda account_id is required")

    # ===============================
    # INTERNALS
    # ===============================

    @property
    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    async def _ensure_connected(self):
        if not self._connected:
            await self.connect()

    async def _request(self, method, path, params=None, payload=None):
        await self._ensure_connected()

        url = f"{self.base_url}{path}"
        async with self.session.request(
            method,
            url,
            headers=self._headers,
            params=params,
            json=payload,
        ) as response:
            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError as exc:
                detail = ""
                try:
                    payload_text = await response.text()
                    detail = payload_text.strip()
                except Exception:
                    detail = ""

                message = f"{exc.status} {exc.message}"
                if detail:
                    message = f"{message}: {detail}"
                raise RuntimeError(message) from exc
            return await response.json()

    def _normalize_symbol(self, symbol):
        if not symbol:
            return symbol
        return str(symbol).replace("/", "_").upper()

    def _normalize_granularity(self, timeframe):
        key = str(timeframe or "1h").lower()
        return self.GRANULARITY_MAP.get(key, "H1")

    def _extract_price_entry(self, payload, symbol):
        prices = payload.get("prices", []) if isinstance(payload, dict) else []
        target = self._normalize_symbol(symbol)
        for price in prices:
            if price.get("instrument") == target:
                return price
        return prices[0] if prices else {}

    async def _ensure_instrument_details(self):
        if self._instrument_details:
            return self._instrument_details

        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/instruments")
        instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
        self._instrument_details = {
            item.get("name"): item
            for item in instruments
            if isinstance(item, dict) and item.get("name")
        }
        return self._instrument_details

    async def _get_instrument_meta(self, symbol):
        instrument = self._normalize_symbol(symbol)
        details = await self._ensure_instrument_details()
        return details.get(instrument, {})

    def _format_units(self, amount, precision):
        units = float(amount)
        precision = max(0, int(precision or 0))
        if precision == 0:
            return str(int(round(units)))
        formatted = f"{units:.{precision}f}".rstrip("0").rstrip(".")
        return formatted or "0"

    def _format_price(self, price, precision):
        precision = max(0, int(precision or 5))
        return f"{float(price):.{precision}f}"

    def _normalize_order_status(self, status):
        normalized = str(status or "").upper()
        mapping = {
            "PENDING": "open",
            "OPEN": "open",
            "FILLED": "filled",
            "CANCELLED": "canceled",
            "CANCEL_PENDING": "canceling",
            "TRIGGERED": "filled",
            "REJECTED": "rejected",
        }
        return mapping.get(normalized, normalized.lower() if normalized else "unknown")

    def _normalize_order_payload(self, payload, fallback_symbol=None, fallback_side=None, fallback_type=None, fallback_amount=None, fallback_price=None):
        if not isinstance(payload, dict):
            return payload

        order = (
            payload.get("order")
            or payload.get("orderCreateTransaction")
            or payload.get("orderCancelTransaction")
            or payload.get("lastTransaction")
            or {}
        )
        fill = payload.get("orderFillTransaction") or {}

        instrument = order.get("instrument") or fill.get("instrument") or self._normalize_symbol(fallback_symbol)
        units_value = (
            order.get("units")
            or fill.get("units")
            or fill.get("tradeOpened", {}).get("units")
            or fallback_amount
            or 0
        )
        try:
            units_float = float(units_value)
        except Exception:
            units_float = float(fallback_amount or 0)

        side = fallback_side
        if side is None:
            side = "buy" if units_float >= 0 else "sell"

        order_type = str(order.get("type") or fallback_type or "").lower() or None
        status = self._normalize_order_status(
            order.get("state")
            or fill.get("reason")
            or payload.get("state")
            or ("FILLED" if fill else None)
        )

        price_value = (
            order.get("price")
            or fill.get("price")
            or fill.get("fullVWAP")
            or fallback_price
        )
        try:
            price_float = float(price_value) if price_value is not None else None
        except Exception:
            price_float = fallback_price

        filled_value = (
            fill.get("units")
            or fill.get("tradeOpened", {}).get("units")
            or (units_float if status == "filled" else 0)
        )
        try:
            filled_float = abs(float(filled_value))
        except Exception:
            filled_float = abs(units_float) if status == "filled" else 0.0

        return {
            "id": str(order.get("id") or fill.get("orderID") or payload.get("id") or ""),
            "symbol": instrument,
            "side": str(side).lower(),
            "type": order_type,
            "status": status,
            "amount": abs(units_float),
            "filled": filled_float,
            "price": price_float,
            "raw": payload,
        }

    # ===============================
    # CONNECT
    # ===============================

    async def connect(self):
        if self._connected:
            return True

        self.session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def close(self):
        if self.session is not None:
            await self.session.close()
        self.session = None
        self._connected = False

    # ===============================
    # MARKET DATA
    # ===============================

    async def fetch_ticker(self, symbol):
        instrument = self._normalize_symbol(symbol)
        payload = await self._request(
            "GET",
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": instrument},
        )
        entry = self._extract_price_entry(payload, instrument)
        bids = entry.get("bids", [])
        asks = entry.get("asks", [])
        bid = float(bids[0]["price"]) if bids else None
        ask = float(asks[0]["price"]) if asks else None

        return {
            "symbol": instrument,
            "bid": bid,
            "ask": ask,
            "last": ask or bid,
            "raw": entry,
        }

    async def fetch_orderbook(self, symbol, limit=50):
        ticker = await self.fetch_ticker(symbol)
        bids = []
        asks = []

        raw = ticker.get("raw", {})
        for level in raw.get("bids", [])[:limit]:
            bids.append([float(level["price"]), float(level.get("liquidity", 0) or 0)])
        for level in raw.get("asks", [])[:limit]:
            asks.append([float(level["price"]), float(level.get("liquidity", 0) or 0)])

        return {"symbol": self._normalize_symbol(symbol), "bids": bids, "asks": asks}

    async def fetch_ohlcv(self, symbol, timeframe="H1", limit=100):
        instrument = self._normalize_symbol(symbol)
        granularity = self._normalize_granularity(timeframe)
        requested = max(1, int(limit or 100))
        collected = []
        seen_times = set()
        cursor_to = None
        previous_oldest = None

        while len(collected) < requested:
            batch_size = min(requested - len(collected), self.MAX_OHLCV_COUNT)
            params = {"granularity": granularity, "count": batch_size, "price": "M"}
            if cursor_to:
                params["to"] = cursor_to

            payload = await self._request(
                "GET",
                f"/v3/instruments/{instrument}/candles",
                params=params,
            )

            batch = []
            for candle in payload.get("candles", []):
                mid = candle.get("mid", {})
                if not candle.get("complete"):
                    continue
                timestamp = candle.get("time")
                if not timestamp:
                    continue
                batch.append(
                    [
                        timestamp,
                        float(mid.get("o", 0) or 0),
                        float(mid.get("h", 0) or 0),
                        float(mid.get("l", 0) or 0),
                        float(mid.get("c", 0) or 0),
                        float(candle.get("volume", 0) or 0),
                    ]
                )

            if not batch:
                break

            batch.sort(key=lambda row: row[0])
            oldest_time = batch[0][0]

            new_rows = 0
            for row in batch:
                if row[0] in seen_times:
                    continue
                seen_times.add(row[0])
                collected.append(row)
                new_rows += 1

            collected.sort(key=lambda row: row[0])

            if len(collected) >= requested:
                break
            if len(batch) < batch_size:
                break
            if new_rows == 0 or oldest_time == previous_oldest:
                break

            previous_oldest = oldest_time
            cursor_to = oldest_time

        return collected[-requested:]

    async def fetch_trades(self, symbol=None, limit=None):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/trades")
        trades = payload.get("trades", [])
        target = self._normalize_symbol(symbol) if symbol else None
        filtered = [trade for trade in trades if target is None or trade.get("instrument") == target]
        return filtered[:limit] if limit else filtered

    async def fetch_symbol(self):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/instruments")
        instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
        self._instrument_details = {
            item.get("name"): item
            for item in instruments
            if isinstance(item, dict) and item.get("name")
        }
        return [item.get("name") for item in instruments if item.get("name")]

    async def fetch_symbols(self):
        return await self.fetch_symbol()

    async def fetch_status(self):
        try:
            await self._request("GET", f"/v3/accounts/{self.account_id}/summary")
            return {"status": "ok", "broker": "oanda"}
        except Exception as exc:
            return {"status": "error", "broker": "oanda", "detail": str(exc)}

    # ===============================
    # ORDERS / ACCOUNT
    # ===============================

    async def fetch_balance(self):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/summary")
        account = payload.get("account", {})
        currency = account.get("currency", "USD")
        balance = float(account.get("balance", 0) or 0)
        margin_used = float(account.get("marginUsed", 0) or 0)
        return {
            "free": {currency: balance - margin_used},
            "used": {currency: margin_used},
            "total": {currency: balance},
            "equity": float(account.get("NAV", balance) or balance),
            "currency": currency,
            "raw": account,
        }

    async def fetch_positions(self, symbols=None):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/openPositions")
        positions = payload.get("positions", [])
        targets = {self._normalize_symbol(symbol) for symbol in (symbols or [])}
        normalized = []
        for position in positions:
            instrument = position.get("instrument")
            if targets and instrument not in targets:
                continue
            long_units = float(position.get("long", {}).get("units", 0) or 0)
            short_units = float(position.get("short", {}).get("units", 0) or 0)
            units = long_units if long_units else -short_units
            normalized.append(
                {
                    "symbol": instrument,
                    "amount": abs(units),
                    "side": "long" if units >= 0 else "short",
                    "entry_price": float(position.get("long", {}).get("averagePrice", 0) or position.get("short", {}).get("averagePrice", 0) or 0),
                    "raw": position,
                }
            )
        return normalized

    async def fetch_orders(self, symbol=None, limit=None):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/orders")
        orders = payload.get("orders", [])
        target = self._normalize_symbol(symbol) if symbol else None
        filtered = [
            self._normalize_order_payload({"order": order}, fallback_symbol=symbol)
            for order in orders
            if target is None or order.get("instrument") == target
        ]
        return filtered[:limit] if limit else filtered

    async def fetch_open_orders(self, symbol=None, limit=None):
        orders = await self.fetch_orders(symbol=symbol, limit=limit)
        return [order for order in orders if order.get("status") in {"open", "pending"}]

    async def fetch_closed_orders(self, symbol=None, limit=None):
        orders = await self.fetch_orders(symbol=symbol, limit=limit)
        return [order for order in orders if order.get("status") in {"filled", "canceled", "rejected"}]

    async def fetch_order(self, order_id, symbol=None):
        payload = await self._request("GET", f"/v3/accounts/{self.account_id}/orders/{order_id}")
        order = payload.get("order", payload)
        normalized = self._normalize_order_payload({"order": order}, fallback_symbol=symbol)
        if symbol is None:
            return normalized
        return normalized if normalized.get("symbol") == self._normalize_symbol(symbol) else None

    async def create_order(self, symbol, side, amount, type="market", price=None, params=None, stop_loss=None, take_profit=None):
        instrument = self._normalize_symbol(symbol)
        order_type = str(type).upper()
        meta = await self._get_instrument_meta(symbol)
        units = float(amount)
        if str(side).lower() == "sell":
            units = -abs(units)
        else:
            units = abs(units)

        units_precision = int(meta.get("tradeUnitsPrecision", 0) or 0)
        minimum_trade_size = float(meta.get("minimumTradeSize", 1) or 1)
        if abs(units) < minimum_trade_size:
            units = minimum_trade_size if units >= 0 else -minimum_trade_size

        order = {
            "instrument": instrument,
            "units": self._format_units(units, units_precision),
            "type": order_type,
            "positionFill": "DEFAULT",
        }

        extra = dict(params or {})
        if order_type == "MARKET":
            order["timeInForce"] = str(extra.pop("timeInForce", "FOK")).upper()
        else:
            order["timeInForce"] = str(extra.pop("timeInForce", "GTC")).upper()
            if price is None or float(price) <= 0:
                raise ValueError("Limit orders require a positive price")
            display_precision = int(meta.get("displayPrecision", 5) or 5)
            order["price"] = self._format_price(price, display_precision)

        stop_loss = extra.pop("stop_loss", stop_loss)
        take_profit = extra.pop("take_profit", take_profit)
        if stop_loss is not None:
            order["stopLossOnFill"] = {"price": self._format_price(stop_loss, int(meta.get("displayPrecision", 5) or 5))}
        if take_profit is not None:
            order["takeProfitOnFill"] = {"price": self._format_price(take_profit, int(meta.get("displayPrecision", 5) or 5))}
        order.update(extra)

        payload = await self._request(
            "POST",
            f"/v3/accounts/{self.account_id}/orders",
            payload={"order": order},
        )
        return self._normalize_order_payload(
            payload,
            fallback_symbol=symbol,
            fallback_side=side,
            fallback_type=type,
            fallback_amount=amount,
            fallback_price=price,
        )

    async def cancel_order(self, order_id, symbol=None):
        payload = await self._request(
            "PUT",
            f"/v3/accounts/{self.account_id}/orders/{order_id}/cancel",
        )
        normalized = self._normalize_order_payload(payload, fallback_symbol=symbol)
        normalized["id"] = str(order_id)
        normalized["status"] = "canceled"
        return normalized

    async def cancel_all_orders(self, symbol=None):
        orders = await self.fetch_open_orders(symbol=symbol)
        canceled = []
        for order in orders:
            order_id = order.get("id")
            if order_id:
                canceled.append(await self.cancel_order(order_id, symbol=symbol))
        return canceled
