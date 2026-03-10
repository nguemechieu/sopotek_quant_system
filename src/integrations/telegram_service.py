import asyncio
import os
from datetime import datetime

import aiohttp


class TelegramService:
    def __init__(self, controller, logger, bot_token, chat_id=None, enabled=False):
        self.controller = controller
        self.logger = logger
        self.bot_token = str(bot_token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.enabled = bool(enabled and self.bot_token)
        self._offset = 0
        self._poll_task = None
        self._session = None
        self._running = False

    @property
    def base_url(self):
        return f"https://api.telegram.org/bot{self.bot_token}"

    def is_configured(self):
        return bool(self.bot_token)

    def can_send(self):
        return bool(self.bot_token and self.chat_id)

    async def start(self):
        if not self.enabled or not self.bot_token:
            return
        if self._poll_task and not self._poll_task.done():
            return
        self._running = True
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))
        self._poll_task = asyncio.create_task(self._poll_loop(), name="telegram_poll")
        if self.can_send():
            await self.send_message("Telegram integration connected. Use /help for commands.")

    async def stop(self):
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def notify_trade(self, trade):
        if not self.can_send() or not isinstance(trade, dict):
            return

        symbol = str(trade.get("symbol") or "-")
        side = str(trade.get("side") or "-").upper()
        status = str(trade.get("status") or "-").upper()
        price = trade.get("price", "-")
        size = trade.get("size", trade.get("amount", "-"))
        pnl = trade.get("pnl", "-")
        order_id = trade.get("order_id", trade.get("id", "-"))
        timestamp = trade.get("timestamp") or datetime.utcnow().isoformat()
        message = (
            "<b>Trading Activity</b>\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Side: <b>{side}</b>\n"
            f"Status: <b>{status}</b>\n"
            f"Price: <code>{price}</code>\n"
            f"Size: <code>{size}</code>\n"
            f"PnL: <code>{pnl}</code>\n"
            f"Order ID: <code>{order_id}</code>\n"
            f"Time: <code>{timestamp}</code>"
        )
        await self.send_message(message)

    async def send_message(self, text):
        if not self.can_send():
            return False
        try:
            await self._ensure_session()
            async with self._session.post(
                f"{self.base_url}/sendMessage",
                data={
                    "chat_id": self.chat_id,
                    "text": str(text or ""),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
            ) as response:
                payload = await response.json(content_type=None)
                return bool(payload.get("ok"))
        except Exception as exc:
            self.logger.debug("Telegram send_message failed: %s", exc)
            return False

    async def send_photo(self, file_path, caption=None):
        if not self.can_send() or not file_path or not os.path.exists(file_path):
            return False
        try:
            await self._ensure_session()
            data = aiohttp.FormData()
            data.add_field("chat_id", self.chat_id)
            if caption:
                data.add_field("caption", str(caption))
            with open(file_path, "rb") as handle:
                data.add_field(
                    "photo",
                    handle,
                    filename=os.path.basename(file_path),
                    content_type="image/png",
                )
                async with self._session.post(f"{self.base_url}/sendPhoto", data=data) as response:
                    payload = await response.json(content_type=None)
                    return bool(payload.get("ok"))
        except Exception as exc:
            self.logger.debug("Telegram send_photo failed: %s", exc)
            return False

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))

    async def _poll_loop(self):
        while self._running:
            try:
                updates = await self._get_updates()
                for update in updates:
                    await self._handle_update(update)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.debug("Telegram polling error: %s", exc)
                await asyncio.sleep(3)

    async def _get_updates(self):
        await self._ensure_session()
        params = {"timeout": 30, "offset": self._offset + 1}
        async with self._session.get(f"{self.base_url}/getUpdates", params=params) as response:
            payload = await response.json(content_type=None)
        if not payload.get("ok"):
            return []
        return payload.get("result", []) or []

    async def _handle_update(self, update):
        update_id = int(update.get("update_id", self._offset) or self._offset)
        self._offset = max(self._offset, update_id)
        message = update.get("message") or update.get("edited_message") or {}
        text = str(message.get("text") or "").strip()
        chat = message.get("chat") or {}
        incoming_chat_id = str(chat.get("id") or "").strip()
        if not text or not incoming_chat_id:
            return
        if self.chat_id and incoming_chat_id != self.chat_id:
            return

        lowered = text.lower()
        if lowered in {"/start", "/help"}:
            await self.send_message(self._help_text())
            return
        if lowered == "/status":
            await self.send_message(await self.controller.telegram_status_text())
            return
        if lowered == "/balances":
            await self.send_message(await self.controller.telegram_balances_text())
            return
        if lowered == "/positions":
            await self.send_message(await self.controller.telegram_positions_text())
            return
        if lowered in {"/orders", "/openorders"}:
            await self.send_message(await self.controller.telegram_open_orders_text())
            return
        if lowered == "/screenshot":
            screenshot_path = await self.controller.capture_telegram_screenshot()
            if screenshot_path:
                sent = await self.send_photo(screenshot_path, caption="Sopotek terminal screenshot")
                if not sent:
                    await self.send_message(f"Screenshot captured but could not be uploaded: <code>{screenshot_path}</code>")
            else:
                await self.send_message("Unable to capture a screenshot right now.")
            return
        if lowered.startswith("/ask ") or lowered.startswith("/chat "):
            question = text.split(" ", 1)[1].strip() if " " in text else ""
            if not question:
                await self.send_message("Send a question after /ask or /chat.")
                return
            answer = await self.controller.ask_openai_about_app(question)
            await self.send_message(answer)
            return

        await self.send_message(self._help_text())

    def _help_text(self):
        return (
            "<b>Sopotek Telegram Commands</b>\n"
            "/status - trading status and AI scope\n"
            "/balances - account balances\n"
            "/positions - open positions\n"
            "/orders - open exchange orders\n"
            "/screenshot - terminal screenshot\n"
            "/ask &lt;question&gt; - ask OpenAI about the app or current market context\n"
            "/help - show this message"
        )
