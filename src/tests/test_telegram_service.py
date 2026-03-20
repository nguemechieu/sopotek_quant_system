import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations.telegram_service import TelegramService


class DummyLogger:
    def debug(self, *args, **kwargs):
        return None


class DummyController:
    def __init__(self):
        self.open_chart_calls = []
        self.chart_capture_calls = []
        self.ask_calls = []
        self.direct_actions = []

    async def telegram_status_text(self):
        return "status"

    def telegram_management_text(self):
        return "management"

    async def telegram_balances_text(self):
        return "balances"

    async def telegram_positions_text(self):
        return "positions"

    async def telegram_open_orders_text(self):
        return "orders"

    async def telegram_recommendations_text(self):
        return "recommendations"

    async def telegram_performance_text(self):
        return "performance"

    async def market_chat_trade_history_summary(self, limit=300, open_window=True):
        return f"history:{limit}:{open_window}"

    async def telegram_position_analysis_text(self, open_window=True):
        return f"analysis:{open_window}"

    async def capture_telegram_screenshot(self):
        return None

    async def ask_openai_about_app(self, question, conversation=None):
        self.ask_calls.append((question, list(conversation or [])))
        return f"answer:{question}"

    async def handle_market_chat_action(self, question):
        self.direct_actions.append(question)
        return f"direct:{question}"

    async def telegram_open_chart(self, symbol, timeframe=None):
        self.open_chart_calls.append((symbol, timeframe))
        return {"ok": True, "message": f"opened {symbol} {timeframe or ''}".strip()}

    async def capture_chart_screenshot(self, symbol=None, timeframe=None, prefix="chart"):
        self.chart_capture_calls.append((symbol, timeframe, prefix))
        return "output/screenshots/chart.png"


class RecordingTelegramService(TelegramService):
    def __init__(self, controller):
        super().__init__(controller=controller, logger=DummyLogger(), bot_token="token", chat_id="1", enabled=True)
        self.messages = []
        self.photos = []
        self.callback_answers = []

    async def send_message(self, text, include_keyboard=False, reply_markup=None):
        self.messages.append((str(text), bool(include_keyboard), reply_markup))
        return True

    async def send_photo(self, file_path, caption=None):
        self.photos.append((file_path, caption))
        return True

    async def _answer_callback_query(self, callback_id, text=""):
        self.callback_answers.append((callback_id, text))
        return True


def build_update(text):
    return {
        "update_id": 1,
        "message": {
            "text": text,
            "chat": {"id": "1"},
        },
    }


def build_callback_update(data, callback_id="cb-1"):
    return {
        "update_id": 2,
        "callback_query": {
            "id": callback_id,
            "data": data,
            "message": {
                "chat": {"id": "1"},
            },
        },
    }


def test_parse_chart_args_supports_symbol_and_timeframe():
    service = RecordingTelegramService(DummyController())
    parsed = service._parse_chart_args("EUR/USD 1h")

    assert parsed["symbol"] == "EUR/USD"
    assert parsed["timeframe"] == "1h"


def test_chart_command_opens_chart():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/chart BTC/USDT 15m")))

    assert controller.open_chart_calls == [("BTC/USDT", "15m")]
    assert service.messages[-1] == ("opened BTC/USDT 15m", False, None)


def test_chartshot_command_captures_and_sends_photo():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/chartshot EUR/USD 1h")))

    assert controller.chart_capture_calls == [("EUR/USD", "1h", "telegram_chart")]
    assert service.photos == [("output/screenshots/chart.png", "Sopotek chart EUR/USD (1h)")]


def test_chartshot_command_without_symbol_uses_current_chart_context():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/chartshot")))

    assert controller.chart_capture_calls == [(None, "", "telegram_chart")]
    assert service.photos == [("output/screenshots/chart.png", "Sopotek chart current chart (1h)")]


def test_help_command_requests_keyboard():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/help")))

    assert service.messages
    assert service.messages[-1][1] is True
    assert "/commands" in service.messages[-1][0]
    assert "/trade ..." not in service.messages[-1][0]
    assert "/chart SYMBOL" not in service.messages[-1][0]


def test_command_keyboard_contains_core_buttons():
    service = RecordingTelegramService(DummyController())

    keyboard = service._command_keyboard_markup()

    assert keyboard["resize_keyboard"] is True
    flat_labels = [button["text"] for row in keyboard["keyboard"] for button in row]
    assert "/status" in flat_labels
    assert "/screenshot" in flat_labels
    assert "/management" in flat_labels
    assert "/history" in flat_labels
    assert "/chartshot" in flat_labels
    assert "/settings" in flat_labels
    assert "/refreshmarkets" in flat_labels
    assert "/autotradeon" in flat_labels
    assert all("EUR/USD" not in label for label in flat_labels)
    assert all("trade buy" not in label.lower() for label in flat_labels)


def test_management_command_returns_management_summary():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/management")))

    assert service.messages[-1] == ("management", False, None)


def test_history_command_returns_trade_history_summary():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/history")))

    assert service.messages[-1] == ("history:300:True", False, None)


def test_generic_action_command_routes_to_direct_action_handler():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/settings")))

    assert controller.direct_actions[-1] == "open settings"
    assert service.messages[-1] == ("direct:open settings", False, None)


def test_plain_text_message_gets_sopotek_pilot_reply():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("What is my current status?")))

    assert controller.ask_calls
    assert controller.ask_calls[-1][0] == "What is my current status?"
    assert service.messages[-1] == ("answer:What is my current status?", False, None)


def test_trade_command_routes_to_direct_action_handler():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/trade buy EUR/USD amount 1000 confirm")))

    assert controller.direct_actions == ["trade buy EUR/USD amount 1000 confirm"]
    assert service.messages[-1] == ("direct:trade buy EUR/USD amount 1000 confirm", False, None)


def test_buy_shortcut_builds_trade_command():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/buy BTC/USDT amount 0.01 type market confirm")))

    assert controller.direct_actions == ["trade buy BTC/USDT amount 0.01 type market confirm"]
    assert service.messages[-1] == ("direct:trade buy BTC/USDT amount 0.01 type market confirm", False, None)


def test_trade_preview_includes_inline_confirmation_buttons():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/trade buy EUR/USD amount 1000")))

    assert controller.direct_actions == ["trade buy EUR/USD amount 1000"]
    message, include_keyboard, reply_markup = service.messages[-1]
    assert message == "direct:trade buy EUR/USD amount 1000"
    assert include_keyboard is False
    assert reply_markup is not None
    inline_row = reply_markup["inline_keyboard"][0]
    assert inline_row[0]["text"] == "Confirm Trade"
    assert inline_row[1]["text"] == "Cancel Trade"


def test_trade_confirm_button_executes_pending_trade():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/trade buy EUR/USD amount 1000")))
    reply_markup = service.messages[-1][2]
    callback_data = reply_markup["inline_keyboard"][0][0]["callback_data"]

    asyncio.run(service._handle_update(build_callback_update(callback_data)))

    assert controller.direct_actions[-1] == "trade buy EUR/USD amount 1000 confirm"
    assert service.callback_answers[-1] == ("cb-1", "Trade submitted.")
    assert service.messages[-1] == ("direct:trade buy EUR/USD amount 1000 confirm", False, None)


def test_trade_cancel_button_clears_pending_trade():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("/trade buy EUR/USD amount 1000")))
    reply_markup = service.messages[-1][2]
    callback_data = reply_markup["inline_keyboard"][0][1]["callback_data"]

    asyncio.run(service._handle_update(build_callback_update(callback_data, callback_id="cb-2")))

    assert service.callback_answers[-1] == ("cb-2", "Trade request canceled.")
    assert service.messages[-1] == ("Trade request canceled.", False, None)


def test_chat_history_is_passed_to_follow_up_messages():
    controller = DummyController()
    service = RecordingTelegramService(controller)

    asyncio.run(service._handle_update(build_update("First question")))
    asyncio.run(service._handle_update(build_update("Second question")))

    assert len(controller.ask_calls) == 2
    second_conversation = controller.ask_calls[1][1]
    assert second_conversation
    assert second_conversation[0]["role"] == "user"
    assert second_conversation[0]["content"] == "First question"

