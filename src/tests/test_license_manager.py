from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from licensing.license_manager import LicenseManager


class DictSettings:
    def __init__(self):
        self.data = {}

    def value(self, key, default=""):
        return self.data.get(key, default)

    def setValue(self, key, value):
        self.data[key] = value

    def remove(self, key):
        self.data.pop(key, None)


def test_trial_starts_automatically_and_unlocks_live_trading():
    settings = DictSettings()
    manager = LicenseManager(settings)

    status = manager.status()

    assert status["tier"] == "trial"
    assert manager.allows_feature("live_trading") is True
    assert "license/trial_started_at" in settings.data


def test_subscription_key_activates_with_duration():
    settings = DictSettings()
    manager = LicenseManager(settings)

    success, message, status = manager.activate_key("SOPOTEK-SUB-12M-TEAM-001")

    assert success is True
    assert "Subscription" in message
    assert status["tier"] == "subscription"
    assert status["is_premium"] is True
    assert status["days_remaining"] is not None
    assert status["days_remaining"] > 300


def test_full_license_is_perpetual():
    settings = DictSettings()
    manager = LicenseManager(settings)

    success, _message, status = manager.activate_key("SOPOTEK-FULL-LIFETIME-001")

    assert success is True
    assert status["tier"] == "full"
    assert status["days_remaining"] is None
    assert manager.allows_feature("live_trading") is True


def test_expired_trial_falls_back_to_community():
    settings = DictSettings()
    settings.setValue(
        "license/trial_started_at",
        (datetime.now(timezone.utc) - timedelta(days=LicenseManager.TRIAL_DAYS + 3)).isoformat(),
    )
    manager = LicenseManager(settings)

    status = manager.status()

    assert status["tier"] == "community"
    assert manager.allows_feature("live_trading") is False
