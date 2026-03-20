from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone


class LicenseManager:
    TRIAL_DAYS = 14
    PREMIUM_FEATURES = {"live_trading"}

    def __init__(self, settings, logger=None):
        self.settings = settings
        self.logger = logger
        self._ensure_trial_started()

    def _now(self):
        return datetime.now(timezone.utc)

    def _iso(self, value):
        if value is None:
            return ""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        return str(value)

    def _parse_datetime(self, value):
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _setting(self, key, default=""):
        try:
            return self.settings.value(key, default)
        except Exception:
            return default

    def _set(self, key, value):
        try:
            self.settings.setValue(key, value)
        except Exception:
            pass

    def _remove(self, key):
        try:
            self.settings.remove(key)
        except Exception:
            pass

    def _ensure_trial_started(self):
        started = self._parse_datetime(self._setting("license/trial_started_at", ""))
        if started is not None:
            return
        self._set("license/trial_started_at", self._iso(self._now()))

    def _normalize_key(self, key):
        cleaned = str(key or "").upper().strip()
        cleaned = re.sub(r"[^A-Z0-9\-]", "", cleaned)
        cleaned = re.sub(r"\-+", "-", cleaned)
        return cleaned

    def _subscription_duration(self, key):
        match = re.search(r"\b(\d+)([DMY])\b", key)
        if not match:
            return timedelta(days=365)
        value = max(1, int(match.group(1)))
        unit = match.group(2)
        if unit == "D":
            return timedelta(days=value)
        if unit == "Y":
            return timedelta(days=value * 365)
        return timedelta(days=value * 30)

    def activate_key(self, raw_key):
        key = self._normalize_key(raw_key)
        if not key.startswith("SOPOTEK-"):
            return False, "License key must start with SOPOTEK-.", self.status()

        now = self._now()
        if key.startswith("SOPOTEK-FULL-") or key.startswith("SOPOTEK-PERP-"):
            self._set("license/key", key)
            self._set("license/type", "perpetual")
            self._set("license/plan_name", "Full License")
            self._set("license/activated_at", self._iso(now))
            self._remove("license/expires_at")
            return True, "Full license activated.", self.status()

        if key.startswith("SOPOTEK-SUB-"):
            duration = self._subscription_duration(key)
            expires_at = now + duration
            self._set("license/key", key)
            self._set("license/type", "subscription")
            self._set("license/plan_name", "Subscription")
            self._set("license/activated_at", self._iso(now))
            self._set("license/expires_at", self._iso(expires_at))
            return True, "Subscription activated.", self.status()

        return False, "License key format was not recognized.", self.status()

    def clear_paid_license(self):
        for key in (
            "license/key",
            "license/type",
            "license/plan_name",
            "license/activated_at",
            "license/expires_at",
        ):
            self._remove(key)
        return self.status()

    def status(self):
        self._ensure_trial_started()
        now = self._now()
        trial_started = self._parse_datetime(self._setting("license/trial_started_at", ""))
        trial_expires = trial_started + timedelta(days=self.TRIAL_DAYS) if trial_started else None
        trial_days_left = None
        if trial_expires is not None:
            trial_days_left = max(0, (trial_expires - now).days)

        license_type = str(self._setting("license/type", "") or "").strip().lower()
        plan_name = str(self._setting("license/plan_name", "") or "").strip()
        expires_at = self._parse_datetime(self._setting("license/expires_at", ""))

        if license_type == "perpetual":
            return {
                "tier": "full",
                "state": "active",
                "plan_name": plan_name or "Full License",
                "badge": "FULL",
                "summary": "Full license active",
                "description": "Perpetual license with live trading unlocked.",
                "days_remaining": None,
                "expires_at": None,
                "is_premium": True,
            }

        if license_type == "subscription" and expires_at is not None and expires_at > now:
            days_remaining = max(0, (expires_at - now).days)
            return {
                "tier": "subscription",
                "state": "active",
                "plan_name": plan_name or "Subscription",
                "badge": "SUB",
                "summary": f"Subscription active ({days_remaining}d left)",
                "description": f"Subscription active until {expires_at.date().isoformat()}.",
                "days_remaining": days_remaining,
                "expires_at": expires_at,
                "is_premium": True,
            }

        if trial_expires is not None and trial_expires > now:
            return {
                "tier": "trial",
                "state": "trial",
                "plan_name": "Trial",
                "badge": "TRIAL",
                "summary": f"Trial active ({trial_days_left}d left)",
                "description": f"Trial ends on {trial_expires.date().isoformat()} and includes live trading.",
                "days_remaining": trial_days_left,
                "expires_at": trial_expires,
                "is_premium": True,
            }

        return {
            "tier": "community",
            "state": "community",
            "plan_name": "Community",
            "badge": "FREE",
            "summary": "Community mode",
            "description": "Paper trading, charts, and research remain available. Live trading requires Trial, Subscription, or Full License.",
            "days_remaining": 0,
            "expires_at": trial_expires,
            "is_premium": False,
        }

    def allows_feature(self, feature):
        feature_name = str(feature or "").strip().lower()
        status = self.status()
        if feature_name in self.PREMIUM_FEATURES:
            return bool(status.get("is_premium"))
        return True

    def feature_message(self, feature):
        feature_name = str(feature or "").strip().lower().replace("_", " ")
        status = self.status()
        if self.allows_feature(feature):
            return f"{feature_name.title()} is available under {status.get('plan_name', 'the current license')}."
        return (
            f"{feature_name.title()} requires Trial, Subscription, or Full License. "
            "Community mode remains available for paper trading and analysis."
        )
