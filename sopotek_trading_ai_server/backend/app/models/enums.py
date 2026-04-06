from __future__ import annotations

from enum import Enum


class UserRole(str, Enum):
    ADMIN = "admin"
    TRADER = "trader"
    VIEWER = "viewer"


class StrategyStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    PAUSED = "paused"


class OrderStatus(str, Enum):
    PENDING = "pending"
    WORKING = "working"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELED = "canceled"
    REJECTED = "rejected"


class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
