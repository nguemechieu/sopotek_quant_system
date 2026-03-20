import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engines.risk_engine import RiskEngine


def test_position_size():
    risk = RiskEngine(account_equity=10000)

    size = risk.position_size(
        entry_price=100,
        stop_price=95,
    )

    assert size > 0


def test_trade_validation():
    risk = RiskEngine(account_equity=10000, max_position_size_pct=0.5)

    approved, msg = risk.validate_trade(
        price=100,
        quantity=1,
    )

    assert approved is True
    assert msg == "Approved"


def test_adjust_trade_scales_large_position_down_to_cap():
    risk = RiskEngine(account_equity=10000, max_position_size_pct=0.1)

    approved, adjusted_quantity, reason = risk.adjust_trade(
        price=100,
        quantity=25,
    )

    assert approved is True
    assert adjusted_quantity == 10
    assert "reduced" in reason.lower()


def test_position_size_is_capped_by_max_position_size():
    risk = RiskEngine(account_equity=10000, max_risk_per_trade=0.02, max_position_size_pct=0.05)

    size = risk.position_size(
        entry_price=100,
        stop_price=95,
    )

    assert size == 5
