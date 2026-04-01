class EventType:
    """Defines the set of event type identifiers used across the trading system.
    Provides a centralized list of string constants to categorize and route events.

    The event types represent key stages of the trading lifecycle such as market
    data ingestion, signal generation, risk checks, order handling, and execution
    reporting. Use these constants to ensure consistent event naming and to avoid
    hard-coded string literals throughout the codebase."""
    
    MARKET_DATA = "MARKET_DATA"
    MARKET_DATA_EVENT = "MARKET_DATA_EVENT"
    MARKET_TICK = "MARKET_TICK"
    CANDLE = "CANDLE"
    ORDER_BOOK = "ORDER_BOOK"
    SIGNAL = "SIGNAL"
    ANALYST_INSIGHT = "ANALYST_INSIGHT"
    STRATEGY_SELECTION = "STRATEGY_SELECTION"
    REASONING_DECISION = "REASONING_DECISION"
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    RISK_ALERT = "RISK_ALERT"
    ORDER_REQUEST = "ORDER_REQUEST"
    ORDER = "ORDER"
    ORDER_EVENT = "ORDER_EVENT"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_FILLED = "ORDER_FILLED"
    EXECUTION_REPORT = "EXECUTION_REPORT"
    FILL = "FILL"
    PAPER_TRADE_EVENT = "PAPER_TRADE_EVENT"
    PAPER_TRADE_RECORDED = "PAPER_TRADE_RECORDED"
    PAPER_DATASET_READY = "PAPER_DATASET_READY"
    POSITION = "POSITION"
    POSITION_EVENT = "POSITION_EVENT"
    REGIME = "REGIME"
    ACCOUNT_EVENT = "ACCOUNT_EVENT"
    PORTFOLIO_SNAPSHOT = "PORTFOLIO_SNAPSHOT"
    EXECUTION_PLAN = "EXECUTION_PLAN"
