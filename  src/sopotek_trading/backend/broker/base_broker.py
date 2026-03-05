from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any


class BaseBroker(ABC):

    def __init__(self):
        self._connected: bool = False

    # =========================================================
    # CONNECTION
    # =========================================================

    @abstractmethod
    async def connect(self) -> bool:
        """Establish broker connection"""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Release all resources (important for async clients like ccxt)"""
        raise NotImplementedError

    @abstractmethod
    async def ping(self) -> bool:
        """Health check"""
        raise NotImplementedError

    def is_connected(self) -> bool:
        return self._connected

    # =========================================================
    # MARKET DATA
    # =========================================================

    @abstractmethod
    async def fetch_symbols(self) -> List[str]:
        """Return list of tradable symbols"""
        raise NotImplementedError

    @abstractmethod
    async def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            limit: int = 500
    ) -> List[List[Any]]:
        """
        Return OHLCV candles
        format:
        [
            [timestamp, open, high, low, close, volume],
            ...
        ]
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict:
        """Return ticker information"""
        raise NotImplementedError

    async def fetch_price(self, symbol: str) -> float:
        """Helper function used by trading engines"""
        ticker = await self.fetch_ticker(symbol)
        return ticker.get("last")

    # =========================================================
    # ACCOUNT
    # =========================================================

    @abstractmethod
    async def fetch_balance(self) -> Dict:
        """
        Expected format:
        {
            equity: float,
            free: float,
            used: float,
            currency: str
        }
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch_positions(self) -> List[Dict]:
        """Return open positions"""
        raise NotImplementedError

    @abstractmethod
    async def fetch_open_orders(self) -> List[Dict]:
        """Return open orders"""
        raise NotImplementedError

    async def fetch_fees(self) -> Dict:
        """
        Optional method (not supported by all brokers)
        """
        return {}

    # =========================================================
    # TRADING
    # =========================================================

    @abstractmethod
    async def create_order(
            self,
            symbol: str,
            side: str,
            order_type: str,
            amount: float,
            price: Optional[float] = None,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            slippage: Optional[float] = None
    ) -> Dict:
        """
        Create order
        """
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Dict:
        """Cancel a specific order"""
        raise NotImplementedError

    @abstractmethod
    async def cancel_all_orders(self) -> None:
        """Cancel all open orders"""
        raise NotImplementedError

    @abstractmethod
    async def fetch_order(self, order_id: str) -> Dict:
        """Return order details"""
        raise NotImplementedError