from pydantic import BaseModel


class KlinesModel(BaseModel):
    """Pydantic model for FastAPI"""
    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    trades: int
