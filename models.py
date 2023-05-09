from sqlalchemy import Column, Integer, String, Float, BigInteger 
from db import Base
from pydantic import BaseModel
import datetime

class Klines(Base):
    """Database model"""
    __tablename__ = "klines"

    symbol = Column(String, primary_key=True, index=True)
    timeframe = Column(String, primary_key=True, index=True)
    open_time = Column(BigInteger, primary_key=True, index=True)
    close_time = Column(BigInteger, index=True)
    open = Column(Float(precision=24))
    high = Column(Float(precision=24))
    low = Column(Float(precision=24))
    close = Column(Float(precision=24))
    volume = Column(Float(precision=24))
    trades = Column(Integer)

class KlinesModel(BaseModel):
    """Pydantic model for FastAPI"""
    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
