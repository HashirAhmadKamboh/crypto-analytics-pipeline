from sqlalchemy import Column, String, Float, DateTime, Integer
from database import Base
import datetime

class CryptoTick(Base):
    __tablename__ = "crypto_ticks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)          # e.g., "BTCUSDT"
    price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)