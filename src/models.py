from sqlalchemy import Integer, String, Float, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class WeatherReading(Base):
    __tablename__ = "weather_readings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(80), index=True, nullable=False)
    temperature_c: Mapped[float] = mapped_column(Float, nullable=False)
    humidity_percent: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp_unix: Mapped[int] = mapped_column(BigInteger, nullable=False)
