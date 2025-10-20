from dotenv import load_dotenv
load_dotenv()

from src.db import engine, Base, SessionLocal
from src.models import WeatherReading

# Create tables
Base.metadata.create_all(bind=engine)

# Insert one row as a test
with SessionLocal() as s:
    s.add(WeatherReading(
        city="Dallas",
        temperature_c=28.1,
        humidity_percent=46,
        timestamp_unix=1760316876
    ))
    s.commit()

print("SQLite DB initialized and one row inserted")
