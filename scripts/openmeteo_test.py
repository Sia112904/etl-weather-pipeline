#!/usr/bin/env python3
# openmeteo_test.py -- geocode a city, fetch hourly temperature, print nearest-hour temp
import requests
from datetime import datetime

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

def geocode(place: str):
    params = {"name": place, "count": 1}
    r = requests.get(GEOCODE_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    results = data.get("results")
    if not results:
        raise SystemExit(f"No geocoding results for '{place}'")
    found = results[0]
    return float(found["latitude"]), float(found["longitude"]), found.get("timezone", "UTC")

def get_hourly_temp(lat, lon, timezone="UTC"):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "forecast_days": 1,
        "timezone": timezone
    }
    r = requests.get(FORECAST_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def nearest_hour_temp(forecast_json):
    times = forecast_json["hourly"]["time"]
    temps = forecast_json["hourly"]["temperature_2m"]
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    now_str = now.strftime("%Y-%m-%dT%H:%M")
    if now_str in times:
        idx = times.index(now_str)
        return temps[idx], times[idx]
    # fallback: pick closest time
    from datetime import datetime as dt
    parsed = [dt.strptime(t, "%Y-%m-%dT%H:%M") for t in times]
    closest = min(range(len(parsed)), key=lambda i: abs(parsed[i] - now))
    return temps[closest], times[closest]

if __name__ == "__main__":
    city = "Dallas"  # <<< change this to any city
    print("Geocoding...", city)
    lat, lon, tz = geocode(city)
    print(f" -> {city}: lat={lat}, lon={lon}, timezone={tz}")
    print("Requesting hourly temperature forecast...")
    forecast = get_hourly_temp(lat, lon, timezone="UTC")
    temp, timestamp = nearest_hour_temp(forecast)
    print(f"Nearest hour (UTC): {timestamp}  temperature_2m = {temp} °C")
    print("\nSample (first 5 hourly rows):")
    for t, v in zip(forecast["hourly"]["time"][:5], forecast["hourly"]["temperature_2m"][:5]):
        print(t, v)
