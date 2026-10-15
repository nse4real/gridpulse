"""Streams UK weather observations/forecasts from Open-Meteo.

No API key required (10,000 calls/day free tier). Temperature drives demand,
wind speed at 100m drives wind farm output, shortwave radiation drives solar --
these are the three weather variables that move UK power prices. Location
defaults to central England; override with OPEN_METEO_LAT / OPEN_METEO_LON.
"""

import requests
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "weather"
API_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = os.getenv("OPEN_METEO_LAT", "52.5")
LONGITUDE = os.getenv("OPEN_METEO_LON", "-1.5")
LOCATION = os.getenv("OPEN_METEO_LOCATION", "central-england")
POLL_SECONDS = 900  # hourly data; 15-min poll is plenty


def fetch_weather():
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,wind_speed_100m,shortwave_radiation",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = requests.get(API_URL, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    while True:
        try:
            data = fetch_weather()
            hourly = data.get("hourly", {})
            timestamps = hourly.get("time", [])
            for i, ts in enumerate(timestamps):
                message = {
                    "timestamp": ts,
                    "location": LOCATION,
                    "temperature_2m": hourly.get("temperature_2m", [None])[i],
                    "wind_speed_100m": hourly.get("wind_speed_100m", [None])[i],
                    "shortwave_radiation": hourly.get("shortwave_radiation", [None])[i],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.produce(
                    TOPIC,
                    key=f"{LOCATION}-{ts}",
                    value=json.dumps(message),
                )
            producer.flush()
            print(f"Produced {len(timestamps)} weather records "
                  f"at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
