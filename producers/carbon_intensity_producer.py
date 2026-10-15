"""Streams GB carbon intensity from the NESO Carbon Intensity API.

No API key required. The grid's carbon intensity is published per half-hour
settlement period with both a forecast and (once known) an actual value.
Polls every 5 minutes; the consumer deduplicates on the settlement window.
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
TOPIC = "carbon-intensity"
API_URL = "https://api.carbonintensity.org.uk/intensity"
POLL_SECONDS = 300  # data updates half-hourly; 5-min poll catches revisions fast


def fetch_carbon_intensity():
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    return response.json()


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    while True:
        try:
            data = fetch_carbon_intensity()
            for record in data.get("data", []):
                intensity = record.get("intensity", {})
                message = {
                    "from": record.get("from"),
                    "to": record.get("to"),
                    "intensity_forecast": intensity.get("forecast"),
                    "intensity_actual": intensity.get("actual"),
                    "intensity_index": intensity.get("index"),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.produce(
                    TOPIC,
                    key=record.get("from"),
                    value=json.dumps(message),
                )
            producer.flush()
            print(f"Produced {len(data.get('data', []))} carbon intensity records "
                  f"at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
