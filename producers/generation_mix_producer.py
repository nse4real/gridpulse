"""Streams GB generation mix by fuel type from the NESO Carbon Intensity API.

No API key required. Returns the share of demand met by each fuel (gas, wind,
nuclear, solar, imports, ...) for the current half-hour settlement period.
Each fuel is produced as its own message so the consumer can upsert per fuel.
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
TOPIC = "generation-mix"
API_URL = "https://api.carbonintensity.org.uk/generation"
POLL_SECONDS = 300


def fetch_generation_mix():
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    return response.json()


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    while True:
        try:
            data = fetch_generation_mix().get("data", {})
            mix = data.get("generationmix", [])
            for entry in mix:
                message = {
                    "from": data.get("from"),
                    "to": data.get("to"),
                    "fuel": entry.get("fuel"),
                    "percentage": entry.get("perc"),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.produce(
                    TOPIC,
                    key=f"{data.get('from')}-{entry.get('fuel')}",
                    value=json.dumps(message),
                )
            producer.flush()
            print(f"Produced {len(mix)} generation mix records "
                  f"at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
