"""Streams NESO's de-rated margin and loss of load probability forecasts.

Elexon BMRS dataset LOLPDRM: for each settlement period, NESO forecasts the
de-rated margin (spare capacity after discounting units by expected
availability) and the loss of load probability at 1, 2, 4, 8 and 12 hours
ahead. It feeds the margin component of the market stress index.
"""

import requests
import json
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "derated-margin"
ELEXON_URL = "https://data.elexon.co.uk/bmrs/api/v1/forecast/system/loss-of-load"


def fetch_window(start, end):
    fmt = "%Y-%m-%dT%H:%MZ"
    params = {"from": start.strftime(fmt), "to": end.strftime(fmt), "format": "json"}
    response = requests.get(ELEXON_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json().get("data", [])


def to_message(record):
    return {
        "settlement_date": record.get("settlementDate"),
        "settlement_period": record.get("settlementPeriod"),
        "start_time": record.get("startTime"),
        "forecast_horizon": record.get("forecastHorizon"),
        "derated_margin_mw": record.get("deratedMargin"),
        "loss_of_load_probability": record.get("lossOfLoadProbability"),
        "publish_time": record.get("publishTime"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def message_key(message):
    return (f"{message['settlement_date']}-{message['settlement_period']}"
            f"-h{message['forecast_horizon']}")


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    while True:
        try:
            # The last hour plus the next 12 covers every horizon in flight.
            now = datetime.now(timezone.utc)
            records = fetch_window(now - timedelta(hours=1), now + timedelta(hours=13))

            for record in records:
                message = to_message(record)
                producer.produce(
                    TOPIC,
                    key=message_key(message),
                    value=json.dumps(message),
                    callback=delivery_report
                )

            producer.flush()
            print(f"Produced {len(records)} records at {datetime.now(timezone.utc).isoformat()}")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(300)


if __name__ == "__main__":
    main()
