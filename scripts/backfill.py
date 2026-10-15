"""Backfills recent history into Redpanda so a fresh clone has charts to look at.

The live producers only fetch "now". This script replays the last N days
(default 7) from the same free, keyless APIs and publishes them to the same
topics with the same message shapes, so the normal consumers land them --
including validation and the DLQ. Safe to re-run: consumers upsert on their
natural keys.

    python scripts/backfill.py            # last 7 days
    python scripts/backfill.py --days 3
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

from producers.derated_margin_producer import fetch_window, message_key, to_message

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
ELEXON_URL = "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices"
CARBON_URL = "https://api.carbonintensity.org.uk"
METEO_URL = "https://api.open-meteo.com/v1/forecast"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def backfill_system_prices(producer, days):
    today = datetime.now(timezone.utc).date()
    total = 0
    for offset in range(days, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        response = requests.get(f"{ELEXON_URL}/{day}", params={"format": "json"}, timeout=30)
        response.raise_for_status()
        for record in response.json().get("data", []):
            message = {
                "settlement_date": record.get("settlementDate"),
                "settlement_period": record.get("settlementPeriod"),
                "system_buy_price": record.get("systemBuyPrice"),
                "system_sell_price": record.get("systemSellPrice"),
                "net_imbalance_volume": record.get("netImbalanceVolume"),
                "ingested_at": now_iso(),
            }
            producer.produce(
                "system-prices",
                key=f"{message['settlement_date']}-{message['settlement_period']}",
                value=json.dumps(message),
            )
            total += 1
    return total


def backfill_derated_margin(producer, days):
    # BMRS caps this endpoint at 7 days per request, so walk it in 6-day chunks.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    cursor = now - timedelta(days=days)
    end = now + timedelta(hours=13)  # forecasts run up to 12 hours ahead
    total = 0
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=6), end)
        for record in fetch_window(cursor, chunk_end):
            message = to_message(record)
            producer.produce("derated-margin", key=message_key(message),
                             value=json.dumps(message))
            total += 1
        cursor = chunk_end
    return total


def carbon_window(days):
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    fmt = "%Y-%m-%dT%H:%MZ"
    return start.strftime(fmt), end.strftime(fmt)


def backfill_carbon_intensity(producer, days):
    start, end = carbon_window(days)
    response = requests.get(f"{CARBON_URL}/intensity/{start}/{end}", timeout=30)
    response.raise_for_status()
    records = response.json().get("data", [])
    for record in records:
        intensity = record.get("intensity", {})
        message = {
            "from": record.get("from"),
            "to": record.get("to"),
            "intensity_forecast": intensity.get("forecast"),
            "intensity_actual": intensity.get("actual"),
            "intensity_index": intensity.get("index"),
            "ingested_at": now_iso(),
        }
        producer.produce("carbon-intensity", key=record.get("from"), value=json.dumps(message))
    return len(records)


def backfill_generation_mix(producer, days):
    start, end = carbon_window(days)
    response = requests.get(f"{CARBON_URL}/generation/{start}/{end}", timeout=30)
    response.raise_for_status()
    total = 0
    for period in response.json().get("data", []):
        for entry in period.get("generationmix", []):
            message = {
                "from": period.get("from"),
                "to": period.get("to"),
                "fuel": entry.get("fuel"),
                "percentage": entry.get("perc"),
                "ingested_at": now_iso(),
            }
            producer.produce(
                "generation-mix",
                key=f"{period.get('from')}-{entry.get('fuel')}",
                value=json.dumps(message),
            )
            total += 1
    return total


def backfill_weather(producer, days):
    location = os.getenv("OPEN_METEO_LOCATION", "central-england")
    params = {
        "latitude": os.getenv("OPEN_METEO_LAT", "52.5"),
        "longitude": os.getenv("OPEN_METEO_LON", "-1.5"),
        "hourly": "temperature_2m,wind_speed_100m,shortwave_radiation",
        "past_days": days,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = requests.get(METEO_URL, params=params, timeout=30)
    response.raise_for_status()
    hourly = response.json().get("hourly", {})
    timestamps = hourly.get("time", [])
    for i, ts in enumerate(timestamps):
        message = {
            "timestamp": ts,
            "location": location,
            "temperature_2m": hourly["temperature_2m"][i],
            "wind_speed_100m": hourly["wind_speed_100m"][i],
            "shortwave_radiation": hourly["shortwave_radiation"][i],
            "ingested_at": now_iso(),
        }
        producer.produce("weather", key=f"{location}-{ts}", value=json.dumps(message))
    return len(timestamps)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=7, help="days of history (max 14)")
    days = min(parser.parse_args().days, 14)  # carbon intensity API caps ranges at 14 days

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    for name, fn in [
        ("system-prices", backfill_system_prices),
        ("derated-margin", backfill_derated_margin),
        ("carbon-intensity", backfill_carbon_intensity),
        ("generation-mix", backfill_generation_mix),
        ("weather", backfill_weather),
    ]:
        try:
            count = fn(producer, days)
            producer.flush()
            print(f"{name}: produced {count} records")
        except Exception as e:
            print(f"{name}: failed -- {e}")


if __name__ == "__main__":
    main()
