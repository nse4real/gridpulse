"""Consumes the weather topic into a TimescaleDB hypertable.

Rows are keyed on (timestamp, location) so re-polls overwrite rather than
duplicate. Invalid rows go to weather-dlq.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

from common.validation import validate_weather
from common.dlq import send_to_dlq

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "weather"
DLQ_TOPIC = "weather-dlq"
GROUP_ID = "weather-consumer"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                timestamp           TIMESTAMPTZ NOT NULL,
                location            TEXT NOT NULL,
                temperature_2m      NUMERIC(5, 2),
                wind_speed_100m     NUMERIC(6, 2),
                shortwave_radiation NUMERIC(7, 2),
                ingested_at         TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (timestamp, location)
            );
            SELECT create_hypertable(
                'weather', 'timestamp',
                if_not_exists => TRUE
            );
        """)
        conn.commit()
    print("Table ready.")


def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO weather (
                timestamp, location, temperature_2m,
                wind_speed_100m, shortwave_radiation, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, location)
            DO UPDATE SET
                temperature_2m      = EXCLUDED.temperature_2m,
                wind_speed_100m     = EXCLUDED.wind_speed_100m,
                shortwave_radiation = EXCLUDED.shortwave_radiation,
                ingested_at         = EXCLUDED.ingested_at;
        """, (
            record["timestamp"], record["location"],
            record["temperature_2m"], record["wind_speed_100m"],
            record["shortwave_radiation"], record["ingested_at"],
        ))
        conn.commit()


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    create_table(conn)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    consumer.subscribe([TOPIC])
    print(f"Consuming from {TOPIC}...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                send_to_dlq(dlq_producer, DLQ_TOPIC, raw, ["invalid JSON"])
                print("Quarantined malformed JSON to DLQ")
                continue

            errors = validate_weather(record)
            if errors:
                send_to_dlq(dlq_producer, DLQ_TOPIC, record, errors)
                print(f"Quarantined invalid record to DLQ: {errors}")
                continue

            insert_record(conn, record)
            print(f"Stored: {record['timestamp']} {record['location']} "
                  f"temp={record['temperature_2m']}C "
                  f"wind={record['wind_speed_100m']}km/h")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
