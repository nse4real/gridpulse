"""Consumes the carbon-intensity topic into a TimescaleDB hypertable.

Validates every message first; failures are quarantined to carbon-intensity-dlq.
Upserts on the settlement window so NESO forecast->actual revisions overwrite
earlier rows (latest value always wins).
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

from common.validation import validate_carbon_intensity
from common.dlq import send_to_dlq

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "carbon-intensity"
DLQ_TOPIC = "carbon-intensity-dlq"
GROUP_ID = "carbon-intensity-consumer"

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
            CREATE TABLE IF NOT EXISTS carbon_intensity (
                from_ts            TIMESTAMPTZ NOT NULL,
                to_ts              TIMESTAMPTZ,
                intensity_forecast INTEGER,
                intensity_actual   INTEGER,
                intensity_index    TEXT,
                ingested_at        TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (from_ts)
            );
            SELECT create_hypertable(
                'carbon_intensity', 'from_ts',
                if_not_exists => TRUE
            );
        """)
        conn.commit()
    print("Table ready.")


def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO carbon_intensity (
                from_ts, to_ts, intensity_forecast,
                intensity_actual, intensity_index, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (from_ts)
            DO UPDATE SET
                to_ts              = EXCLUDED.to_ts,
                intensity_forecast = EXCLUDED.intensity_forecast,
                intensity_actual   = EXCLUDED.intensity_actual,
                intensity_index    = EXCLUDED.intensity_index,
                ingested_at        = EXCLUDED.ingested_at;
        """, (
            record["from"], record["to"],
            record["intensity_forecast"], record["intensity_actual"],
            record["intensity_index"], record["ingested_at"],
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

            errors = validate_carbon_intensity(record)
            if errors:
                send_to_dlq(dlq_producer, DLQ_TOPIC, record, errors)
                print(f"Quarantined invalid record to DLQ: {errors}")
                continue

            insert_record(conn, record)
            print(f"Stored: {record['from']} "
                  f"forecast={record['intensity_forecast']} "
                  f"actual={record['intensity_actual']} "
                  f"({record['intensity_index']})")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
