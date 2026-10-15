import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

from common.validation import validate_derated_margin
from common.dlq import send_to_dlq

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "derated-margin"
DLQ_TOPIC = "derated-margin-dlq"
GROUP_ID = "derated-margin-consumer"

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
            CREATE TABLE IF NOT EXISTS derated_margin (
                start_time               TIMESTAMPTZ NOT NULL,
                forecast_horizon         INTEGER NOT NULL,
                settlement_date          DATE NOT NULL,
                settlement_period        INTEGER NOT NULL,
                derated_margin_mw        NUMERIC(12, 3),
                loss_of_load_probability NUMERIC(12, 9),
                publish_time             TIMESTAMPTZ,
                ingested_at              TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (start_time, forecast_horizon)
            );
            SELECT create_hypertable(
                'derated_margin', 'start_time',
                if_not_exists => TRUE
            );
        """)
        conn.commit()
    print("Table ready.")

def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO derated_margin (
                start_time, forecast_horizon,
                settlement_date, settlement_period,
                derated_margin_mw, loss_of_load_probability,
                publish_time, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (start_time, forecast_horizon)
            DO UPDATE SET
                derated_margin_mw = EXCLUDED.derated_margin_mw,
                loss_of_load_probability = EXCLUDED.loss_of_load_probability,
                publish_time = EXCLUDED.publish_time,
                ingested_at = EXCLUDED.ingested_at;
        """, (
            record["start_time"],
            record["forecast_horizon"],
            record["settlement_date"],
            record["settlement_period"],
            record["derated_margin_mw"],
            record.get("loss_of_load_probability"),
            record.get("publish_time"),
            record["ingested_at"],
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

            errors = validate_derated_margin(record)
            if errors:
                send_to_dlq(dlq_producer, DLQ_TOPIC, record, errors)
                print(f"Quarantined invalid record to DLQ: {errors}")
                continue

            insert_record(conn, record)
            print(f"Stored: {record['settlement_date']} SP{record['settlement_period']} "
                  f"h{record['forecast_horizon']} margin={record['derated_margin_mw']} MW "
                  f"LOLP={record.get('loss_of_load_probability')}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
