import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv
from datetime import datetime, timezone

from common.validation import validate_system_price
from common.dlq import send_to_dlq

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "system-prices"
DLQ_TOPIC = "system-prices-dlq"
GROUP_ID = "system-prices-consumer"

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
            CREATE TABLE IF NOT EXISTS system_prices (
                settlement_date     DATE NOT NULL,
                settlement_period   INTEGER NOT NULL,
                system_buy_price    NUMERIC(10, 4),
                system_sell_price   NUMERIC(10, 4),
                net_imbalance_volume NUMERIC(15, 6),
                ingested_at         TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (settlement_date, settlement_period)
            );
            SELECT create_hypertable(
                'system_prices', 'settlement_date',
                if_not_exists => TRUE
            );
        """)
        conn.commit()
    print("Table ready.")

def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO system_prices (
                settlement_date, settlement_period,
                system_buy_price, system_sell_price,
                net_imbalance_volume, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (settlement_date, settlement_period)
            DO UPDATE SET
                system_buy_price = EXCLUDED.system_buy_price,
                system_sell_price = EXCLUDED.system_sell_price,
                net_imbalance_volume = EXCLUDED.net_imbalance_volume,
                ingested_at = EXCLUDED.ingested_at;
        """, (
            record["settlement_date"],
            record["settlement_period"],
            record["system_buy_price"],
            record["system_sell_price"],
            record["net_imbalance_volume"],
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

            errors = validate_system_price(record)
            if errors:
                send_to_dlq(dlq_producer, DLQ_TOPIC, record, errors)
                print(f"Quarantined invalid record to DLQ: {errors}")
                continue

            insert_record(conn, record)
            print(f"Stored: {record['settlement_date']} SP{record['settlement_period']} "
                  f"Buy={record['system_buy_price']} Sell={record['system_sell_price']}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()