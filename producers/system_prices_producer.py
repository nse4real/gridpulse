import requests
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "system-prices"
ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices"

def fetch_system_prices():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{ELEXON_BASE_URL}/{today}"
    params = {"format": "json"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    while True:
        try:
            data = fetch_system_prices()
            records = data.get("data", [])

            for record in records:
                message = {
                    "settlement_date": record.get("settlementDate"),
                    "settlement_period": record.get("settlementPeriod"),
                    "system_buy_price": record.get("systemBuyPrice"),
                    "system_sell_price": record.get("systemSellPrice"),
                    "net_imbalance_volume": record.get("netImbalanceVolume"),
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                }

                producer.produce(
                    TOPIC,
                    key=f"{record.get('settlementDate')}-{record.get('settlementPeriod')}",
                    value=json.dumps(message),
                    callback=delivery_report
                )

            producer.flush()
            print(f"Produced {len(records)} records at {datetime.now(timezone.utc).isoformat()}")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()