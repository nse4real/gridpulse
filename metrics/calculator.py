"""Domain intelligence engine: computes commercial metrics from live data.

Runs as a long-lived loop alongside the consumers. Every cycle it reads the
latest silver-layer data from TimescaleDB, applies the pure functions in
metrics/domain.py, and upserts results into the market_metrics gold-layer
hypertable that Grafana reads.

Run with:
    python metrics/calculator.py
"""

import json
import sys
import os
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from dotenv import load_dotenv

from metrics.domain import (
    price_cap_headroom,
    spark_spread,
    wind_shortfall_pct,
    market_stress_index,
)

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

CYCLE_SECONDS = 60


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_metrics (
                metric_ts    TIMESTAMPTZ NOT NULL,
                metric_name  TEXT NOT NULL,
                metric_value NUMERIC(15, 4),
                detail       JSONB,
                PRIMARY KEY (metric_ts, metric_name)
            );
            SELECT create_hypertable(
                'market_metrics', 'metric_ts',
                if_not_exists => TRUE
            );
        """)
        conn.commit()


def get_reference_value(conn, key, fallback):
    with conn.cursor() as cur:
        cur.execute("SELECT value_numeric FROM reference_data WHERE key = %s", (key,))
        row = cur.fetchone()
    return float(row[0]) if row else fallback


def get_latest_system_price(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT system_buy_price, system_sell_price, net_imbalance_volume
            FROM system_prices
            ORDER BY settlement_date DESC, settlement_period DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
    if not row:
        return None
    return {"buy": float(row[0]), "sell": float(row[1]),
            "niv": float(row[2]) if row[2] is not None else 0.0}


def get_price_volatility_24h(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(STDDEV(system_buy_price), 0)
            FROM system_prices
            WHERE settlement_date >= CURRENT_DATE - INTERVAL '1 day';
        """)
        return float(cur.fetchone()[0])


def get_latest_wind_share(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT percentage FROM generation_mix
            WHERE fuel = 'wind'
            ORDER BY from_ts DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
    return float(row[0]) if row else None


def upsert_metric(conn, ts, name, value, detail):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO market_metrics (metric_ts, metric_name, metric_value, detail)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (metric_ts, metric_name)
            DO UPDATE SET metric_value = EXCLUDED.metric_value,
                          detail       = EXCLUDED.detail;
        """, (ts, name, value, json.dumps(detail)))
        conn.commit()


def compute_cycle(conn):
    ts = datetime.now(timezone.utc)

    latest = get_latest_system_price(conn)
    if latest is None:
        print("No system price data yet -- skipping cycle")
        return

    allowance = get_reference_value(
        conn, "ofgem_assumed_wholesale_electricity_gbp_mwh", 75.0)
    gas_price = get_reference_value(conn, "assumed_gas_price_p_per_therm", 90.0)
    baseline_wind = get_reference_value(conn, "baseline_wind_share_pct", 30.0)

    # Wholesale proxy: system sell price tracks the wholesale market; document
    # this proxy in the README -- day-ahead auction prices would be better but
    # require ENTSO-E or Elexon MID access.
    wholesale_now = latest["sell"]

    headroom = price_cap_headroom(allowance, wholesale_now)
    upsert_metric(conn, ts, "price_cap_headroom_pct", headroom["headroom_pct"],
                  {"status": headroom["status"],
                   "assumed_allowance_gbp_mwh": allowance,
                   "current_price_gbp_mwh": wholesale_now})

    spread = spark_spread(wholesale_now, gas_price)
    upsert_metric(conn, ts, "spark_spread_gbp_mwh", spread,
                  {"electricity_price_gbp_mwh": wholesale_now,
                   "gas_price_p_per_therm": gas_price})

    volatility = get_price_volatility_24h(conn)
    wind_share = get_latest_wind_share(conn)
    shortfall = wind_shortfall_pct(wind_share, baseline_wind) if wind_share is not None else 0.0

    stress = market_stress_index(
        buy_sell_spread_gbp=max(0.0, latest["buy"] - latest["sell"]),
        abs_niv_mwh=abs(latest["niv"]),
        price_volatility_gbp=volatility,
        wind_shortfall_pct_value=shortfall,
    )
    upsert_metric(conn, ts, "market_stress_index", stress["index"],
                  stress["components"])

    print(f"{ts.isoformat()} headroom={headroom['headroom_pct']}% "
          f"({headroom['status']}) spark_spread={spread} "
          f"stress={stress['index']}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    create_table(conn)
    print("Metrics engine running...")
    try:
        while True:
            try:
                compute_cycle(conn)
            except Exception as e:
                print(f"Cycle error: {e}")
            time.sleep(CYCLE_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
