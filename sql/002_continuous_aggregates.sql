-- Continuous aggregates: pre-computed rollups that refresh incrementally.
--
-- TimescaleDB maintains these as materialized views that only recompute new
-- data, so dashboards stay fast as history grows. Run once after the base
-- hypertables exist.

-- Daily average system prices with imbalance volume and daily spike peak.
-- (system_prices partitions by settlement_date, a DATE, so daily buckets are
-- the finest granularity a continuous aggregate can reference here.)
CREATE MATERIALIZED VIEW IF NOT EXISTS system_prices_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', settlement_date) AS bucket,
    AVG(system_buy_price)  AS avg_buy_price,
    AVG(system_sell_price) AS avg_sell_price,
    MAX(system_buy_price)  AS max_buy_price,
    AVG(net_imbalance_volume) AS avg_niv
FROM system_prices
GROUP BY bucket
WITH NO DATA;

-- Daily average generation mix per fuel
CREATE MATERIALIZED VIEW IF NOT EXISTS generation_mix_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', from_ts) AS bucket,
    fuel,
    AVG(percentage) AS avg_percentage
FROM generation_mix
GROUP BY bucket, fuel
WITH NO DATA;

-- Refresh policies keep aggregates current without full recomputation
SELECT add_continuous_aggregate_policy('system_prices_daily',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('generation_mix_daily',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);
