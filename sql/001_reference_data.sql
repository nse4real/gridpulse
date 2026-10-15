-- Reference data: manually maintained market assumptions.
--
-- Ofgem and DESNZ publish price cap models as downloadable Excel files --
-- there is no API. This table is the durable store for those published
-- assumptions; update the seed values each quarter when Ofgem releases
-- a new cap.
--
-- Values below are working estimates for Q2 2026 and must be checked against
-- Ofgem's published cap model before being relied on commercially.

CREATE TABLE IF NOT EXISTS reference_data (
    key           TEXT PRIMARY KEY,
    value_numeric NUMERIC(15, 6) NOT NULL,
    unit          TEXT NOT NULL,
    description   TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ofgem price cap for a typical dual-fuel household, Q2 2026 (GBP/year)
INSERT INTO reference_data (key, value_numeric, unit, description) VALUES
    ('ofgem_cap_annual_gbp', 1641, 'GBP/year',
     'Ofgem price cap for a typical dual-fuel household, current quarter');

-- Wholesale electricity cost Ofgem assumed when setting the cap (GBP/MWh).
-- Derived from the wholesale allowance inside the published cap model.
INSERT INTO reference_data (key, value_numeric, unit, description) VALUES
    ('ofgem_assumed_wholesale_electricity_gbp_mwh', 75, 'GBP/MWh',
     'Wholesale electricity allowance assumed in the current price cap');

-- Wholesale gas cost assumption (pence/therm), for spark spread calculation
INSERT INTO reference_data (key, value_numeric, unit, description) VALUES
    ('assumed_gas_price_p_per_therm', 90, 'p/therm',
     'Working assumption for NBP gas price; update from market data');

-- Baseline wind share of GB generation, used to spot wind shortfalls (%)
INSERT INTO reference_data (key, value_numeric, unit, description) VALUES
    ('baseline_wind_share_pct', 30, '%',
     'Typical wind share of GB generation mix; shortfalls stress the grid')
ON CONFLICT (key) DO NOTHING;
