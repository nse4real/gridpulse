"""Tests for the domain intelligence functions.

These functions encode the commercial logic hiring managers care about, so
the maths is locked down with tests.
"""

import pytest

from metrics.domain import (
    price_cap_headroom,
    spark_spread,
    wind_shortfall_pct,
    market_stress_index,
    margin_tightness_score,
    KWH_PER_THERM,
)


class TestPriceCapHeadroom:
    def test_full_headroom_is_green(self):
        # Live price 50% below the assumed allowance
        result = price_cap_headroom(75.0, 37.5)
        assert result["headroom_pct"] == 50.0
        assert result["status"] == "green"

    def test_thin_headroom_is_amber(self):
        result = price_cap_headroom(75.0, 72.0)
        assert result["headroom_pct"] == 4.0
        assert result["status"] == "amber"

    def test_negative_headroom_is_red(self):
        # The 2021 scenario: wholesale running above the cap assumption
        result = price_cap_headroom(75.0, 174.23)
        assert result["headroom_pct"] < 0
        assert result["status"] == "red"

    def test_zero_allowance_rejected(self):
        with pytest.raises(ValueError):
            price_cap_headroom(0, 50)


class TestSparkSpread:
    def test_gas_cost_conversion(self):
        # 90 p/therm -> GBP 0.90/therm; 1 MWh = 1000/29.3071 ~ 34.12 therms
        # At 49% efficiency: 0.90 * 34.12 / 0.49 ~ 62.68 GBP/MWh electric
        spread = spark_spread(electricity_price_gbp_mwh=100.0,
                              gas_price_p_per_therm=90.0,
                              efficiency=0.49)
        expected_gas_cost = 0.90 * (1000 / KWH_PER_THERM) / 0.49
        assert spread == pytest.approx(100.0 - expected_gas_cost, abs=0.01)

    def test_negative_spread_when_gas_expensive(self):
        spread = spark_spread(40.0, 200.0)
        assert spread < 0

    def test_invalid_efficiency_rejected(self):
        with pytest.raises(ValueError):
            spark_spread(100.0, 90.0, efficiency=1.5)


class TestWindShortfall:
    def test_no_shortfall_when_wind_at_baseline(self):
        assert wind_shortfall_pct(30.0, 30.0) == 0.0

    def test_surplus_wind_clamps_to_zero(self):
        assert wind_shortfall_pct(63.9, 30.0) == 0.0

    def test_calm_day_shortfall(self):
        assert wind_shortfall_pct(10.0, 30.0) == pytest.approx(66.67, abs=0.01)


class TestMarginTightness:
    def test_comfortable_margin_scores_zero(self):
        # Median October de-rated margin is ~21 GW
        assert margin_tightness_score(21_445.0) == 0.0

    def test_scarcity_margin_scores_100(self):
        assert margin_tightness_score(2_000.0) == 100.0
        assert margin_tightness_score(-500.0) == 100.0

    def test_evening_peak_is_partially_tight(self):
        # Tightest period of 8-15 Oct 2026: 7,104 MW at the 18:30 peak
        assert margin_tightness_score(7_104.0) == pytest.approx(71.6, abs=0.1)

    def test_missing_forecast_scores_zero(self):
        assert margin_tightness_score(None) == 0.0


class TestMarketStressIndex:
    def test_calm_market_near_zero(self):
        result = market_stress_index(
            derated_margin_mw=25_000.0, abs_niv_mwh=10.0,
            price_volatility_gbp=2.0, wind_shortfall_pct_value=0.0)
        assert result["index"] < 5

    def test_severe_market_near_100(self):
        result = market_stress_index(
            derated_margin_mw=2_000.0, abs_niv_mwh=3000.0,
            price_volatility_gbp=150.0, wind_shortfall_pct_value=100.0)
        assert result["index"] == pytest.approx(100.0)

    def test_components_capped_at_100(self):
        result = market_stress_index(
            derated_margin_mw=-9_999.0, abs_niv_mwh=99999.0,
            price_volatility_gbp=9999.0, wind_shortfall_pct_value=500.0)
        for component in result["components"].values():
            assert component <= 100.0

    def test_weights_sum_to_one(self):
        # Only the margin component active at full scale -> index = 35
        result = market_stress_index(
            derated_margin_mw=2_000.0, abs_niv_mwh=0.0,
            price_volatility_gbp=0.0, wind_shortfall_pct_value=0.0)
        assert result["index"] == pytest.approx(35.0)

    def test_margin_component_reported(self):
        result = market_stress_index(
            derated_margin_mw=11_000.0, abs_niv_mwh=0.0,
            price_volatility_gbp=0.0, wind_shortfall_pct_value=0.0)
        assert result["components"]["margin_tightness"] == pytest.approx(50.0)
        assert "buy_sell_spread" not in result["components"]
