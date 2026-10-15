"""Tests for the data-quality validation layer.

These are the rules that decide whether a Kafka message reaches TimescaleDB
or gets quarantined to the dead letter queue.
"""

import pytest

from common.validation import (
    validate_system_price,
    validate_carbon_intensity,
    validate_generation_mix,
    validate_weather,
    validate_derated_margin,
)


def valid_system_price():
    return {
        "settlement_date": "2026-10-15",
        "settlement_period": 1,
        "system_buy_price": 90.5,
        "system_sell_price": 88.2,
        "net_imbalance_volume": -412.0,
        "ingested_at": "2026-10-15T00:13:00+00:00",
    }


class TestSystemPriceValidation:
    def test_valid_record_passes(self):
        assert validate_system_price(valid_system_price()) == []

    def test_missing_required_field_fails(self):
        record = valid_system_price()
        del record["system_buy_price"]
        errors = validate_system_price(record)
        assert any("system_buy_price" in e for e in errors)

    def test_negative_price_is_valid(self):
        # GB prices legitimately go negative on windy nights
        record = valid_system_price()
        record["system_buy_price"] = -45.0
        assert validate_system_price(record) == []

    def test_extreme_price_fails(self):
        record = valid_system_price()
        record["system_buy_price"] = 15000.0
        errors = validate_system_price(record)
        assert any("system_buy_price" in e for e in errors)

    def test_impossible_settlement_period_fails(self):
        record = valid_system_price()
        record["settlement_period"] = 99
        errors = validate_system_price(record)
        assert any("settlement_period" in e for e in errors)

    def test_clock_change_day_periods_49_and_50_pass(self):
        for period in (49, 50):
            record = valid_system_price()
            record["settlement_period"] = period
            assert validate_system_price(record) == []


class TestCarbonIntensityValidation:
    def test_valid_record_passes(self):
        record = {
            "from": "2026-10-15T00:00Z",
            "to": "2026-10-15T00:30Z",
            "intensity_forecast": 64,
            "intensity_actual": 64,
            "intensity_index": "low",
            "ingested_at": "2026-10-15T00:36:00+00:00",
        }
        assert validate_carbon_intensity(record) == []

    def test_impossible_intensity_fails(self):
        record = {
            "from": "2026-10-15T00:00Z",
            "to": "2026-10-15T00:30Z",
            "intensity_forecast": 9000,
            "intensity_actual": None,  # actual not yet published is fine
            "intensity_index": "high",
            "ingested_at": "2026-10-15T00:36:00+00:00",
        }
        errors = validate_carbon_intensity(record)
        assert any("intensity_forecast" in e for e in errors)


class TestGenerationMixValidation:
    def test_valid_record_passes(self):
        record = {"from": "2026-10-15T00:00Z", "fuel": "wind",
                  "percentage": 63.9, "ingested_at": "2026-10-15T00:36:00+00:00"}
        assert validate_generation_mix(record) == []

    def test_percentage_over_100_fails(self):
        record = {"from": "2026-10-15T00:00Z", "fuel": "wind",
                  "percentage": 140.0, "ingested_at": "2026-10-15T00:36:00+00:00"}
        assert validate_generation_mix(record) != []


class TestWeatherValidation:
    def test_valid_record_passes(self):
        record = {"timestamp": "2026-10-15T00:00", "location": "central-england",
                  "temperature_2m": 11.9, "wind_speed_100m": 28.1,
                  "shortwave_radiation": 0.0,
                  "ingested_at": "2026-10-15T00:36:00+00:00"}
        assert validate_weather(record) == []

    def test_impossible_temperature_fails(self):
        record = {"timestamp": "2026-10-15T00:00", "location": "central-england",
                  "temperature_2m": 80.0, "wind_speed_100m": 28.1,
                  "shortwave_radiation": 0.0,
                  "ingested_at": "2026-10-15T00:36:00+00:00"}
        assert any("temperature_2m" in e for e in validate_weather(record))


def valid_derated_margin():
    return {
        "settlement_date": "2026-10-11",
        "settlement_period": 40,
        "start_time": "2026-10-11T18:30:00Z",
        "forecast_horizon": 1,
        "derated_margin_mw": 7104.4893,
        "loss_of_load_probability": 0.0,
        "publish_time": "2026-10-11T17:34:42Z",
        "ingested_at": "2026-10-11T17:36:00+00:00",
    }


class TestDeratedMarginValidation:
    def test_valid_record_passes(self):
        assert validate_derated_margin(valid_derated_margin()) == []

    def test_missing_margin_fails(self):
        record = valid_derated_margin()
        record["derated_margin_mw"] = None
        errors = validate_derated_margin(record)
        assert any("derated_margin_mw" in e for e in errors)

    def test_small_negative_margin_is_valid(self):
        # A genuine shortfall can push the forecast margin below zero
        record = valid_derated_margin()
        record["derated_margin_mw"] = -250.0
        assert validate_derated_margin(record) == []

    def test_implausible_margin_fails(self):
        # Larger than GB peak demand -- almost certainly kW, not MW
        record = valid_derated_margin()
        record["derated_margin_mw"] = 7_104_489.3
        errors = validate_derated_margin(record)
        assert any("derated_margin_mw" in e for e in errors)

    def test_probability_above_one_fails(self):
        record = valid_derated_margin()
        record["loss_of_load_probability"] = 1.5
        errors = validate_derated_margin(record)
        assert any("loss_of_load_probability" in e for e in errors)

    def test_impossible_horizon_fails(self):
        record = valid_derated_margin()
        record["forecast_horizon"] = 0
        errors = validate_derated_margin(record)
        assert any("forecast_horizon" in e for e in errors)
