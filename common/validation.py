"""Shared data-quality validation for GridPulse consumers.

Every incoming Kafka message passes through one of these validators before it
touches TimescaleDB. Records that fail validation are routed to the topic's
dead letter queue (DLQ) instead of being silently dropped or crashing the
consumer -- malformed data should be visible, not hidden.

Range rationale (UK market domain knowledge):
- Electricity prices: GB system prices can go negative on windy nights and can
  spike to several thousand GBP/MWh in scarcity events. 0-9999 GBP/MWh is the
  sane envelope; outside that, something upstream is broken.
- Carbon intensity: GB grid has ranged roughly 0-500 gCO2/kWh historically.
  0-600 leaves headroom without accepting garbage.
- Generation mix: percentages are 0-100 and the mix should sum to ~100%.
- De-rated margin: NESO's spare-capacity forecast. GB peak demand is ~45-60 GW,
  so a margin above 60,000 MW is implausible; it can in principle go slightly
  negative in a genuine shortfall. Loss of load probability is a probability.
- Weather: UK extremes are within -30..45 C, 0-80 m/s wind at 100m hub height,
  0-1400 W/m2 shortwave radiation.
"""

MAX_PRICE_GBP_MWH = 9999.0
MIN_PRICE_GBP_MWH = -500.0  # negative prices are valid but noteworthy
MAX_CARBON_GCO2_KWH = 600
MAX_SETTLEMENT_PERIOD = 50  # 48 normal + 2 extra on clock-change days
MIN_DERATED_MARGIN_MW = -10_000
MAX_DERATED_MARGIN_MW = 60_000
MAX_FORECAST_HORIZON_H = 24  # BMRS publishes 1, 2, 4, 8 and 12 hours ahead


class ValidationError(Exception):
    """Raised when a record fails data-quality checks. Carries all failures."""

    def __init__(self, errors):
        self.errors = errors
        super().__init__("; ".join(errors))


def _require(fields, record):
    """Return a list of missing/None required fields."""
    return [f for f in fields if record.get(f) is None]


def _check_range(errors, record, field, lo, hi):
    value = record.get(field)
    if value is None:
        return
    if not (lo <= float(value) <= hi):
        errors.append(f"{field}={value} outside expected range [{lo}, {hi}]")


def validate_system_price(record):
    """Validate a system-prices message. Returns list of error strings."""
    errors = [f"missing field: {f}" for f in _require(
        ["settlement_date", "settlement_period", "system_buy_price",
         "system_sell_price", "ingested_at"], record)]

    sp = record.get("settlement_period")
    if sp is not None and not (1 <= int(sp) <= MAX_SETTLEMENT_PERIOD):
        errors.append(f"settlement_period={sp} outside [1, {MAX_SETTLEMENT_PERIOD}]")

    _check_range(errors, record, "system_buy_price",
                 MIN_PRICE_GBP_MWH, MAX_PRICE_GBP_MWH)
    _check_range(errors, record, "system_sell_price",
                 MIN_PRICE_GBP_MWH, MAX_PRICE_GBP_MWH)
    return errors


def validate_derated_margin(record):
    """Validate a de-rated margin / LOLP forecast message."""
    errors = [f"missing field: {f}" for f in _require(
        ["settlement_date", "settlement_period", "start_time",
         "forecast_horizon", "derated_margin_mw", "ingested_at"], record)]

    sp = record.get("settlement_period")
    if sp is not None and not (1 <= int(sp) <= MAX_SETTLEMENT_PERIOD):
        errors.append(f"settlement_period={sp} outside [1, {MAX_SETTLEMENT_PERIOD}]")

    _check_range(errors, record, "forecast_horizon", 1, MAX_FORECAST_HORIZON_H)
    _check_range(errors, record, "derated_margin_mw",
                 MIN_DERATED_MARGIN_MW, MAX_DERATED_MARGIN_MW)
    _check_range(errors, record, "loss_of_load_probability", 0, 1)
    return errors


def validate_carbon_intensity(record):
    errors = [f"missing field: {f}" for f in _require(
        ["from", "to", "intensity_forecast", "ingested_at"], record)]
    _check_range(errors, record, "intensity_forecast", 0, MAX_CARBON_GCO2_KWH)
    _check_range(errors, record, "intensity_actual", 0, MAX_CARBON_GCO2_KWH)
    return errors


def validate_generation_mix(record):
    errors = [f"missing field: {f}" for f in _require(
        ["from", "fuel", "percentage", "ingested_at"], record)]
    _check_range(errors, record, "percentage", 0, 100)
    return errors


def validate_weather(record):
    errors = [f"missing field: {f}" for f in _require(
        ["timestamp", "location", "ingested_at"], record)]
    _check_range(errors, record, "temperature_2m", -30, 45)
    _check_range(errors, record, "wind_speed_100m", 0, 80)
    _check_range(errors, record, "shortwave_radiation", 0, 1400)
    return errors
