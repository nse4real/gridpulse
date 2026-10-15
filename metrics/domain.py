"""Pure domain-intelligence functions -- the commercial heart of GridPulse.

These functions contain no I/O so they can be unit-tested in isolation.
The calculator (metrics/calculator.py) feeds them live data and persists
results; Grafana reads the results.
"""

KWH_PER_THERM = 29.3071
DEFAULT_CCGT_EFFICIENCY = 0.49  # modern combined-cycle gas turbine


def price_cap_headroom(assumed_allowance_gbp_mwh, current_price_gbp_mwh):
    """How far live wholesale prices sit from Ofgem's assumed allowance.

    Ofgem sets the retail cap using forward prices observed months earlier.
    When live wholesale runs above the assumed allowance, suppliers buy at
    market but sell at the cap -- negative headroom is what killed 28
    suppliers in 2021.

    Returns headroom as a percentage of the allowance plus a traffic-light
    status: green (>10%), amber (0-10%), red (negative).
    """
    if assumed_allowance_gbp_mwh <= 0:
        raise ValueError("allowance must be positive")
    headroom_pct = ((assumed_allowance_gbp_mwh - current_price_gbp_mwh)
                    / assumed_allowance_gbp_mwh * 100)
    if headroom_pct > 10:
        status = "green"
    elif headroom_pct >= 0:
        status = "amber"
    else:
        status = "red"
    return {"headroom_pct": round(headroom_pct, 2), "status": status}


def spark_spread(electricity_price_gbp_mwh, gas_price_p_per_therm,
                 efficiency=DEFAULT_CCGT_EFFICIENCY):
    """GBP/MWh profit from burning gas to generate electricity.

    Positive spread: gas plants print money. Negative spread: running a CCGT
    loses money per MWh generated. This is the number gas plant operators
    watch constantly.
    """
    if not (0 < efficiency <= 1):
        raise ValueError("efficiency must be in (0, 1]")
    gas_gbp_per_mwh_thermal = gas_price_p_per_therm / 100 * (1000 / KWH_PER_THERM)
    gas_gbp_per_mwh_electric = gas_gbp_per_mwh_thermal / efficiency
    return round(electricity_price_gbp_mwh - gas_gbp_per_mwh_electric, 2)


def wind_shortfall_pct(actual_wind_share_pct, baseline_wind_share_pct):
    """How far wind generation sits below its typical share, in percent.

    A calm day with wind at 10% instead of a typical 30% means ~20 points of
    demand must be met by expensive gas -- a direct driver of price spikes.
    """
    if baseline_wind_share_pct <= 0:
        raise ValueError("baseline wind share must be positive")
    shortfall = (baseline_wind_share_pct - actual_wind_share_pct) / baseline_wind_share_pct * 100
    return round(max(0.0, shortfall), 2)


def _cap_0_100(value, scale_max):
    return max(0.0, min(100.0, value / scale_max * 100))


def market_stress_index(buy_sell_spread_gbp, abs_niv_mwh,
                        price_volatility_gbp, wind_shortfall_pct_value):
    """Composite 0-100 gauge of real-time grid stress.

    Weighted blend of four signals traders watch:
    - System buy/sell spread (35%): ESO paying a premium for power = scarcity
    - Net imbalance volume (25%): how far the grid missed supply/demand balance
    - Price volatility over 24h (25%): instability in settlement prices
    - Wind shortfall vs baseline (15%): weather-driven supply gap
    """
    spread_score = _cap_0_100(buy_sell_spread_gbp, 300.0)
    niv_score = _cap_0_100(abs_niv_mwh, 3000.0)
    vol_score = _cap_0_100(price_volatility_gbp, 150.0)
    wind_score = _cap_0_100(wind_shortfall_pct_value, 100.0)

    index = (0.35 * spread_score + 0.25 * niv_score
             + 0.25 * vol_score + 0.15 * wind_score)
    return {
        "index": round(index, 1),
        "components": {
            "buy_sell_spread": round(spread_score, 1),
            "net_imbalance_volume": round(niv_score, 1),
            "price_volatility": round(vol_score, 1),
            "wind_shortfall": round(wind_score, 1),
        },
    }
