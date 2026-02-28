"""
Trend Quality & Advanced Indicators
======================================
1. ADX / DMI  — trend strength & directional movement
2. Supertrend — ATR-based trailing stop / trend line
3. Candlestick Pattern Recognition (via TA-Lib)
"""
from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from typing import Optional

warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value")


# ══════════════════════════════════════════════════════════════
# 1.  ADX & DMI (Average Directional Index)
# ══════════════════════════════════════════════════════════════

def adx_dmi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> dict:
    """
    Compute ADX, +DI, and -DI using Wilder's smoothing.

    ADX > 25 → strong trend.
    +DI > -DI → bullish bias.

    Returns
    -------
    dict with keys:
        available, period, current_adx, current_plus_di, current_minus_di,
        trend_strength, directional_bias,
        _adx_series, _plus_di_series, _minus_di_series
    """
    n = len(close)
    if n < period * 3:
        return {"available": False, "reason": f"Need ≥{period*3} bars"}

    h = high.values.astype(float)
    l = low.values.astype(float)
    c = close.values.astype(float)

    # True Range
    tr = np.zeros(n)
    plus_dm = np.zeros(n)
    minus_dm = np.zeros(n)

    for i in range(1, n):
        hl = h[i] - l[i]
        hpc = abs(h[i] - c[i - 1])
        lpc = abs(l[i] - c[i - 1])
        tr[i] = max(hl, hpc, lpc)

        up_move = h[i] - h[i - 1]
        down_move = l[i - 1] - l[i]
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0

    # Wilder's smoothing (exponential moving average)
    def wilder_smooth(arr, p):
        out = np.zeros_like(arr)
        out[p] = arr[1:p + 1].sum()
        for i in range(p + 1, len(arr)):
            out[i] = out[i - 1] - out[i - 1] / p + arr[i]
        return out

    atr = wilder_smooth(tr, period)
    smooth_plus = wilder_smooth(plus_dm, period)
    smooth_minus = wilder_smooth(minus_dm, period)

    plus_di = np.where(atr > 0, 100 * smooth_plus / atr, 0)
    minus_di = np.where(atr > 0, 100 * smooth_minus / atr, 0)

    dx = np.where(
        (plus_di + minus_di) > 0,
        100 * np.abs(plus_di - minus_di) / (plus_di + minus_di),
        0,
    )

    # ADX uses the *average* of first p DX values as seed (not sum)
    adx = np.zeros(n)
    adx_start = period * 2
    if adx_start < n:
        adx[adx_start] = dx[period + 1: adx_start + 1].mean()
        for i in range(adx_start + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period

    # The first 2*period points are unreliable
    start = period * 2

    idx = close.index
    adx_s = pd.Series(adx, index=idx, name="ADX")
    plus_di_s = pd.Series(plus_di, index=idx, name="+DI")
    minus_di_s = pd.Series(minus_di, index=idx, name="-DI")

    cur_adx = round(float(adx_s.iloc[-1]), 2)
    cur_pdi = round(float(plus_di_s.iloc[-1]), 2)
    cur_mdi = round(float(minus_di_s.iloc[-1]), 2)

    if cur_adx >= 40:
        strength = "VERY STRONG"
    elif cur_adx >= 25:
        strength = "STRONG"
    elif cur_adx >= 20:
        strength = "MODERATE"
    else:
        strength = "WEAK / NO TREND"

    if cur_pdi > cur_mdi:
        bias = "BULLISH"
    elif cur_mdi > cur_pdi:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    return {
        "available": True,
        "period": period,
        "current_adx": cur_adx,
        "current_plus_di": cur_pdi,
        "current_minus_di": cur_mdi,
        "trend_strength": strength,
        "directional_bias": bias,
        "_adx_series": adx_s.iloc[start:],
        "_plus_di_series": plus_di_s.iloc[start:],
        "_minus_di_series": minus_di_s.iloc[start:],
    }


# ══════════════════════════════════════════════════════════════
# 2.  Supertrend
# ══════════════════════════════════════════════════════════════

def supertrend(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 10,
    multiplier: float = 3.0,
) -> dict:
    """
    Compute the Supertrend indicator.

    Returns
    -------
    dict with keys:
        available, period, multiplier,
        current_supertrend, current_direction,
        _supertrend_series, _direction_series
    """
    n = len(close)
    if n < period + 5:
        return {"available": False, "reason": f"Need ≥{period+5} bars"}

    h = high.values.astype(float)
    l = low.values.astype(float)
    c = close.values.astype(float)

    # ATR via Wilder's smoothing
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
    atr = np.zeros(n)
    atr[period] = tr[1:period + 1].mean()
    for i in range(period + 1, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    hl2 = (h + l) / 2
    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    supertrend_val = np.zeros(n)
    direction = np.zeros(n)  # 1 = up (bullish), -1 = down (bearish)

    # Initialize
    supertrend_val[period] = upper_band[period]
    direction[period] = -1

    for i in range(period + 1, n):
        # Adjust bands
        if lower_band[i] > lower_band[i - 1] or c[i - 1] < lower_band[i - 1]:
            pass  # keep new lower_band
        else:
            lower_band[i] = lower_band[i - 1]

        if upper_band[i] < upper_band[i - 1] or c[i - 1] > upper_band[i - 1]:
            pass  # keep new upper_band
        else:
            upper_band[i] = upper_band[i - 1]

        # Direction logic
        if direction[i - 1] == 1:  # was bullish
            if c[i] < lower_band[i]:
                direction[i] = -1  # flip to bearish
                supertrend_val[i] = upper_band[i]
            else:
                direction[i] = 1
                supertrend_val[i] = lower_band[i]
        else:  # was bearish
            if c[i] > upper_band[i]:
                direction[i] = 1  # flip to bullish
                supertrend_val[i] = lower_band[i]
            else:
                direction[i] = -1
                supertrend_val[i] = upper_band[i]

    idx = close.index
    st_series = pd.Series(supertrend_val, index=idx, name="Supertrend")
    dir_series = pd.Series(direction, index=idx, name="Direction")

    # Trim initial unreliable values
    valid = st_series.iloc[period:]
    dir_valid = dir_series.iloc[period:]

    cur_st = round(float(valid.iloc[-1]), 2)
    cur_dir = "BULLISH" if int(dir_valid.iloc[-1]) == 1 else "BEARISH"

    return {
        "available": True,
        "period": period,
        "multiplier": multiplier,
        "current_supertrend": cur_st,
        "current_direction": cur_dir,
        "_supertrend_series": valid,
        "_direction_series": dir_valid,
    }


# ══════════════════════════════════════════════════════════════
# 3.  Candlestick Pattern Recognition (TA-Lib)
# ══════════════════════════════════════════════════════════════

def candlestick_patterns(
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback: int = 5,
) -> dict:
    """
    Scan for high-probability candlestick patterns using TA-Lib.

    Returns
    -------
    dict with keys:
        available, patterns_found (list of dicts with name, date, signal)
    """
    try:
        import talib
    except ImportError:
        return {"available": False, "reason": "TA-Lib not installed"}

    o = open_.values.astype(float)
    h = high.values.astype(float)
    l = low.values.astype(float)
    c = close.values.astype(float)

    # List of patterns to scan: (function_name, display_name, type)
    patterns = [
        ("CDLENGULFING",      "Engulfing"),
        ("CDLDOJI",           "Doji"),
        ("CDLMORNINGSTAR",    "Morning Star"),
        ("CDLEVENINGSTAR",    "Evening Star"),
        ("CDLHAMMER",         "Hammer"),
        ("CDLINVERTEDHAMMER", "Inverted Hammer"),
        ("CDLSHOOTINGSTAR",   "Shooting Star"),
        ("CDLHARAMI",         "Harami"),
        ("CDLPIERCING",       "Piercing Line"),
        ("CDLDARKCLOUDCOVER", "Dark Cloud Cover"),
        ("CDL3WHITESOLDIERS", "Three White Soldiers"),
        ("CDL3BLACKCROWS",    "Three Black Crows"),
        ("CDLMORNINGDOJISTAR", "Morning Doji Star"),
        ("CDLEVENINGDOJISTAR", "Evening Doji Star"),
        ("CDLSPINNINGTOP",    "Spinning Top"),
        ("CDLMARUBOZU",       "Marubozu"),
        ("CDLHANGINGMAN",     "Hanging Man"),
        ("CDLDRAGONFLYDOJI",  "Dragonfly Doji"),
        ("CDLGRAVESTONEDOJI",  "Gravestone Doji"),
    ]

    found = []
    idx = close.index

    for func_name, display_name in patterns:
        fn = getattr(talib, func_name, None)
        if fn is None:
            continue
        try:
            result = fn(o, h, l, c)
        except Exception:
            continue

        # Check last `lookback` candles only
        for j in range(-lookback, 0):
            val = int(result[j])
            if val != 0:
                signal = "BULLISH" if val > 0 else "BEARISH"
                bar_date = str(idx[j].date()) if hasattr(idx[j], 'date') else str(idx[j])
                found.append({
                    "pattern": display_name,
                    "date": bar_date,
                    "signal": signal,
                    "strength": abs(val),  # 100 = normal, 200 = strong
                })

    # Sort by date (most recent first)
    found.sort(key=lambda x: x["date"], reverse=True)

    return {
        "available": True,
        "patterns_found": found,
        "count": len(found),
    }
