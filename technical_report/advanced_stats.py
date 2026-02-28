"""
Advanced Statistical Features
================================
1. HMM Regime Detection  — classifies market into Bull/Bear/Sideways regimes
2. Hurst Exponent        — trend vs mean-reversion vs random walk
3. ACF / PACF            — return autocorrelation structure
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


# ══════════════════════════════════════════════════════════════
# 1.  Hidden Markov Model — Regime Detection
# ══════════════════════════════════════════════════════════════

def hmm_regime_detection(
    close: pd.Series,
    n_regimes: int = 3,
    lookback: int = 252,
) -> dict:
    """
    Fit a Gaussian HMM on [returns, volatility] features and label
    each regime as Bull-Volatile, Bear-Trending, or Sideways-Choppy.

    Returns
    -------
    dict with keys:
        available, n_regimes, current_regime, regime_label,
        regime_means, regime_vols, _regime_series
    """
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError:
        return {"available": False, "reason": "hmmlearn not installed"}

    if close is None or len(close) < max(60, lookback // 2):
        return {"available": False, "reason": "Insufficient data"}

    returns = close.pct_change().dropna().tail(lookback)
    vol_5d = returns.rolling(5).std().dropna()
    common = returns.index.intersection(vol_5d.index)
    returns = returns.loc[common]
    vol_5d = vol_5d.loc[common]

    if len(returns) < 60:
        return {"available": False, "reason": "Insufficient data after alignment"}

    X = np.column_stack([returns.values, vol_5d.values])

    # Add tiny regularisation to avoid singular covariance matrices
    X = X + np.random.RandomState(42).normal(0, 1e-6, X.shape)

    try:
        model = GaussianHMM(
            n_components=n_regimes, covariance_type="diag",
            n_iter=300, random_state=42, tol=1e-4,
        )
        model.fit(X)
        hidden_states = model.predict(X)
    except Exception as e:
        return {"available": False, "reason": f"HMM fit failed: {e}"}

    # ── Label regimes by mean return & mean volatility ──
    regime_stats = []
    for r in range(n_regimes):
        mask = hidden_states == r
        r_mean = float(returns.values[mask].mean()) if mask.sum() > 0 else 0
        r_vol = float(vol_5d.values[mask].mean()) if mask.sum() > 0 else 0
        regime_stats.append({"id": r, "mean_ret": r_mean, "mean_vol": r_vol})

    # Sort by mean return descending → highest-return = "Bull"
    regime_stats.sort(key=lambda x: x["mean_ret"], reverse=True)

    label_map = {}
    labels_list = []
    for i, rs in enumerate(regime_stats):
        if i == 0:
            lbl = "Bull / High-Momentum"
        elif i == n_regimes - 1:
            lbl = "Bear / Trending-Down"
        else:
            lbl = "Sideways / Choppy"
        label_map[rs["id"]] = lbl
        labels_list.append({
            "regime_id": rs["id"],
            "label": lbl,
            "avg_daily_return_pct": round(rs["mean_ret"] * 100, 4),
            "avg_5d_vol_pct": round(rs["mean_vol"] * 100, 4),
        })

    regime_series = pd.Series(hidden_states, index=returns.index, name="regime")
    current_regime_id = int(hidden_states[-1])

    return {
        "available": True,
        "n_regimes": n_regimes,
        "current_regime_id": current_regime_id,
        "current_regime_label": label_map[current_regime_id],
        "regimes": labels_list,
        "_regime_series": regime_series,
        "_label_map": label_map,
    }


# ══════════════════════════════════════════════════════════════
# 2.  Hurst Exponent (Rescaled-Range method)
# ══════════════════════════════════════════════════════════════

def hurst_exponent(close: pd.Series, max_lag: int = 100) -> dict:
    """
    Compute the Hurst exponent via R/S analysis.

    H > 0.5  → trending / persistent
    H ≈ 0.5  → random walk
    H < 0.5  → mean-reverting / anti-persistent

    Returns
    -------
    dict with keys: available, hurst, interpretation
    """
    if close is None or len(close) < 60:
        return {"available": False, "reason": "Need ≥60 bars"}

    prices = close.dropna().values.astype(float)
    n = len(prices)
    max_lag = min(max_lag, n // 2)
    if max_lag < 10:
        return {"available": False, "reason": "Series too short for R/S"}

    lags = range(10, max_lag + 1)
    rs_list = []
    for lag in lags:
        rs_vals = []
        for start in range(0, n - lag, lag):
            segment = prices[start: start + lag]
            returns = np.diff(segment) / segment[:-1]
            mean_r = returns.mean()
            deviations = np.cumsum(returns - mean_r)
            R = deviations.max() - deviations.min()
            S = returns.std(ddof=1)
            if S > 0:
                rs_vals.append(R / S)
        if rs_vals:
            rs_list.append((np.log(lag), np.log(np.mean(rs_vals))))

    if len(rs_list) < 5:
        return {"available": False, "reason": "Not enough valid R/S points"}

    log_lags = np.array([x[0] for x in rs_list])
    log_rs = np.array([x[1] for x in rs_list])
    H = float(np.polyfit(log_lags, log_rs, 1)[0])

    if H > 0.55:
        interp = "TRENDING (persistent)"
    elif H < 0.45:
        interp = "MEAN-REVERTING (anti-persistent)"
    else:
        interp = "RANDOM WALK"

    return {
        "available": True,
        "hurst": round(H, 4),
        "interpretation": interp,
        "implication": (
            "Momentum / trend-following strategies favoured"
            if H > 0.55
            else "Oscillator / mean-reversion strategies favoured"
            if H < 0.45
            else "No structural edge — price moves are largely random"
        ),
    }


# ══════════════════════════════════════════════════════════════
# 3.  Autocorrelation (ACF / PACF)
# ══════════════════════════════════════════════════════════════

def acf_pacf(close: pd.Series, n_lags: int = 30) -> dict:
    """
    Compute ACF and PACF of daily returns.

    Returns
    -------
    dict with keys: available, n_lags, acf_values, pacf_values,
                    significant_acf_lags, _acf, _pacf
    """
    try:
        from statsmodels.tsa.stattools import acf as sm_acf, pacf as sm_pacf
    except ImportError:
        return {"available": False, "reason": "statsmodels not installed"}

    if close is None or len(close) < n_lags + 10:
        return {"available": False, "reason": "Insufficient data"}

    returns = close.pct_change().dropna()
    n = len(returns)
    n_lags = min(n_lags, n // 2 - 1)

    acf_vals, acf_ci = sm_acf(returns, nlags=n_lags, alpha=0.05)
    pacf_vals, pacf_ci = sm_pacf(returns, nlags=n_lags, alpha=0.05)

    # Confidence bound (approx ±1.96/√n)
    bound = 1.96 / np.sqrt(n)
    sig_lags = [int(i) for i in range(1, len(acf_vals))
                if abs(acf_vals[i]) > bound]

    return {
        "available": True,
        "n_lags": n_lags,
        "confidence_bound": round(bound, 4),
        "significant_acf_lags": sig_lags,
        "momentum_persistence": len(sig_lags) > 3,
        "_acf": acf_vals.tolist(),
        "_pacf": pacf_vals.tolist(),
        "_returns": returns,
    }
