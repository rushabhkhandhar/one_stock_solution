"""
Relative & Benchmark Analysis
================================
1. Rolling Beta & Correlation vs Benchmark (Nifty 50)
2. Mansfield Relative Strength (Price / Index ratio, smoothed)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


def rolling_beta_correlation(
    close: pd.Series,
    benchmark_close: pd.Series,
    window: int = 63,
) -> dict:
    """
    Compute rolling Beta and rolling Correlation of the stock
    versus a benchmark index over a given window.

    Beta  = Cov(stock, benchmark) / Var(benchmark)
    Correlation = Pearson r over the rolling window.

    Returns
    -------
    dict with keys:
        available, window, current_beta, current_correlation,
        avg_beta, avg_correlation,
        _beta_series, _correlation_series
    """
    if close is None or benchmark_close is None:
        return {"available": False, "reason": "Missing stock or benchmark data"}

    if len(close) < window + 10 or len(benchmark_close) < window + 10:
        return {"available": False, "reason": f"Need ≥{window+10} bars"}

    stock_ret = close.pct_change().dropna()
    bench_ret = benchmark_close.pct_change().dropna()
    common = stock_ret.index.intersection(bench_ret.index)

    if len(common) < window + 10:
        return {"available": False, "reason": "Insufficient overlapping dates"}

    sr = stock_ret.loc[common]
    br = bench_ret.loc[common]

    # Rolling covariance & variance
    cov_series = sr.rolling(window).cov(br)
    var_series = br.rolling(window).var()
    beta_series = (cov_series / var_series).dropna()

    corr_series = sr.rolling(window).corr(br).dropna()

    current_beta = round(float(beta_series.iloc[-1]), 4) if len(beta_series) > 0 else None
    current_corr = round(float(corr_series.iloc[-1]), 4) if len(corr_series) > 0 else None

    return {
        "available": True,
        "window": window,
        "current_beta": current_beta,
        "current_correlation": current_corr,
        "avg_beta": round(float(beta_series.mean()), 4),
        "avg_correlation": round(float(corr_series.mean()), 4),
        "min_beta": round(float(beta_series.min()), 4),
        "max_beta": round(float(beta_series.max()), 4),
        "_beta_series": beta_series,
        "_correlation_series": corr_series,
    }


def mansfield_relative_strength(
    close: pd.Series,
    benchmark_close: pd.Series,
    ma_period: int = 52,
) -> dict:
    """
    Mansfield Relative Strength = ((RS / SMA(RS, N)) - 1) * 100

    where RS = Stock Price / Index Price.

    Positive → stock outperforming the index.
    Negative → stock underperforming.

    Parameters
    ----------
    close           : stock close prices
    benchmark_close : index close prices
    ma_period       : smoothing period (default 52 = ~quarterly)

    Returns
    -------
    dict with keys:
        available, current_mrs, interpretation,
        _rs_ratio, _mrs_series
    """
    if close is None or benchmark_close is None:
        return {"available": False, "reason": "Missing stock or benchmark data"}

    common = close.index.intersection(benchmark_close.index)
    if len(common) < ma_period + 20:
        return {"available": False, "reason": "Insufficient overlapping data"}

    stock = close.loc[common].astype(float)
    bench = benchmark_close.loc[common].astype(float)

    # Raw Relative Strength ratio
    rs = stock / bench
    rs_sma = rs.rolling(ma_period).mean()
    mrs = ((rs / rs_sma) - 1) * 100
    mrs = mrs.dropna()

    if len(mrs) < 5:
        return {"available": False, "reason": "Not enough data after smoothing"}

    current = round(float(mrs.iloc[-1]), 4)
    if current > 2:
        interp = "OUTPERFORMING (strong)"
    elif current > 0:
        interp = "OUTPERFORMING (mild)"
    elif current > -2:
        interp = "UNDERPERFORMING (mild)"
    else:
        interp = "UNDERPERFORMING (strong)"

    return {
        "available": True,
        "ma_period": ma_period,
        "current_mrs": current,
        "interpretation": interp,
        "_rs_ratio": rs,
        "_mrs_series": mrs,
    }
