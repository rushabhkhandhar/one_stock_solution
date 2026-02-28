"""
Seasonality & Cycle Analysis
================================
1. Monthly Return Heatmap — historical % return per calendar month
2. Day-of-Week Effect     — average return by weekday
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


def monthly_return_heatmap(close: pd.Series) -> dict:
    """
    Compute the percentage return for every (Year, Month) bucket.

    Returns
    -------
    dict with keys:
        available, heatmap_df (Year × Month DataFrame of returns %),
        best_month, worst_month, _df
    """
    if close is None or len(close) < 60:
        return {"available": False, "reason": "Need ≥60 bars"}

    prices = close.astype(float).copy()
    prices.index = pd.to_datetime(prices.index)

    # Resample to month-end close, then compute monthly returns
    monthly = prices.resample("M").last().dropna()
    monthly_ret = monthly.pct_change().dropna() * 100  # percent

    if len(monthly_ret) < 3:
        return {"available": False, "reason": "Not enough monthly data"}

    # Build pivot: rows=Year, cols=Month
    df_m = pd.DataFrame({
        "year": monthly_ret.index.year,
        "month": monthly_ret.index.month,
        "return_pct": monthly_ret.values,
    })

    pivot = df_m.pivot_table(
        index="year", columns="month", values="return_pct", aggfunc="first"
    )
    # Rename columns to month abbreviations
    month_names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }
    pivot = pivot.rename(columns=month_names)
    pivot.index.name = "Year"

    # Average by month
    avg_by_month = df_m.groupby("month")["return_pct"].mean()
    best = int(avg_by_month.idxmax())
    worst = int(avg_by_month.idxmin())

    return {
        "available": True,
        "best_month": month_names.get(best, str(best)),
        "best_month_avg_pct": round(float(avg_by_month[best]), 2),
        "worst_month": month_names.get(worst, str(worst)),
        "worst_month_avg_pct": round(float(avg_by_month[worst]), 2),
        "_heatmap_df": pivot.round(2),
        "_avg_by_month": avg_by_month,
    }


def day_of_week_effect(close: pd.Series) -> dict:
    """
    Compute average return grouped by day of the week.

    Returns
    -------
    dict with keys:
        available, day_returns (dict), best_day, worst_day, _df
    """
    if close is None or len(close) < 30:
        return {"available": False, "reason": "Need ≥30 bars"}

    returns = close.pct_change().dropna() * 100  # percent
    returns.index = pd.to_datetime(returns.index)

    day_names = {0: "Monday", 1: "Tuesday", 2: "Wednesday",
                 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}

    # Filter out weekend sessions (special Saturday / Sunday trading)
    # They have too few data points and distort the weekday averages
    returns = returns[returns.index.dayofweek < 5]

    grouped = returns.groupby(returns.index.dayofweek).agg(["mean", "std", "count"])
    grouped.columns = ["avg_return_pct", "std_pct", "count"]
    grouped.index = [day_names.get(i, str(i)) for i in grouped.index]

    # Replace NaN std (happens when count < 2) with 0
    grouped["std_pct"] = grouped["std_pct"].fillna(0)

    day_dict = grouped.to_dict(orient="index")
    for d in day_dict:
        for k in day_dict[d]:
            day_dict[d][k] = round(float(day_dict[d][k]), 4)

    best_day = grouped["avg_return_pct"].idxmax()
    worst_day = grouped["avg_return_pct"].idxmin()

    return {
        "available": True,
        "day_returns": day_dict,
        "best_day": best_day,
        "best_day_avg_pct": round(float(grouped.loc[best_day, "avg_return_pct"]), 4),
        "worst_day": worst_day,
        "worst_day_avg_pct": round(float(grouped.loc[worst_day, "avg_return_pct"]), 4),
        "_grouped_df": grouped,
    }
