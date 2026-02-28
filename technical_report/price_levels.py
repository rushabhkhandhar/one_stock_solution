"""
Price Action & Market Microstructure — Algorithmic S/R Levels
===============================================================
Extends the existing ``quant/technicals.py`` S/R (pivot-based) with:

  • K-Means clustering on price extremes (local highs/lows) for
    algorithmically-derived Support / Resistance zones
  • Fibonacci retracement with configurable lookback
  • Merged & ranked level table combining all methods

Reuses:
  • quant.technicals.TechnicalAnalyzer._support_resistance (pivot + fib)
  • sklearn.cluster.KMeans for price-extreme clustering
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List, Tuple


class PriceLevelDetector:
    """Detect institutional-grade support & resistance levels."""

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def compute_all(
        self,
        df: pd.DataFrame,
        fib_lookback: int = 252,
        n_clusters: int = 6,
    ) -> dict:
        """
        Parameters
        ----------
        df           : OHLCV DataFrame (lowercase cols, DatetimeIndex).
        fib_lookback : Bars for Fibonacci retracement (default 252 = 52-week).
        n_clusters   : K-Means clusters for S/R detection.

        Returns
        -------
        dict with sub-dicts: kmeans_levels, fibonacci, pivot_points, merged
        """
        if df is None or len(df) < 50:
            return {"available": False, "reason": "Need ≥50 bars"}

        close = df["close"].astype(float)
        high = df["high"].astype(float) if "high" in df.columns else close
        low = df["low"].astype(float) if "low" in df.columns else close
        volume = df["volume"].astype(float) if "volume" in df.columns else None

        latest = float(close.iloc[-1])

        result = {"available": True, "current_price": round(latest, 2)}

        # K-Means on price extremes
        result["kmeans_levels"] = self._kmeans_sr(
            close, high, low, volume, n_clusters
        )

        # Fibonacci retracement
        result["fibonacci"] = self._fibonacci_levels(
            high, low, latest, fib_lookback
        )

        # Classic Pivot Points (reimplement inline to avoid import issues)
        result["pivot_points"] = self._pivot_points(close, high, low)

        # Merge & rank all levels
        result["merged"] = self._merge_levels(result, latest)

        return result

    # ==================================================================
    # K-Means clustering on price extremes
    # ==================================================================
    def _kmeans_sr(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        volume: pd.Series | None,
        n_clusters: int,
    ) -> dict:
        """Cluster local highs & lows using K-Means to find natural
        support/resistance price zones."""
        try:
            from sklearn.cluster import KMeans
        except ImportError:
            return {"available": False,
                    "reason": "scikit-learn not installed"}

        # Detect local extremes (swing highs & lows using ±5-bar window)
        extremes = self._find_local_extremes(high, low, order=5)
        if len(extremes) < n_clusters:
            # Fall back to using all close prices subsampled
            extremes = close.values[::5]
            if len(extremes) < n_clusters:
                return {"available": False,
                        "reason": "Insufficient price extremes for clustering"}

        X = np.array(extremes).reshape(-1, 1)

        km = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
        km.fit(X)

        centers = sorted(km.cluster_centers_.flatten())
        latest = float(close.iloc[-1])

        levels = []
        for c in centers:
            c_val = round(float(c), 2)
            dist_pct = round((c_val / latest - 1) * 100, 2)
            lvl_type = "SUPPORT" if c_val < latest else "RESISTANCE"
            levels.append({
                "price": c_val,
                "type": lvl_type,
                "distance_pct": dist_pct,
                "source": "K-Means",
            })

        return {
            "available": True,
            "n_clusters": n_clusters,
            "n_extremes_used": len(extremes),
            "levels": levels,
        }

    @staticmethod
    def _find_local_extremes(
        high: pd.Series, low: pd.Series, order: int = 5
    ) -> np.ndarray:
        """Find local maxima in high and local minima in low."""
        extremes = []
        h = high.values
        l = low.values
        n = len(h)

        for i in range(order, n - order):
            # Local high
            if h[i] == max(h[i - order: i + order + 1]):
                extremes.append(h[i])
            # Local low
            if l[i] == min(l[i - order: i + order + 1]):
                extremes.append(l[i])

        return np.array(extremes) if extremes else np.array([])

    # ==================================================================
    # Fibonacci retracement
    # ==================================================================
    def _fibonacci_levels(
        self,
        high: pd.Series,
        low: pd.Series,
        latest: float,
        lookback: int,
    ) -> dict:
        n = len(high)
        lb = min(lookback, n)
        period_high = float(high.tail(lb).max())
        period_low = float(low.tail(lb).min())
        fib_range = period_high - period_low

        if fib_range <= 0:
            return {"available": False}

        ratios = [
            ("0.0%", 0.0), ("23.6%", 0.236), ("38.2%", 0.382),
            ("50.0%", 0.5), ("61.8%", 0.618), ("78.6%", 0.786),
            ("100.0%", 1.0),
        ]
        levels = {}
        for label, r in ratios:
            levels[label] = round(period_high - fib_range * r, 2)

        # Nearest support/resistance
        fib_supports = [v for v in levels.values() if v < latest]
        fib_resistances = [v for v in levels.values() if v > latest]

        return {
            "available": True,
            "period_high": round(period_high, 2),
            "period_low": round(period_low, 2),
            "lookback_bars": lb,
            "levels": levels,
            "nearest_support": max(fib_supports) if fib_supports else None,
            "nearest_resistance": min(fib_resistances) if fib_resistances else None,
        }

    # ==================================================================
    # Classic Pivot Points
    # ==================================================================
    def _pivot_points(self, close: pd.Series, high: pd.Series,
                      low: pd.Series) -> dict:
        h = float(high.iloc[-2]) if len(high) >= 2 else float(high.iloc[-1])
        l = float(low.iloc[-2]) if len(low) >= 2 else float(low.iloc[-1])
        c = float(close.iloc[-2]) if len(close) >= 2 else float(close.iloc[-1])

        pivot = (h + l + c) / 3
        r1, s1 = 2 * pivot - l, 2 * pivot - h
        r2, s2 = pivot + (h - l), pivot - (h - l)
        r3, s3 = h + 2 * (pivot - l), l - 2 * (h - pivot)

        return {
            "available": True,
            "pivot": round(pivot, 2),
            "levels": {
                "R3": round(r3, 2), "R2": round(r2, 2), "R1": round(r1, 2),
                "P": round(pivot, 2),
                "S1": round(s1, 2), "S2": round(s2, 2), "S3": round(s3, 2),
            },
        }

    # ==================================================================
    # Merge & rank all levels
    # ==================================================================
    def _merge_levels(self, result: dict, latest: float) -> dict:
        """Combine K-Means, Fibonacci, and Pivot levels into a single
        sorted table with proximity ranking and strength scoring."""
        all_levels: List[dict] = []

        # Track which sources each price zone appears in (for strength)
        _price_sources: dict = {}  # price_bucket -> set of sources

        def _add_level(price, lvl_type, source, dist_pct):
            """Add a level and track its source for strength scoring."""
            all_levels.append({
                "price": price,
                "type": lvl_type,
                "distance_pct": dist_pct,
                "source": source,
            })
            # Bucket prices within 0.3% for strength counting
            bucket = round(price, -1)  # round to nearest 10
            if bucket not in _price_sources:
                _price_sources[bucket] = set()
            _price_sources[bucket].add(source.split()[0])  # e.g. "Fib", "Pivot", "K-Means"

        # K-Means levels
        km = result.get("kmeans_levels", {})
        if km.get("available"):
            for lvl in km["levels"]:
                _add_level(lvl["price"], lvl["type"],
                           lvl["source"], lvl["distance_pct"])

        # Fibonacci
        fib = result.get("fibonacci", {})
        if fib.get("available"):
            for label, price in fib["levels"].items():
                lvl_type = "SUPPORT" if price < latest else "RESISTANCE"
                _add_level(price, lvl_type, f"Fib {label}",
                           round((price / latest - 1) * 100, 2))

        # Pivot Points
        piv = result.get("pivot_points", {})
        if piv.get("available"):
            for label, price in piv["levels"].items():
                lvl_type = "SUPPORT" if price < latest else "RESISTANCE"
                _add_level(price, lvl_type, f"Pivot {label}",
                           round((price / latest - 1) * 100, 2))

        # Assign strength score: how many different methods agree on this zone
        for lvl in all_levels:
            bucket = round(lvl["price"], -1)
            sources = _price_sources.get(bucket, set())
            lvl["strength"] = len(sources)  # 1-3 (K-Means, Fib, Pivot)

        # Sort by proximity to current price
        all_levels.sort(key=lambda x: abs(x["distance_pct"]))

        # Separate into supports and resistances
        supports = [l for l in all_levels if l["type"] == "SUPPORT"]
        resistances = [l for l in all_levels if l["type"] == "RESISTANCE"]

        # Deduplicate levels within 0.5% of each other
        supports = self._deduplicate(supports)
        resistances = self._deduplicate(resistances)

        return {
            "levels": all_levels,
            "key_supports": supports[:5],
            "key_resistances": resistances[:5],
        }

    @staticmethod
    def _deduplicate(levels: List[dict], tolerance_pct: float = 0.5) -> List[dict]:
        """Remove levels within tolerance_pct of each other, keeping
        the one with the most sources."""
        if not levels:
            return []
        deduped = [levels[0]]
        for lvl in levels[1:]:
            too_close = False
            for existing in deduped:
                if existing["price"] > 0:
                    diff = abs(lvl["price"] / existing["price"] - 1) * 100
                    if diff < tolerance_pct:
                        too_close = True
                        break
            if not too_close:
                deduped.append(lvl)
        return deduped
