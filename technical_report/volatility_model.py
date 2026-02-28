"""
Advanced Volatility Modeling
==============================
Wraps the existing GARCH engine in ``predictive/arima_ets.py`` and adds:

  • Rolling annualised volatility (multiple windows)
  • ATR bands (upper/lower) from ``quant/technicals.py`` ATR logic
  • Volatility regime mapping (Contraction / Normal / Expansion)
  • GARCH(1,1) conditional volatility forecast (delegated)
  • Parkinson & Garman-Klass estimators for intraday-range vol

Reuses:
  • predictive.arima_ets.HybridPredictor (GARCH family)
  • quant.technicals.TechnicalAnalyzer (ATR)
"""
from __future__ import annotations

import numpy as np
import pandas as pd


class VolatilityModel:
    """Advanced volatility analytics from OHLCV data."""

    TRADING_DAYS = 252

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def compute_all(self, df: pd.DataFrame) -> dict:
        """
        Parameters
        ----------
        df : DataFrame with columns [open, high, low, close, volume]
             (lowercase, DatetimeIndex)

        Returns
        -------
        dict with sub-dicts: rolling_vol, atr_bands, regime, garch, estimators
        """
        if df is None or len(df) < 30:
            return {"available": False, "reason": "Need ≥30 bars"}

        close = df["close"].astype(float)
        high = df["high"].astype(float) if "high" in df.columns else close
        low = df["low"].astype(float) if "low" in df.columns else close
        opn = df["open"].astype(float) if "open" in df.columns else close

        result = {"available": True}
        result["rolling_vol"] = self._rolling_volatility(close)
        result["atr_bands"] = self._atr_bands(close, high, low)
        result["regime"] = self._volatility_regime(close)
        result["garch"] = self._garch_analysis(close)
        result["estimators"] = self._range_estimators(opn, high, low, close)

        return result

    # ==================================================================
    # Rolling annualised volatility
    # ==================================================================
    def _rolling_volatility(self, close: pd.Series) -> dict:
        returns = close.pct_change().dropna()
        n = len(returns)

        windows = {}
        for w_label, w in [("10d", 10), ("21d", 21), ("63d", 63), ("126d", 126), ("252d", 252)]:
            if n >= w + 5:
                rv = returns.rolling(w).std() * np.sqrt(self.TRADING_DAYS) * 100
                rv = rv.dropna()
                windows[w_label] = {
                    "current_pct": round(float(rv.iloc[-1]), 2),
                    "mean_pct": round(float(rv.mean()), 2),
                    "min_pct": round(float(rv.min()), 2),
                    "max_pct": round(float(rv.max()), 2),
                    "_series": rv,
                }

        return {
            "available": bool(windows),
            "windows": windows,
        }

    # ==================================================================
    # ATR Bands
    # ==================================================================
    def _atr_bands(self, close: pd.Series, high: pd.Series,
                   low: pd.Series, period: int = 14,
                   multiplier: float = 2.0) -> dict:
        n = len(close)
        if n < period + 1:
            return {"available": False}

        # True Range
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()

        upper_band = close + multiplier * atr
        lower_band = close - multiplier * atr
        mid_band = close.copy()

        atr_pct = (atr / close * 100).dropna()

        return {
            "available": True,
            "period": period,
            "multiplier": multiplier,
            "current_atr": round(float(atr.iloc[-1]), 2),
            "current_atr_pct": round(float(atr_pct.iloc[-1]), 2),
            "_upper_band": upper_band.dropna(),
            "_lower_band": lower_band.dropna(),
            "_mid_band": mid_band,
            "_atr_series": atr.dropna(),
        }

    # ==================================================================
    # Volatility regime mapping
    # ==================================================================
    def _volatility_regime(self, close: pd.Series) -> dict:
        """Classify current vol environment using percentile rank
        of 21-day realised vol vs its own 1-year history."""
        returns = close.pct_change().dropna()
        if len(returns) < 63:
            return {"available": False}

        rv_21 = returns.rolling(21).std() * np.sqrt(self.TRADING_DAYS) * 100
        rv_21 = rv_21.dropna()

        current = float(rv_21.iloc[-1])
        percentile = float((rv_21 < current).sum() / len(rv_21) * 100)

        if percentile <= 25:
            regime = "CONTRACTION"
            label = "Low volatility — trend continuation likely"
        elif percentile <= 75:
            regime = "NORMAL"
            label = "Average volatility — no regime signal"
        else:
            regime = "EXPANSION"
            label = "High volatility — expect larger price swings"

        return {
            "available": True,
            "regime": regime,
            "label": label,
            "current_rv_21d_pct": round(current, 2),
            "percentile": round(percentile, 1),
            "_rv_series": rv_21,
        }

    # ==================================================================
    # GARCH analysis (delegates to existing engine)
    # ==================================================================
    def _garch_analysis(self, close: pd.Series) -> dict:
        """Train GARCH via the existing HybridPredictor and extract
        conditional volatility + multi-step forecast."""
        try:
            from predictive.arima_ets import HybridPredictor
        except ImportError:
            return {"available": False,
                    "reason": "predictive.arima_ets not importable"}

        predictor = HybridPredictor()
        train_info = predictor.train(close)

        if not train_info.get("available"):
            return {
                "available": False,
                "reason": train_info.get("reason", "GARCH training failed"),
            }

        # Extract the conditional volatility series from the fitted model
        cond_vol_series = None
        if predictor._garch_result is not None:
            try:
                cond_vol_series = predictor._garch_result.conditional_volatility
            except Exception:
                pass

        # Multi-step forecast (30-day)
        vol_forecast = predictor._forecast_garch_vol(30)

        return {
            "available": True,
            "model": train_info.get("garch_model", "N/A"),
            "aic": train_info.get("garch_aic"),
            "annualised_vol_pct": train_info.get("annualised_vol_pct"),
            "conditional_vol_pct": train_info.get("conditional_vol_pct"),
            "vol_regime": train_info.get("vol_regime", "Unknown"),
            "vol_percentile": train_info.get("vol_percentile"),
            "_cond_vol_series": cond_vol_series,
            "_vol_forecast_30d": (
                [round(float(v), 4) for v in vol_forecast]
                if vol_forecast is not None else None
            ),
        }

    # ==================================================================
    # Range-based estimators (Parkinson, Garman-Klass)
    # ==================================================================
    def _range_estimators(self, opn: pd.Series, high: pd.Series,
                          low: pd.Series, close: pd.Series,
                          window: int = 21) -> dict:
        """Parkinson & Garman-Klass range-based volatility estimators."""
        n = len(close)
        if n < window + 5:
            return {"available": False}

        # Parkinson: σ² = (1 / 4·ln2) · E[(ln(H/L))²]
        log_hl = np.log(high / low)
        parkinson = (
            log_hl**2 / (4 * np.log(2))
        ).rolling(window).mean().apply(lambda x: np.sqrt(x * self.TRADING_DAYS) * 100)

        # Garman-Klass: combines O, H, L, C
        gk = (
            0.5 * np.log(high / low)**2
            - (2 * np.log(2) - 1) * np.log(close / opn)**2
        ).rolling(window).mean().apply(lambda x: np.sqrt(abs(x) * self.TRADING_DAYS) * 100)

        parkinson = parkinson.dropna()
        gk = gk.dropna()

        return {
            "available": True,
            "parkinson_vol_pct": round(float(parkinson.iloc[-1]), 2) if len(parkinson) > 0 else None,
            "garman_klass_vol_pct": round(float(gk.iloc[-1]), 2) if len(gk) > 0 else None,
            "window": window,
            "_parkinson_series": parkinson,
            "_garman_klass_series": gk,
        }
