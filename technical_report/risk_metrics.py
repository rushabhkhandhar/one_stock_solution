"""
Performance & Risk Metrics Engine
===================================
Computes institutional-grade return distribution analytics from OHLCV data.

Metrics:
  • Rolling & annualised Sharpe, Sortino, Calmar, Information Ratio
  • Maximum Drawdown (MDD) with peak/trough dates
  • Value at Risk (VaR) — historical & parametric
  • Conditional VaR / Expected Shortfall (CVaR)
  • Omega Ratio, Tail Ratio, Gain-to-Pain Ratio

Re-uses:
  • config.market.risk_free_rate (live India 10Y G-Sec yield)

No external API calls — all computed from price arrays.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


class RiskMetricsEngine:
    """Calculate performance & risk metrics from a price series."""

    TRADING_DAYS = 252

    def __init__(self, risk_free_rate: Optional[float] = None):
        """
        Parameters
        ----------
        risk_free_rate : float or None
            Annualised risk-free rate (decimal, e.g. 0.069).
            If None, attempts to read from config.
        """
        if risk_free_rate is not None:
            self.rf = risk_free_rate
        else:
            try:
                from config import config
                self.rf = config.market.risk_free_rate or 0.0
            except Exception:
                self.rf = 0.0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def compute_all(
        self,
        close: pd.Series,
        benchmark_close: Optional[pd.Series] = None,
    ) -> dict:
        """Run the full risk-metrics suite.

        Parameters
        ----------
        close           : Daily close prices (DatetimeIndex).
        benchmark_close : Optional benchmark close for Information Ratio.

        Returns
        -------
        dict  with sub-dicts: performance, drawdown, var, ratios
        """
        if close is None or len(close) < 30:
            return {"available": False, "reason": "Need ≥30 price bars"}

        returns = close.pct_change().dropna()
        log_ret = np.log(close / close.shift(1)).dropna()

        result = {"available": True}
        result["performance"] = self._performance_metrics(returns, close)
        result["drawdown"] = self._drawdown_analysis(close)
        result["var"] = self._var_cvar(returns)
        cagr = result["performance"]["cagr_pct"] / 100.0  # decimal
        result["ratios"] = self._risk_ratios(returns, benchmark_close, cagr=cagr)
        result["rolling"] = self._rolling_metrics(returns)

        return result

    # ==================================================================
    # Performance metrics
    # ==================================================================
    def _performance_metrics(self, returns: pd.Series,
                             close: pd.Series) -> dict:
        n = len(returns)
        total_return = float(close.iloc[-1] / close.iloc[0] - 1)
        years = n / self.TRADING_DAYS
        cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0.0

        ann_vol = float(returns.std() * np.sqrt(self.TRADING_DAYS))
        ann_mean = float(returns.mean() * self.TRADING_DAYS)

        # Downside deviation (Sortino denominator)
        downside = returns[returns < 0]
        downside_dev = float(downside.std() * np.sqrt(self.TRADING_DAYS)) if len(downside) > 1 else 0.0

        return {
            "total_return_pct": round(total_return * 100, 2),
            "cagr_pct": round(cagr * 100, 2),
            "annualised_mean_pct": round(ann_mean * 100, 2),
            "annualised_volatility_pct": round(ann_vol * 100, 2),
            "downside_deviation_pct": round(downside_dev * 100, 2),
            "skewness": round(float(returns.skew()), 4),
            "kurtosis": round(float(returns.kurtosis()), 4),
            "best_day_pct": round(float(returns.max()) * 100, 2),
            "worst_day_pct": round(float(returns.min()) * 100, 2),
            "positive_days_pct": round(
                float((returns > 0).sum() / len(returns)) * 100, 1
            ),
            "num_observations": n,
        }

    # ==================================================================
    # Drawdown analysis
    # ==================================================================
    def _drawdown_analysis(self, close: pd.Series) -> dict:
        cum_max = close.cummax()
        drawdown = (close - cum_max) / cum_max
        dd_series = drawdown.copy()

        mdd = float(drawdown.min())
        trough_date = drawdown.idxmin()
        peak_date = close.loc[:trough_date].idxmax()

        # Recovery date: first date after trough where price ≥ peak price
        peak_price = float(close.loc[peak_date])
        post_trough = close.loc[trough_date:]
        recovered = post_trough[post_trough >= peak_price]
        recovery_date = recovered.index[0] if len(recovered) > 0 else None

        # Duration in trading days
        dd_duration = (trough_date - peak_date).days if peak_date and trough_date else None
        recovery_duration = (
            (recovery_date - trough_date).days
            if recovery_date is not None else None
        )

        # Current drawdown from most recent peak
        current_dd = float(drawdown.iloc[-1])

        # Average drawdown
        avg_dd = float(drawdown[drawdown < 0].mean()) if (drawdown < 0).any() else 0.0

        return {
            "max_drawdown_pct": round(mdd * 100, 2),
            "peak_date": str(peak_date.date()) if hasattr(peak_date, 'date') else str(peak_date),
            "trough_date": str(trough_date.date()) if hasattr(trough_date, 'date') else str(trough_date),
            "recovery_date": (
                str(recovery_date.date())
                if recovery_date is not None and hasattr(recovery_date, 'date')
                else None
            ),
            "dd_duration_days": dd_duration,
            "recovery_duration_days": recovery_duration,
            "current_drawdown_pct": round(current_dd * 100, 2),
            "avg_drawdown_pct": round(avg_dd * 100, 2),
            # Full series for plotting
            "_dd_series": dd_series,
        }

    # ==================================================================
    # VaR & CVaR
    # ==================================================================
    def _var_cvar(self, returns: pd.Series,
                  confidence: float = 0.95) -> dict:
        alpha = 1 - confidence

        # --- Historical VaR ---
        hist_var = float(np.percentile(returns, alpha * 100))
        # CVaR = mean of returns ≤ VaR
        tail = returns[returns <= hist_var]
        hist_cvar = float(tail.mean()) if len(tail) > 0 else hist_var

        # --- Parametric (Gaussian) VaR ---
        from scipy.stats import norm
        mu = float(returns.mean())
        sigma = float(returns.std())
        z = norm.ppf(alpha)
        param_var = mu + z * sigma
        # Parametric CVaR for Gaussian: μ - σ·φ(z)/α
        param_cvar = mu - sigma * float(norm.pdf(z)) / alpha

        # --- Cornish-Fisher VaR (skew + kurtosis adjusted) ---
        s = float(returns.skew())
        k = float(returns.kurtosis())  # excess kurtosis
        z_cf = (
            z
            + (z**2 - 1) * s / 6
            + (z**3 - 3 * z) * k / 24
            - (2 * z**3 - 5 * z) * s**2 / 36
        )
        cf_var = mu + z_cf * sigma

        return {
            "confidence_level": confidence,
            "historical_var_pct": round(hist_var * 100, 4),
            "historical_cvar_pct": round(hist_cvar * 100, 4),
            "parametric_var_pct": round(param_var * 100, 4),
            "parametric_cvar_pct": round(param_cvar * 100, 4),
            "cornish_fisher_var_pct": round(cf_var * 100, 4),
            "interpretation": (
                f"At {confidence*100:.0f}% confidence, daily loss will not "
                f"exceed {abs(hist_var)*100:.2f}% (historical) or "
                f"{abs(param_var)*100:.2f}% (parametric)."
            ),
        }

    # ==================================================================
    # Risk-adjusted ratios
    # ==================================================================
    def _risk_ratios(self, returns: pd.Series,
                     benchmark_close: Optional[pd.Series] = None,
                     cagr: float = 0.0) -> dict:
        ann_mean = float(returns.mean() * self.TRADING_DAYS)
        ann_vol = float(returns.std() * np.sqrt(self.TRADING_DAYS))

        # Excess return over risk-free
        excess = ann_mean - self.rf

        # Sharpe
        sharpe = excess / ann_vol if ann_vol > 0 else 0.0

        # Sortino (downside deviation)
        downside = returns[returns < 0]
        dd_ann = float(downside.std() * np.sqrt(self.TRADING_DAYS)) if len(downside) > 1 else 1e-9
        sortino = excess / dd_ann if dd_ann > 0 else 0.0

        # Calmar = CAGR / |Max Drawdown|
        cum_max = (1 + returns).cumprod().cummax()
        drawdown = ((1 + returns).cumprod() - cum_max) / cum_max
        mdd = abs(float(drawdown.min()))
        calmar = cagr / mdd if mdd > 0 else 0.0

        # Information Ratio (vs benchmark)
        info_ratio = None
        tracking_error = None
        if benchmark_close is not None and len(benchmark_close) > 1:
            bench_ret = benchmark_close.pct_change().dropna()
            common = returns.index.intersection(bench_ret.index)
            if len(common) > 30:
                active = returns.loc[common] - bench_ret.loc[common]
                te = float(active.std() * np.sqrt(self.TRADING_DAYS))
                tracking_error = round(te * 100, 2)
                if te > 0:
                    info_ratio = round(
                        float(active.mean() * self.TRADING_DAYS) / te, 4
                    )

        # Omega Ratio (threshold = 0)
        gains = returns[returns > 0].sum()
        losses = abs(returns[returns < 0].sum())
        omega = float(gains / losses) if losses > 0 else float('inf')

        # Tail Ratio = |95th percentile| / |5th percentile|
        p95 = abs(float(np.percentile(returns, 95)))
        p5 = abs(float(np.percentile(returns, 5)))
        tail_ratio = p95 / p5 if p5 > 0 else float('inf')

        # Gain-to-Pain Ratio
        gain_to_pain = float(returns.sum() / abs(returns[returns < 0].sum())) if losses > 0 else 0.0

        return {
            "sharpe_ratio": round(sharpe, 4),
            "sortino_ratio": round(sortino, 4),
            "calmar_ratio": round(calmar, 4),
            "information_ratio": info_ratio,
            "tracking_error_pct": tracking_error,
            "omega_ratio": round(omega, 4),
            "tail_ratio": round(tail_ratio, 4),
            "gain_to_pain_ratio": round(gain_to_pain, 4),
            "risk_free_rate_used": round(self.rf, 4),
        }

    # ==================================================================
    # Rolling metrics
    # ==================================================================
    def _rolling_metrics(self, returns: pd.Series,
                         window: int = 63) -> dict:
        """63-day (~3-month) rolling Sharpe & Sortino."""
        if len(returns) < window + 10:
            return {"available": False}

        rf_daily = self.rf / self.TRADING_DAYS
        excess = returns - rf_daily

        rolling_mean = excess.rolling(window).mean() * self.TRADING_DAYS
        rolling_vol = returns.rolling(window).std() * np.sqrt(self.TRADING_DAYS)
        rolling_sharpe = rolling_mean / rolling_vol

        # Rolling Sortino
        def _rolling_sortino(r, w):
            vals = []
            for i in range(w, len(r)):
                chunk = r.iloc[i-w:i]
                down = chunk[chunk < 0]
                dd = float(down.std() * np.sqrt(252)) if len(down) > 1 else 1e-9
                ann_ex = float(chunk.mean() - rf_daily) * 252
                vals.append(ann_ex / dd if dd > 0 else 0.0)
            idx = r.index[w:]
            return pd.Series(vals, index=idx)

        rolling_sort = _rolling_sortino(returns, window)

        return {
            "available": True,
            "window_days": window,
            "_rolling_sharpe": rolling_sharpe.dropna(),
            "_rolling_sortino": rolling_sort,
            "current_rolling_sharpe": round(float(rolling_sharpe.iloc[-1]), 4)
            if not np.isnan(rolling_sharpe.iloc[-1]) else None,
            "current_rolling_sortino": round(float(rolling_sort.iloc[-1]), 4)
            if len(rolling_sort) > 0 else None,
        }
