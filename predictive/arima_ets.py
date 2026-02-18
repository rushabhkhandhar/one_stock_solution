"""
Advanced Hybrid Predictor — ARIMA + ETS + GARCH Family
=======================================================
Architecture:
  • ARIMA  — linear mean equation (auto-selected order via AIC grid search)
  • ETS    — Holt-Winters exponential smoothing for trends
  • GARCH  — family of conditional volatility models:
      – GARCH(1,1)   : symmetric volatility clustering
      – EGARCH(1,1)  : asymmetric (leverage) — bad news increases vol more
      – GJR-GARCH(1,1): Glosten-Jagannathan-Runkle asymmetric variant
  • Ensemble: AIC-weighted softmax across mean models + best GARCH for
    volatility-adjusted confidence intervals
  • Volatility regime detection (Low / Medium / High)

Dependencies:
  statsmodels  — ARIMA / ETS
  arch         — GARCH family (pip install arch)
"""
import warnings
import math
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', module='statsmodels')
warnings.filterwarnings('ignore', module='arch')


class HybridPredictor:
    """ARIMA + ETS + GARCH-family ensemble model for equity price forecasting."""

    def __init__(self):
        self._arima_result = None
        self._ets_result = None
        self._garch_result = None
        self._garch_model_name = None   # 'GARCH', 'EGARCH', or 'GJR-GARCH'
        self._trained = False
        self._training_series = None
        self._arima_weight = None
        self._ets_weight = None
        self._vol_regime = None          # LOW / MEDIUM / HIGH
        self._annualised_vol = None
        self._conditional_vol = None     # last conditional sigma from GARCH
        self._returns = None

    @property
    def available(self) -> bool:
        return self._trained

    # ------------------------------------------------------------------
    # Train
    # ------------------------------------------------------------------
    def train(self, price_series, exog=None) -> dict:
        """
        Train on historical price data.

        Parameters:
            price_series : pd.Series or array-like of daily close prices
                           (min 60 observations recommended)
            exog         : optional pd.DataFrame of exogenous variables
                           (same length as price_series)

        Returns:
            dict with training summary (AIC, order, GARCH model, vol regime, etc.)
        """
        try:
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
        except ImportError:
            return {
                'available': False,
                'reason': 'statsmodels not installed. Run: pip install statsmodels',
            }

        series = pd.Series(price_series).dropna().astype(float)
        if len(series) < 30:
            return {
                'available': False,
                'reason': f'Need ≥30 observations, got {len(series)}',
            }

        self._training_series = series.reset_index(drop=True)
        self._returns = series.pct_change().dropna()

        # ── 1. ARIMA: auto-select order using AIC grid search ─────────
        best_aic = np.inf
        best_order = (1, 1, 1)

        for p in range(0, 4):
            for q in range(0, 4):
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        model = SARIMAX(
                            series,
                            exog=exog,
                            order=(p, 1, q),
                            enforce_stationarity=False,
                            enforce_invertibility=False,
                        )
                        result = model.fit(disp=False, maxiter=100)
                        if result.aic < best_aic:
                            best_aic = result.aic
                            best_order = (p, 1, q)
                            self._arima_result = result
                except Exception:
                    continue

        # ── 2. ETS: Holt-Winters ──────────────────────────────────────
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                ets_model = ExponentialSmoothing(
                    series,
                    trend='add',
                    seasonal=None,
                    damped_trend=True,
                )
                self._ets_result = ets_model.fit(optimized=True)
        except Exception:
            self._ets_result = None

        self._trained = self._arima_result is not None
        self._exog = exog

        # ── 3. AIC-weighted ensemble for mean models ──────────────────
        if self._arima_result is not None and self._ets_result is not None:
            arima_aic = self._arima_result.aic
            ets_aic = self._ets_result.aic
            min_aic = min(arima_aic, ets_aic)
            w_arima = math.exp(-(arima_aic - min_aic) / 2)
            w_ets = math.exp(-(ets_aic - min_aic) / 2)
            total_w = w_arima + w_ets
            self._arima_weight = w_arima / total_w
            self._ets_weight = w_ets / total_w
        else:
            self._arima_weight = 1.0
            self._ets_weight = 0.0

        # ── 4. GARCH-family volatility modelling ──────────────────────
        garch_summary = self._train_garch_family(series)

        # ── 5. Volatility regime detection ────────────────────────────
        vol_regime_info = self._detect_vol_regime(series)

        return {
            'available': self._trained,
            'arima_order': best_order,
            'arima_aic': round(best_aic, 2) if self._trained else None,
            'ets_available': self._ets_result is not None,
            'arima_weight': round(self._arima_weight, 3),
            'ets_weight': round(self._ets_weight, 3),
            'training_obs': len(series),
            # GARCH info
            'garch_model': garch_summary.get('best_model', 'N/A'),
            'garch_aic': garch_summary.get('best_aic'),
            'garch_available': garch_summary.get('available', False),
            'annualised_vol_pct': garch_summary.get('annualised_vol_pct'),
            'conditional_vol_pct': garch_summary.get('conditional_vol_pct'),
            # Volatility regime
            'vol_regime': vol_regime_info.get('regime', 'Unknown'),
            'vol_percentile': vol_regime_info.get('percentile'),
        }

    # ------------------------------------------------------------------
    # GARCH family training
    # ------------------------------------------------------------------
    def _train_garch_family(self, price_series: pd.Series) -> dict:
        """
        Fit GARCH(1,1), EGARCH(1,1), and GJR-GARCH(1,1) on log returns.
        Select the best by AIC. Provides conditional volatility forecasts.
        """
        try:
            from arch import arch_model
        except ImportError:
            # arch not installed — graceful degradation
            self._garch_result = None
            self._garch_model_name = None
            self._annualised_vol = None
            self._conditional_vol = None
            return {
                'available': False,
                'reason': 'arch library not installed. Run: pip install arch',
            }

        # Compute percentage log returns (arch expects * 100 scale)
        log_returns = (np.log(price_series / price_series.shift(1))
                       .dropna() * 100)

        if len(log_returns) < 30:
            return {'available': False,
                    'reason': 'Not enough data for GARCH'}

        # Candidate models
        candidates = [
            ('GARCH',     {'vol': 'GARCH',  'p': 1, 'q': 1}),
            ('EGARCH',    {'vol': 'EGARCH', 'p': 1, 'q': 1}),
            ('GJR-GARCH', {'vol': 'GARCH',  'p': 1, 'o': 1, 'q': 1}),
        ]

        best_aic = np.inf
        best_name = None
        best_result = None

        for name, params in candidates:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    am = arch_model(
                        log_returns,
                        mean='ARX',
                        lags=1,
                        **params,
                        dist='StudentsT',   # fat tails — better for finance
                    )
                    res = am.fit(disp='off', show_warning=False)
                    if res.aic < best_aic:
                        best_aic = res.aic
                        best_name = name
                        best_result = res
            except Exception:
                continue

        if best_result is None:
            self._garch_result = None
            self._garch_model_name = None
            return {'available': False, 'reason': 'All GARCH models failed'}

        self._garch_result = best_result
        self._garch_model_name = best_name

        # Extract conditional volatility
        cond_vol = best_result.conditional_volatility
        last_cond_vol = float(cond_vol.iloc[-1]) if len(cond_vol) > 0 else None

        # Annualise: daily σ * √252  (already in % terms)
        ann_vol = float(cond_vol.mean() * np.sqrt(252)) if len(cond_vol) > 0 else None

        self._annualised_vol = ann_vol
        self._conditional_vol = last_cond_vol

        return {
            'available': True,
            'best_model': best_name,
            'best_aic': round(best_aic, 2),
            'annualised_vol_pct': round(ann_vol, 2) if ann_vol else None,
            'conditional_vol_pct': round(last_cond_vol, 2) if last_cond_vol else None,
        }

    # ------------------------------------------------------------------
    # Volatility regime detection
    # ------------------------------------------------------------------
    def _detect_vol_regime(self, price_series: pd.Series) -> dict:
        """
        Classify current volatility regime as LOW / MEDIUM / HIGH
        using rolling 21-day realised volatility vs its own history.
        """
        returns = price_series.pct_change().dropna()
        if len(returns) < 42:
            self._vol_regime = 'Unknown'
            return {'regime': 'Unknown', 'percentile': None}

        # Rolling 21-day (1 month) realised vol, annualised
        rolling_vol = returns.rolling(21).std() * np.sqrt(252) * 100
        rolling_vol = rolling_vol.dropna()

        if len(rolling_vol) < 10:
            self._vol_regime = 'Unknown'
            return {'regime': 'Unknown', 'percentile': None}

        current_vol = float(rolling_vol.iloc[-1])
        percentile = float(
            (rolling_vol < current_vol).sum() / len(rolling_vol) * 100
        )

        if percentile >= 80:
            regime = 'HIGH'
        elif percentile >= 40:
            regime = 'MEDIUM'
        else:
            regime = 'LOW'

        self._vol_regime = regime
        return {
            'regime': regime,
            'percentile': round(percentile, 1),
            'current_rv_pct': round(current_vol, 2),
        }

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------
    def predict(self, days: int = 30) -> dict:
        """
        Forecast future prices with GARCH-enhanced confidence intervals.

        Returns:
            {available, forecast, ci_lower, ci_upper,
             trend, pct_change_30d, vol_regime, garch_model,
             annualised_vol_pct, conditional_vol_pct}
        """
        if not self._trained:
            return {
                'available': False,
                'reason': 'Model not yet trained. Call train() first.',
            }

        # ── Mean forecast: ARIMA + ETS ensemble ───────────────────────
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            arima_fc = self._arima_result.get_forecast(steps=days)
            arima_mean = arima_fc.predicted_mean.values
            arima_ci = arima_fc.conf_int()

        if self._ets_result is not None:
            ets_mean = self._ets_result.forecast(days).values
            forecast = self._arima_weight * arima_mean + self._ets_weight * ets_mean
        else:
            forecast = arima_mean

        # ── Confidence intervals: GARCH-enhanced if available ─────────
        ci_lower, ci_upper = self._compute_confidence_intervals(
            forecast, arima_ci, days
        )

        last_price = float(self._training_series.iloc[-1])
        end_price = float(forecast[-1])
        pct_change = round((end_price / last_price - 1) * 100, 2)

        # ── Trend classification — volatility-relative thresholds ─────
        returns = self._training_series.pct_change().dropna()
        monthly_vol = float(returns.std() * np.sqrt(21) * 100)
        strong_thresh = max(monthly_vol, 1.0)
        mild_thresh = strong_thresh * 0.2
        if pct_change > strong_thresh:
            trend = 'BULLISH'
        elif pct_change > mild_thresh:
            trend = 'MILDLY BULLISH'
        elif pct_change > -mild_thresh:
            trend = 'SIDEWAYS'
        elif pct_change > -strong_thresh:
            trend = 'MILDLY BEARISH'
        else:
            trend = 'BEARISH'

        result = {
            'available': True,
            'days': days,
            'last_price': round(last_price, 2),
            'forecast': [round(float(f), 2) for f in forecast],
            'ci_lower':  [round(float(c), 2) for c in ci_lower],
            'ci_upper':  [round(float(c), 2) for c in ci_upper],
            'end_price': round(end_price, 2),
            'pct_change_30d': pct_change,
            'trend': trend,
            # GARCH-enriched fields
            'vol_regime': self._vol_regime or 'Unknown',
            'garch_model': self._garch_model_name or 'N/A',
            'annualised_vol_pct': (round(self._annualised_vol, 2)
                                   if self._annualised_vol else None),
            'conditional_vol_pct': (round(self._conditional_vol, 2)
                                    if self._conditional_vol else None),
        }

        # ── GARCH multi-step volatility forecast ──────────────────────
        garch_vol_fc = self._forecast_garch_vol(days)
        if garch_vol_fc is not None:
            result['garch_vol_forecast'] = [
                round(float(v), 4) for v in garch_vol_fc
            ]

        return result

    # ------------------------------------------------------------------
    # GARCH-enhanced confidence intervals
    # ------------------------------------------------------------------
    def _compute_confidence_intervals(self, forecast, arima_ci, days):
        """
        If GARCH is available, use its multi-step conditional variance
        to widen/narrow CIs dynamically.  Otherwise fall back to ARIMA CI.
        """
        ci_lower_arima = arima_ci.iloc[:, 0].values
        ci_upper_arima = arima_ci.iloc[:, 1].values

        garch_vol_fc = self._forecast_garch_vol(days)
        if garch_vol_fc is None:
            return ci_lower_arima, ci_upper_arima

        # GARCH conditional σ is in % of log returns.
        # Convert to price-level CIs:  forecast ± 1.96 * σ_t * price_t / 100
        ci_lower = np.empty(days)
        ci_upper = np.empty(days)
        for t in range(days):
            price_t = forecast[t]
            sigma_pct = garch_vol_fc[t]             # daily σ in %
            sigma_price = price_t * sigma_pct / 100
            ci_lower[t] = price_t - 1.96 * sigma_price
            ci_upper[t] = price_t + 1.96 * sigma_price

        return ci_lower, ci_upper

    def _forecast_garch_vol(self, days: int):
        """Return multi-step GARCH volatility forecast (daily σ in %)."""
        if self._garch_result is None:
            return None
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                fc = self._garch_result.forecast(horizon=days)
                # variance forecast → σ
                var_fc = fc.variance.iloc[-1].values   # shape (days,)
                return np.sqrt(var_fc)
        except Exception:
            return None
