"""
Hybrid ARIMA + Exponential-Smoothing Predictor
================================================
Architecture:
  • ARIMA  handles linear trends (auto-selected order via AIC)
  • ETS    captures seasonal / exponential patterns
  • Ensemble: weighted average (60 % ARIMA, 40 % ETS)
  • Exogenous support via SARIMAX (crude oil, USD/INR)

Note: We use statsmodels only — no TF/PyTorch dependency for the
      predictive module, keeping the package footprint light.
"""
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', module='statsmodels')


class HybridPredictor:
    """ARIMA + ETS ensemble model for equity price forecasting."""

    def __init__(self):
        self._arima_result = None
        self._ets_result = None
        self._trained = False
        self._training_series = None

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
            dict with training summary (AIC, order, etc.)
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

        # --- ARIMA: auto-select order using grid search (AIC) ----------
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

        # --- ETS: Holt-Winters ---
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

        return {
            'available': self._trained,
            'arima_order': best_order,
            'arima_aic': round(best_aic, 2) if self._trained else None,
            'ets_available': self._ets_result is not None,
            'training_obs': len(series),
        }

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------
    def predict(self, days: int = 30) -> dict:
        """
        Forecast future prices.

        Returns:
            {available, forecast: [prices], ci_lower, ci_upper,
             trend, pct_change_30d}
        """
        if not self._trained:
            return {
                'available': False,
                'reason': 'Model not yet trained. Call train() first.',
            }

        # ARIMA forecast
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            arima_fc = self._arima_result.get_forecast(steps=days)
            arima_mean = arima_fc.predicted_mean.values
            arima_ci = arima_fc.conf_int()

        # ETS forecast (if available)
        if self._ets_result is not None:
            ets_mean = self._ets_result.forecast(days).values
            # Ensemble: 60 % ARIMA + 40 % ETS
            forecast = 0.6 * arima_mean + 0.4 * ets_mean
        else:
            forecast = arima_mean

        # Confidence interval from ARIMA
        ci_lower = arima_ci.iloc[:, 0].values
        ci_upper = arima_ci.iloc[:, 1].values

        last_price = float(self._training_series.iloc[-1])
        end_price = float(forecast[-1])
        pct_change = round((end_price / last_price - 1) * 100, 2)

        # Trend classification
        if pct_change > 5:
            trend = 'BULLISH'
        elif pct_change > 1:
            trend = 'MILDLY BULLISH'
        elif pct_change > -1:
            trend = 'SIDEWAYS'
        elif pct_change > -5:
            trend = 'MILDLY BEARISH'
        else:
            trend = 'BEARISH'

        return {
            'available': True,
            'days': days,
            'last_price': round(last_price, 2),
            'forecast': [round(float(f), 2) for f in forecast],
            'ci_lower':  [round(float(c), 2) for c in ci_lower],
            'ci_upper':  [round(float(c), 2) for c in ci_upper],
            'end_price': round(end_price, 2),
            'pct_change_30d': pct_change,
            'trend': trend,
        }
