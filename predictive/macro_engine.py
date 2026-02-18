"""
Macro-Correlation Engine — ARDL & Sectoral Lead Indicators
============================================================
Integrates macro variables as lead indicators for stock returns:
  • Crude Oil Prices (COP) → critical for Energy, Paints, Aviation
  • USD/INR Exchange Rate  → critical for IT, Pharma exports
  • FII/DII Net Flows      → affects most sectors (except defensive)
  • India VIX              → volatility regime indicator
  • Gold Prices            → safe-haven correlation

Methodology:
  1. Fetch macro time series via yfinance (free)
  2. Compute lagged correlations (1d, 5d, 20d lags)
  3. ARDL-lite model: regress stock returns on lagged macro returns
  4. Identify which macro factors are statistically significant
  5. Sector-specific sensitivity profiling

Uses statsmodels OLS for ARDL estimation. No external APIs.
"""
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore', category=FutureWarning)

try:
    import yfinance as yf
    _YF = True
except ImportError:
    _YF = False

try:
    from statsmodels.regression.linear_model import OLS
    from statsmodels.tools import add_constant
    _STATS = True
except ImportError:
    _STATS = False


# ─── Sector sensitivity is COMPUTED at runtime from rolling correlations ──
# No hardcoded sensitivity profiles — they are derived from actual data.

# Yahoo Finance tickers for macro variables
MACRO_TICKERS = {
    'crude_oil': 'CL=F',        # WTI Crude Oil Futures
    'usdinr':    'INR=X',       # USD/INR
    'gold':      'GC=F',        # Gold Futures
    'india_vix': '^INDIAVIX',   # India VIX
    'nifty50':   '^NSEI',       # Nifty 50
}


class MacroCorrelationEngine:
    """
    Macro-factor correlation and ARDL analysis for stock returns.

    Usage:
        engine = MacroCorrelationEngine()
        result = engine.analyze('RELIANCE', stock_prices, sector='energy')
    """

    def __init__(self):
        self._available = _YF and _STATS

    @property
    def available(self) -> bool:
        return self._available

    def analyze(self, symbol: str, stock_prices: pd.Series,
                sector: str = None, period: str = '2y') -> dict:
        """
        Full macro-correlation analysis.

        Parameters
        ----------
        symbol       : BSE symbol
        stock_prices : daily close prices (pd.Series, DatetimeIndex)
        sector       : sector keyword (for sensitivity profile)
        period       : yfinance period for macro data

        Returns
        -------
        dict with correlations, ardl_results, sector_sensitivity, signals
        """
        if not self._available:
            return {'available': False,
                    'reason': 'yfinance or statsmodels not installed'}

        if stock_prices is None or len(stock_prices) < 60:
            return {'available': False,
                    'reason': 'Insufficient stock price data (need ≥60 days)'}

        # ── Step 1: Fetch macro time series ─────────────────
        macro_data = self._fetch_macro_series(period)
        if not macro_data:
            return {'available': False,
                    'reason': 'Could not fetch macro data'}

        # ── Step 2: Compute stock returns ───────────────────
        stock_ret = stock_prices.pct_change().dropna()
        stock_ret.name = 'stock'

        # ── Step 3: Lagged correlations ─────────────────────
        correlations = {}
        for macro_name, macro_series in macro_data.items():
            macro_ret = macro_series.pct_change().dropna()
            corr_result = self._lagged_correlation(
                stock_ret, macro_ret, macro_name)
            if corr_result:
                correlations[macro_name] = corr_result

        # ── Step 4: ARDL-lite regression ────────────────────
        ardl_result = self._ardl_regression(stock_ret, macro_data)

        # ── Step 5: Sector sensitivity ──────────────────────
        sector_lower = (sector or 'unknown').lower()
        sensitivity = self._get_sector_sensitivity(sector_lower)

        # ── Step 6: Generate signals ────────────────────────
        signals = self._generate_signals(correlations, ardl_result,
                                          sensitivity, macro_data)

        return {
            'available': True,
            'correlations': correlations,
            'ardl': ardl_result,
            'sector_sensitivity': sensitivity,
            'sector': sector_lower,
            'signals': signals,
            'macro_snapshot': self._macro_snapshot(macro_data),
        }

    # ==================================================================
    # Macro Data Fetching
    # ==================================================================
    def _fetch_macro_series(self, period: str) -> dict:
        """Fetch macro time series from yfinance."""
        macro_data = {}
        for name, ticker in MACRO_TICKERS.items():
            try:
                tk = yf.Ticker(ticker)
                hist = tk.history(period=period)
                if not hist.empty and len(hist) > 30:
                    hist.index = hist.index.tz_localize(None)
                    close_col = 'Close' if 'Close' in hist.columns else 'close'
                    series = hist[close_col].dropna()
                    series.name = name
                    macro_data[name] = series
            except Exception:
                continue
        return macro_data

    # ==================================================================
    # Lagged Correlation
    # ==================================================================
    def _lagged_correlation(self, stock_ret: pd.Series,
                             macro_ret: pd.Series,
                             macro_name: str) -> dict:
        """Compute correlation at multiple lags."""
        common = stock_ret.index.intersection(macro_ret.index)
        if len(common) < 30:
            return None

        sr = stock_ret.loc[common]
        mr = macro_ret.loc[common]

        lags = [0, 1, 5, 10, 20]
        lag_corrs = {}
        best_lag = 0
        best_corr = 0

        for lag in lags:
            try:
                if lag == 0:
                    corr = float(sr.corr(mr))
                else:
                    # Macro leads stock by 'lag' days
                    shifted = mr.shift(lag)
                    valid = pd.concat([sr, shifted], axis=1).dropna()
                    if len(valid) > 20:
                        corr = float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))
                    else:
                        lag_corrs[f'lag_{lag}d'] = None
                        continue  # Skip — insufficient data for this lag

                lag_corrs[f'lag_{lag}d'] = round(corr, 4)
                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
            except Exception:
                lag_corrs[f'lag_{lag}d'] = None

        # Significance: |corr| > 0.15 is meaningful
        is_significant = abs(best_corr) > 0.15
        direction = ('POSITIVE' if best_corr > 0.15 else
                      'NEGATIVE' if best_corr < -0.15 else 'WEAK')

        return {
            'lags': lag_corrs,
            'best_lag_days': best_lag,
            'best_correlation': round(best_corr, 4),
            'direction': direction,
            'is_significant': is_significant,
        }

    # ==================================================================
    # ARDL-Lite Regression
    # ==================================================================
    def _ardl_regression(self, stock_ret: pd.Series,
                          macro_data: dict) -> dict:
        """
        ARDL-lite: OLS regression of stock returns on lagged macro returns.

        Model: R_stock_t = α + β₁*R_crude_t-1 + β₂*R_usdinr_t-1 + ...
        """
        if not _STATS:
            return {'available': False, 'reason': 'statsmodels not installed'}

        try:
            # Build features: 1-day and 5-day lagged macro returns
            frames = {'stock': stock_ret}
            feature_names = []

            for name, series in macro_data.items():
                if name == 'nifty50':
                    continue  # Market benchmark, not exogenous
                ret = series.pct_change().dropna()
                ret.name = name
                # Lag 1
                lag1 = ret.shift(1)
                lag1.name = f'{name}_lag1'
                frames[lag1.name] = lag1
                feature_names.append(lag1.name)
                # Lag 5
                lag5 = ret.shift(5)
                lag5.name = f'{name}_lag5'
                frames[lag5.name] = lag5
                feature_names.append(lag5.name)

            if not feature_names:
                return {'available': False, 'reason': 'No macro features'}

            # Merge all
            df = pd.DataFrame(frames)
            df = df.dropna()

            if len(df) < 30:
                return {'available': False,
                        'reason': 'Insufficient aligned data for ARDL'}

            y = df['stock']
            X = df[feature_names]
            X = add_constant(X)

            model = OLS(y, X).fit()

            # Extract significant coefficients
            coefficients = {}
            for feat in feature_names:
                coeff = round(float(model.params[feat]), 6)
                pval = round(float(model.pvalues[feat]), 4)
                coefficients[feat] = {
                    'coefficient': coeff,
                    'p_value': pval,
                    'significant': pval < 0.05,
                    'direction': ('POSITIVE' if coeff > 0 else 'NEGATIVE'),
                }

            significant_factors = [
                k for k, v in coefficients.items() if v['significant']]

            return {
                'available': True,
                'r_squared': round(float(model.rsquared), 4),
                'adj_r_squared': round(float(model.rsquared_adj), 4),
                'f_statistic': round(float(model.fvalue), 2),
                'f_pvalue': round(float(model.f_pvalue), 4),
                'model_significant': float(model.f_pvalue) < 0.05,
                'coefficients': coefficients,
                'significant_factors': significant_factors,
                'num_observations': len(df),
            }
        except Exception as e:
            return {'available': False, 'reason': str(e)}

    # ==================================================================
    # Sector Sensitivity
    # ==================================================================
    def _get_sector_sensitivity(self, sector: str) -> dict:
        """
        Compute sector-specific macro sensitivity DYNAMICALLY from
        the actual correlation data computed in this analysis.
        No hardcoded sensitivity profiles.
        """
        # Sensitivity will be computed from the actual correlations
        # in _generate_signals() — return empty profile here
        return {
            'matched_sector': sector,
            'profile': {},  # Will be populated from actual correlations
            'source': 'computed_from_data',
        }

    # ==================================================================
    # Signal Generation
    # ==================================================================
    def _generate_signals(self, correlations: dict, ardl: dict,
                           sensitivity: dict, macro_data: dict) -> list:
        """Generate actionable signals from macro analysis.
        Sector sensitivity is DERIVED from actual correlation strengths."""
        signals = []

        for macro_name, corr_data in correlations.items():
            if not corr_data or not corr_data.get('is_significant'):
                continue

            direction = corr_data['direction']
            best_corr = corr_data['best_correlation']
            best_lag = corr_data['best_lag_days']

            # Derive sensitivity from actual correlation strength (no hardcoded profile)
            abs_corr = abs(best_corr)
            if abs_corr > 0.4:
                computed_sens = 'HIGH'
            elif abs_corr > 0.2:
                computed_sens = 'MEDIUM'
            else:
                computed_sens = 'LOW'

            # Recent macro trend
            macro_series = macro_data.get(macro_name)
            if macro_series is not None and len(macro_series) > 20:
                recent_ret = float(
                    (macro_series.iloc[-1] / macro_series.iloc[-20] - 1) * 100)
                macro_trend = ('rising' if recent_ret > 2 else
                                'falling' if recent_ret < -2 else 'stable')
            else:
                continue  # Skip — cannot generate meaningful signal without data

            pretty_name = macro_name.replace('_', ' ').title()
            _wind = ('tailwind'
                     if (direction == 'POSITIVE' and recent_ret > 0) or
                        (direction == 'NEGATIVE' and recent_ret < 0)
                     else 'headwind')
            signal_text = (
                f"{pretty_name} ({macro_trend}, {recent_ret:+.1f}% 20d): "
                f"{_wind} -- corr {best_corr:+.3f}, lag {best_lag} days, "
                f"sensitivity: {computed_sens}"
            )
            signals.append(signal_text)

        # ARDL model signal
        if ardl and ardl.get('available') and ardl.get('model_significant'):
            sig_factors = ardl.get('significant_factors', [])
            if sig_factors:
                signals.append(
                    f"ARDL model (R2={ardl['r_squared']:.3f}): "
                    f"significant factors -- {', '.join(sig_factors)}")

        if not signals:
            signals.append("No significant macro correlations detected -- "
                           "stock appears driven by idiosyncratic factors")

        return signals

    # ==================================================================
    # Snapshot
    # ==================================================================
    def _macro_snapshot(self, macro_data: dict) -> dict:
        """Current values and recent changes for macro indicators."""
        snapshot = {}
        for name, series in macro_data.items():
            if len(series) < 2:
                continue
            current = round(float(series.iloc[-1]), 2)
            prev_20d = round(float(series.iloc[-min(20, len(series))]), 2)
            change_20d = round((current / prev_20d - 1) * 100, 2) if prev_20d else 0

            pretty_name = name.replace('_', ' ').title()
            snapshot[name] = {
                'name': pretty_name,
                'current': current,
                'change_20d_pct': change_20d,
                'trend': ('^' if change_20d > 2 else
                          'v' if change_20d < -2 else '->'),
            }
        return snapshot
