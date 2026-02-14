"""
Institutional Flow & Return Correlation
========================================
Computes rolling-window correlation between stock returns and
market / macro factors from yfinance.

Features:
  • Rolling 30-day correlation: stock vs Nifty50
  • Rolling 30-day correlation: stock vs sector index
  • Relative-strength (RS) trend
  • Regime detection: correlated / decorrelated phases
"""
import numpy as np
import pandas as pd


class FlowCorrelation:
    """Correlate stock returns with market & macro signals."""

    def __init__(self, window: int = 30):
        self._window = window

    @property
    def available(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------
    def compute(self, stock_prices: pd.Series,
                market_prices: pd.Series,
                sector_prices: pd.Series = None) -> dict:
        """
        Compute rolling correlation between stock and market/sector.

        Parameters:
            stock_prices  : daily close prices (pd.Series, DatetimeIndex)
            market_prices : daily close prices for benchmark (e.g., Nifty50)
            sector_prices : optional sector index daily close prices

        Returns:
            dict with correlation stats, regime, and relative strength
        """
        if stock_prices is None or market_prices is None:
            return {'available': False, 'reason': 'Missing price data'}

        stock_ret = stock_prices.pct_change().dropna()
        mkt_ret   = market_prices.pct_change().dropna()

        # Align on common dates
        common = stock_ret.index.intersection(mkt_ret.index)
        if len(common) < self._window + 5:
            return {
                'available': False,
                'reason': f'Need ≥{self._window + 5} overlapping days, '
                          f'got {len(common)}',
            }

        sr = stock_ret.loc[common]
        mr = mkt_ret.loc[common]

        # Rolling correlation
        rolling_corr = sr.rolling(self._window).corr(mr).dropna()
        current_corr = round(float(rolling_corr.iloc[-1]), 4)
        avg_corr     = round(float(rolling_corr.mean()), 4)
        min_corr     = round(float(rolling_corr.min()), 4)
        max_corr     = round(float(rolling_corr.max()), 4)

        # Regime classification
        if current_corr > 0.7:
            regime = 'HIGHLY CORRELATED — moves with market'
        elif current_corr > 0.4:
            regime = 'MODERATELY CORRELATED'
        elif current_corr > 0.1:
            regime = 'LOW CORRELATION — idiosyncratic driver'
        elif current_corr > -0.1:
            regime = 'DECORRELATED — stock-specific factors dominate'
        else:
            regime = 'NEGATIVE CORRELATION — contrarian to market'

        # Relative Strength (RS) = stock cumulative return / market cumulative return
        stock_cum = (1 + sr).cumprod()
        mkt_cum   = (1 + mr).cumprod()
        rs_line   = stock_cum / mkt_cum
        rs_trend  = 'OUTPERFORMING' if rs_line.iloc[-1] > rs_line.iloc[-self._window] \
                     else 'UNDERPERFORMING'

        result = {
            'available': True,
            'window': self._window,
            'current_corr_with_market': current_corr,
            'avg_corr': avg_corr,
            'min_corr': min_corr,
            'max_corr': max_corr,
            'regime': regime,
            'relative_strength_trend': rs_trend,
            'rs_30d_ratio': round(float(rs_line.iloc[-1] / rs_line.iloc[-self._window]), 4),
        }

        # Sector correlation (if provided)
        if sector_prices is not None:
            sect_ret = sector_prices.pct_change().dropna()
            common_s = sr.index.intersection(sect_ret.index)
            if len(common_s) >= self._window + 5:
                sec_corr = sr.loc[common_s].rolling(self._window).corr(
                    sect_ret.loc[common_s]
                ).dropna()
                result['current_corr_with_sector'] = round(float(sec_corr.iloc[-1]), 4)

        return result
