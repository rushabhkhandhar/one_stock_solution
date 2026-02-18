"""
Real-Time Market & Macro Feeds
================================
Powered by yfinance (free, no API key).

Provides:
  • Nifty 50 daily OHLCV history
  • Stock-vs-index beta estimation (rolling)
  • Macro proxies: Crude Oil (CL=F), USD/INR (INR=X), Gold (GC=F)
  • FII/DII net flow approximation via ETF volume deltas
"""
import datetime
import pandas as pd
import numpy as np

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False


class RealtimeFeeds:
    """Fetch live / daily market data via yfinance."""

    # Yahoo Finance tickers for Indian market proxies
    NIFTY_TICKER   = "^NSEI"
    SENSEX_TICKER  = "^BSEESN"
    CRUDE_TICKER   = "CL=F"        # WTI Crude Oil futures
    USDINR_TICKER  = "INR=X"       # USD/INR exchange rate
    GOLD_TICKER    = "GC=F"        # Gold futures
    VIX_TICKER     = "^INDIAVIX"   # India VIX

    def __init__(self):
        self._available = _YF_AVAILABLE

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Nifty 50 History
    # ------------------------------------------------------------------
    def nifty50_history(self, days: int = 365,
                        period: str = None) -> pd.DataFrame:
        """Return Nifty-50 OHLCV history for the last N days."""
        if not self._available:
            return pd.DataFrame()
        try:
            tk = yf.Ticker(self.NIFTY_TICKER)
            if period:
                df = tk.history(period=period)
            else:
                end = datetime.date.today()
                start = end - datetime.timedelta(days=days)
                df = tk.history(start=str(start), end=str(end))
            if df.empty:
                return pd.DataFrame()
            df.index = df.index.tz_localize(None)
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            return df[['open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            print(f"  ⚠ Nifty50 fetch failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Stock price history (for beta calculation)
    # ------------------------------------------------------------------
    def stock_history(self, bse_symbol: str,
                      days: int = 365,
                      period: str = None) -> pd.DataFrame:
        """Return daily OHLCV for a BSE-listed stock."""
        if not self._available:
            return pd.DataFrame()
        try:
            ticker = f"{bse_symbol}.BO"
            tk = yf.Ticker(ticker)
            if period:
                df = tk.history(period=period)
            else:
                end = datetime.date.today()
                start = end - datetime.timedelta(days=days)
                df = tk.history(start=str(start), end=str(end))
            if df.empty:
                return pd.DataFrame()
            df.index = df.index.tz_localize(None)
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            return df[['open', 'high', 'low', 'close', 'volume']]
        except Exception:
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Beta estimation (stock vs Nifty)
    # ------------------------------------------------------------------
    def estimate_beta(self, bse_symbol: str,
                      days: int = 365) -> dict:
        """
        Compute beta of a stock against Nifty 50.

        β = Cov(Rₛ, Rₘ) / Var(Rₘ)
        """
        if not self._available:
            return {'available': False,
                    'reason': 'yfinance not installed'}
        try:
            nifty = self.nifty50_history(days)
            stock = self.stock_history(bse_symbol, days)
            if nifty.empty or stock.empty or len(nifty) < 30:
                return {'available': False,
                        'reason': 'Insufficient price data'}

            # Align dates
            common = nifty.index.intersection(stock.index)
            if len(common) < 30:
                return {'available': False,
                        'reason': 'Not enough overlapping dates'}

            r_market = nifty.loc[common, 'close'].pct_change().dropna()
            r_stock  = stock.loc[common, 'close'].pct_change().dropna()

            # Align after pct_change
            common2 = r_market.index.intersection(r_stock.index)
            r_market = r_market.loc[common2]
            r_stock  = r_stock.loc[common2]

            cov = np.cov(r_stock, r_market)
            if cov[1, 1] == 0:
                return {'available': False,
                        'reason': 'Zero market variance — cannot compute beta'}
            beta = round(float(cov[0, 1] / cov[1, 1]), 3)
            # Report raw beta — no artificial clipping
            raw_beta = beta
            beta_clipped = False

            # R-squared
            corr = np.corrcoef(r_stock, r_market)[0, 1]
            r_squared = round(corr ** 2, 3)

            return {
                'available': True,
                'beta': beta,
                'beta_clipped': beta_clipped,
                'r_squared': r_squared,
                'data_points': len(common2),
                'source': 'yfinance (BSE daily)',
            }
        except Exception as e:
            return {'available': False, 'reason': str(e)}

    # ------------------------------------------------------------------
    # Macro indicators
    # ------------------------------------------------------------------
    def macro_indicators(self) -> dict:
        """
        Return latest macro variables used as ARIMAX exogenous features.
        All sourced from yfinance (free).
        """
        if not self._available:
            return {'available': False}

        result = {'available': True}
        tickers = {
            'crude_oil_usd': self.CRUDE_TICKER,
            'usdinr':        self.USDINR_TICKER,
            'gold_usd':      self.GOLD_TICKER,
            'india_vix':     self.VIX_TICKER,
        }
        for key, sym in tickers.items():
            try:
                tk = yf.Ticker(sym)
                hist = tk.history(period='5d')
                if not hist.empty:
                    result[key] = round(float(hist['Close'].iloc[-1]), 2)
                else:
                    result[key] = None
            except Exception:
                result[key] = None

        # Nifty 50 latest
        try:
            nifty = yf.Ticker(self.NIFTY_TICKER)
            h = nifty.history(period='5d')
            if not h.empty:
                result['nifty50'] = round(float(h['Close'].iloc[-1]), 2)
                if len(h) >= 2:
                    pct = (h['Close'].iloc[-1] / h['Close'].iloc[-2] - 1) * 100
                    result['nifty50_change_pct'] = round(float(pct), 2)
        except Exception:
            result['nifty50'] = None

        return result

    # ------------------------------------------------------------------
    # Live Risk-Free Rate (10Y India G-Sec Yield)
    # ------------------------------------------------------------------
    def live_risk_free_rate(self) -> dict:
        """
        Fetch the current India 10-year government bond yield.
        Primary: scrape RBI / CCIL public data.
        Fallback: yfinance India 10Y bond proxy (^IRX equivalent).
        Returns dict with 'rate' (decimal, e.g. 0.069 for 6.9%).
        """
        import urllib.request
        import json
        import re

        # ── Method 1: Scrape Investing.com India 10Y page ─────────
        try:
            url = 'https://www.worldgovernmentbonds.com/bond-historical-data/india/10-years/'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode('utf-8', errors='ignore')
            # Look for yield value in the page — pattern: India 10 Years ... X.XX%
            m = re.search(r'<td[^>]*>\s*([0-9]+\.\s*[0-9]+)\s*%', html)
            if m:
                rate = float(m.group(1).replace(' ', '')) / 100.0
                if 0.03 < rate < 0.15:  # sanity check (3%-15%)
                    return {'available': True, 'rate': round(rate, 4),
                            'source': 'worldgovernmentbonds.com'}
        except Exception:
            pass

        # ── Method 2: yfinance — US 10Y + live India-US spread ─────
        if self._available:
            try:
                # Fetch US 10Y yield
                tk_us = yf.Ticker('^TNX')
                h_us = tk_us.history(period='5d')
                if not h_us.empty:
                    us_10y = float(h_us['Close'].iloc[-1]) / 100.0

                    # Compute India-US spread live from India vs US bond ETF proxy
                    # Try fetching India 10Y via Investing.com ticker or use
                    # historical average spread from bond market data
                    india_spread = self._fetch_live_india_us_spread(us_10y)
                    rate = us_10y + india_spread
                    if 0.03 < rate < 0.15:
                        return {'available': True,
                                'rate': round(rate, 4),
                                'source': f'yfinance (US 10Y {us_10y:.4f} + India spread {india_spread:.4f})'}
            except Exception:
                pass

        return {'available': False, 'reason': 'Could not fetch live risk-free rate'}

    def _fetch_live_india_us_spread(self, us_10y: float) -> float:
        """
        Compute India-US 10Y bond yield spread from live data.
        Primary: scrape worldgovernmentbonds for both yields.
        Fallback: derive from INR depreciation trend (covered interest parity).
        """
        import urllib.request
        import re as _re

        # Method 1: scrape India yield directly and subtract US
        try:
            url = 'https://www.worldgovernmentbonds.com/country/india/'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode('utf-8', errors='ignore')
            m = _re.search(r'10\s*(?:year|Y)[^<]*?(\d+\.\d+)\s*%', html)
            if m:
                india_10y = float(m.group(1)) / 100.0
                spread = india_10y - us_10y
                if 0.01 < spread < 0.06:
                    return round(spread, 4)
        except Exception:
            pass

        # Method 2: covered interest parity — INR depreciation trend
        try:
            tk = yf.Ticker('INR=X')
            hist = tk.history(period='2y')
            if hist is not None and len(hist) > 200:
                start = float(hist['Close'].iloc[0])
                end = float(hist['Close'].iloc[-1])
                years = len(hist) / 252.0
                inr_depreciation = (end / start) ** (1 / years) - 1
                # Covered interest parity: spread ≈ INR depreciation rate
                if 0.005 < inr_depreciation < 0.08:
                    return round(inr_depreciation, 4)
        except Exception:
            pass

        # Cannot determine spread — return None so callers know it's unavailable
        return None

    # ------------------------------------------------------------------
    # Sector / Peer ticker lookup
    # ------------------------------------------------------------------
    def get_sector_peers(self, bse_symbol: str) -> dict:
        """Return sector and industry info for a stock."""
        if not self._available:
            return {'available': False}
        try:
            tk = yf.Ticker(f"{bse_symbol}.BO")
            info = tk.info or {}
            return {
                'available': True,
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'market_cap': info.get('marketCap'),
                'pe_trailing': info.get('trailingPE'),
                'pe_forward': info.get('forwardPE'),
                'ev_ebitda': info.get('enterpriseToEbitda'),
                'dividend_yield': info.get('dividendYield'),
                'source': 'yfinance',
            }
        except Exception:
            return {'available': False}
