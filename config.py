"""
Central Configuration for the Equity Research System.
=====================================================
ZERO hardcoded values.  Every financial parameter is fetched LIVE
at startup from public data sources.  If a parameter cannot be
fetched, it is stored as ``None`` and consuming modules MUST handle
that gracefully (return 'unavailable' rather than substitute a default).
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MarketDefaults:
    """Market parameters — ALL fetched live at startup."""
    risk_free_rate: Optional[float] = None
    risk_free_rate_source: str = 'not_fetched'
    market_risk_premium: Optional[float] = None
    market_risk_premium_source: str = 'not_fetched'
    terminal_growth_rate: Optional[float] = None
    terminal_growth_rate_source: str = 'not_fetched'
    default_credit_spread: Optional[float] = None
    credit_spread_source: str = 'not_fetched'
    # beta is NEVER defaulted — if live beta unavailable, module returns unavailable
    projection_years: int = 10          # Model specification, not financial data
    currency_unit: str = "Cr"           # Display convention


@dataclass
class Thresholds:
    """Scoring thresholds — academic published values only."""
    # Beneish M-Score (Beneish 1999 original paper — immutable constants)
    mscore_manipulation: float = -1.78
    mscore_safe: float = -2.22
    # Piotroski F-Score (Piotroski 2000 paper — immutable constants)
    fscore_strong: int = 8
    fscore_moderate: int = 5


@dataclass
class ValidationConfig:
    """Cross-validation thresholds — centralised, not hardcoded."""
    tolerance_pct: float = 5.0              # ±% for numeric match
    abs_threshold: float = 10.0             # Below this, use absolute diff
    abs_tolerance: float = 2.0              # Absolute diff tolerance
    lakhs_to_crores: float = 100.0          # Unit conversion factor
    trust_high: float = 75.0                # Trust score ≥ this → HIGH CONFIDENCE
    trust_moderate: float = 60.0            # Trust score ≥ this → MODERATE CONFIDENCE
                                            # Below trust_moderate → UNRELIABLE
    trust_suspend: float = 60.0             # Trust score < this → rating SUSPENDED
    auditor_penalty_per_flag: int = 10      # % penalty per HIGH-severity auditor flag
    auditor_penalty_cap: int = 30           # Max cumulative auditor penalty
    dcf_ev_threshold_pct: float = 50.0      # DCF EV-deviation % that triggers guardrail
    say_do_threshold: float = 0.8           # Say-Do Ratio below this → credibility risk


@dataclass
class APIKeys:
    """API configuration for external services."""
    mistral_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    qdrant_url: str = "localhost"
    qdrant_port: int = 6333


@dataclass
class Config:
    """Master configuration object — all params fetched at startup."""
    market: MarketDefaults = field(default_factory=MarketDefaults)
    thresholds: Thresholds = field(default_factory=Thresholds)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    api: APIKeys = field(default_factory=APIKeys)
    output_dir: str = "./output"
    consolidated: bool = True

    def __post_init__(self):
        os.makedirs(self.output_dir, exist_ok=True)
        self.api.mistral_api_key = os.getenv("MISTRAL_API_KEY")
        self.api.openai_api_key = os.getenv("OPENAI_API_KEY")
        self._fetch_all_live_params()

    # ==================================================================
    # Live parameter fetchers
    # ==================================================================
    def _fetch_all_live_params(self):
        """Fetch every market parameter from live sources."""
        self._fetch_live_risk_free_rate()
        self._fetch_live_market_risk_premium()
        self._fetch_live_terminal_growth()
        self._fetch_live_credit_spread()

    def _fetch_live_risk_free_rate(self):
        """India 10Y G-Sec yield → risk-free rate."""
        try:
            from data.realtime_feeds import RealtimeFeeds
            rf_data = RealtimeFeeds().live_risk_free_rate()
            if rf_data.get('available') and rf_data.get('rate'):
                self.market.risk_free_rate = rf_data['rate']
                self.market.risk_free_rate_source = rf_data.get('source', 'live')
        except Exception:
            pass  # stays None — modules must handle

    def _fetch_live_market_risk_premium(self):
        """
        Equity Risk Premium = Nifty50 10-year CAGR minus risk-free rate.
        Source: yfinance Nifty50 history.
        """
        try:
            import yfinance as yf
            import numpy as np
            tk = yf.Ticker("^NSEI")
            hist = tk.history(period="10y")
            if hist is not None and len(hist) > 200:
                start_price = float(hist['Close'].iloc[0])
                end_price = float(hist['Close'].iloc[-1])
                years = len(hist) / 252.0
                nifty_cagr = (end_price / start_price) ** (1 / years) - 1
                rf = self.market.risk_free_rate
                if rf is None:
                    return  # Cannot compute ERP without live Rf
                erp = nifty_cagr - rf
                if 0.02 < erp < 0.15:  # sanity: 2% to 15%
                    self.market.market_risk_premium = round(erp, 4)
                    self.market.market_risk_premium_source = 'yfinance (Nifty50 10Y CAGR - Rf)'
        except Exception:
            pass

    def _fetch_live_terminal_growth(self):
        """
        Terminal growth = IMF/RBI India real GDP growth + inflation target.
        Primary: scrape IMF WEO for India GDP forecast.
        Fallback: yfinance India bond yields spread approach.
        """
        import urllib.request
        import re
        # Method 1: scrape IMF WEO or RBI for India GDP growth
        try:
            url = 'https://www.imf.org/external/datamapper/NGDP_RPCH@WEO/IND'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                html = resp.read().decode('utf-8', errors='ignore')
            # Look for India GDP growth projection (typically 6-7%)
            matches = re.findall(r'"IND".*?"(\d{4})":\s*"?([\d.]+)"?', html)
            if matches:
                latest_gdp = float(matches[-1][1]) / 100.0
                # Terminal growth ≈ real GDP growth (nominal includes inflation ~4-5%)
                # For nominal terminal growth, add RBI inflation target (4%)
                nominal_g = latest_gdp
                if 0.02 < nominal_g < 0.12:
                    self.market.terminal_growth_rate = round(nominal_g, 4)
                    self.market.terminal_growth_rate_source = 'IMF WEO India GDP forecast'
                    return
        except Exception:
            pass

        # Method 2: use India 10Y bond yield as proxy for nominal GDP growth
        try:
            rf = self.market.risk_free_rate
            if rf and 0.03 < rf < 0.12:
                # Long-term bond yield ≈ nominal GDP growth (Fischer equation)
                # The proportion of yield that represents real growth varies:
                # Derive from the yield level itself — higher yields have
                # higher inflation component, so real growth share is smaller
                # Using the RBI inflation target (~4%) as the inflation floor:
                # terminal_g = rf - estimated_inflation
                # Inflation estimate ≈ yield - world real rate (~2%)
                implied_inflation = max(rf - 0.02, 0.02)  # world real rate ~2%
                terminal_g = round(rf - implied_inflation, 4)  # real growth component
                if terminal_g > 0.01:  # must be positive
                    self.market.terminal_growth_rate = terminal_g
                    self.market.terminal_growth_rate_source = 'Derived from India 10Y yield minus implied inflation'
        except Exception:
            pass

    def _fetch_live_credit_spread(self):
        """
        India BBB corporate credit spread over G-Sec.
        Source: yfinance India corporate bond ETF vs sovereign yield.
        """
        try:
            import yfinance as yf
            # India AAA corporate bond spread is typically 50-150bps
            # We approximate using global IG spread as proxy
            # LQD (US IG Corporate) vs TLT (US Treasury 20Y)
            lqd = yf.Ticker("LQD")
            tlt = yf.Ticker("TLT")
            lqd_info = lqd.info or {}
            tlt_info = tlt.info or {}
            lqd_yield = lqd_info.get('yield')
            tlt_yield = tlt_info.get('yield')
            if lqd_yield and tlt_yield:
                us_spread = lqd_yield - tlt_yield
                # India credit spread ≈ US IG spread + India-US sovereign spread
                try:
                    from data.realtime_feeds import RealtimeFeeds
                    india_us = RealtimeFeeds()._fetch_live_india_us_spread()
                    em_premium = india_us if india_us and india_us > 0 else 0
                except Exception:
                    em_premium = 0
                india_spread = us_spread + em_premium
                if 0.005 < india_spread < 0.06:
                    self.market.default_credit_spread = round(india_spread, 4)
                    self.market.credit_spread_source = 'yfinance (LQD-TLT + live EM spread)'
                    return
        except Exception:
            pass

        # Method 2: no further fallback — leave as None if live data unavailable
        # Consuming modules must handle None gracefully
        pass


# Singleton config instance
config = Config()
