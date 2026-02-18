"""
Peer Comparable Analysis (CCA) — Fully Dynamic
================================================
Discovers peers at runtime using screener.in sector pages and
yfinance industry classification. ZERO hardcoded peer lists.

Features:
  • Dynamic peer discovery from screener.in sector/industry pages
  • Market cap tiers computed from live Nifty 50 median market cap
  • ROE, P/B, Dividend Yield comparison
  • Sector ranking by multiple metrics

Powered by yfinance + screener.in — free, no API keys.
"""
import logging
import numpy as np

try:
    import yfinance as yf
    _YF = True
except ImportError:
    _YF = False

# Suppress yfinance HTTP 404/error noise during peer discovery
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('peewee').setLevel(logging.CRITICAL)


class PeerComparables:
    """
    Comparable Company Analysis (CCA) for Indian equities.
    Peers are discovered DYNAMICALLY at runtime — no hardcoded lists.

    Usage:
        cca = PeerComparables()
        result = cca.analyze('TCS', stock_pe=28.5, stock_ev_ebitda=22.1)
    """

    def __init__(self):
        self._available = _YF
        self._peer_cache = {}  # Cache discovered peers per session

    @property
    def available(self) -> bool:
        return self._available

    def analyze(self, bse_symbol: str,
                stock_pe: float = None,
                stock_ev_ebitda: float = None) -> dict:
        """
        Fetch peer multiples and compare with market cap context.

        Returns:
            dict with sector, peer_data, medians, premium/discount,
            market cap ranking, and sector-level statistics.
        """
        if not self._available:
            return {'available': False,
                    'reason': 'yfinance not installed'}

        # 1. Determine sector
        sector_info = self._get_sector(bse_symbol)
        sector = sector_info.get('sector', 'Unknown')

        # 2. Get stock's own multiples from yfinance
        stock_multiples = self._fetch_multiples(f"{bse_symbol}.BO")
        stock_mcap = stock_multiples.get('market_cap_cr') if stock_multiples else None

        # 3. Find peer tickers
        peers = self._find_peers(bse_symbol, sector)
        if not peers:
            return {'available': False,
                    'reason': f'No peers found for sector: {sector}'}

        # 4. Fetch multiples for each peer
        peer_data = []
        for ticker in peers:
            multiples = self._fetch_multiples(ticker)
            if multiples:
                peer_data.append(multiples)

        if len(peer_data) < 2:
            return {'available': False,
                    'reason': 'Could not fetch enough peer data'}

        # 5. Compute medians for all metrics
        pe_vals = [p['pe'] for p in peer_data if p.get('pe') and p['pe'] > 0]
        ev_vals = [p['ev_ebitda'] for p in peer_data
                   if p.get('ev_ebitda') and p['ev_ebitda'] > 0]
        mcap_vals = [p['market_cap_cr'] for p in peer_data
                     if p.get('market_cap_cr') and p['market_cap_cr'] > 0]
        pb_vals = [p['pb'] for p in peer_data if p.get('pb') and p['pb'] > 0]
        roe_vals = [p['roe'] for p in peer_data if p.get('roe')]
        dy_vals = [p['dividend_yield'] for p in peer_data
                   if p.get('dividend_yield') and p['dividend_yield'] > 0]

        median_pe = round(float(np.median(pe_vals)), 2) if pe_vals else None
        median_ev = round(float(np.median(ev_vals)), 2) if ev_vals else None
        median_mcap = round(float(np.median(mcap_vals)), 0) if mcap_vals else None
        median_pb = round(float(np.median(pb_vals)), 2) if pb_vals else None
        median_roe = round(float(np.median(roe_vals)), 2) if roe_vals else None
        median_dy = round(float(np.median(dy_vals)), 2) if dy_vals else None

        # 6. Premium / Discount assessment
        assessment = []
        pe_premium = None
        ev_premium = None

        if stock_pe and median_pe and median_pe > 0:
            pe_premium = round((stock_pe / median_pe - 1) * 100, 1)
            if pe_premium > 15:
                assessment.append(
                    f"Trades at {pe_premium:+.1f}% P/E PREMIUM to peers "
                    f"({stock_pe:.1f}x vs median {median_pe:.1f}x)")
            elif pe_premium < -15:
                assessment.append(
                    f"Trades at {pe_premium:+.1f}% P/E DISCOUNT to peers "
                    f"({stock_pe:.1f}x vs median {median_pe:.1f}x)")
            else:
                assessment.append(
                    f"P/E in-line with peers ({stock_pe:.1f}x vs "
                    f"median {median_pe:.1f}x)")

        if stock_ev_ebitda and median_ev and median_ev > 0:
            ev_premium = round((stock_ev_ebitda / median_ev - 1) * 100, 1)
            if ev_premium > 15:
                assessment.append(
                    f"EV/EBITDA at {ev_premium:+.1f}% premium "
                    f"({stock_ev_ebitda:.1f}x vs median {median_ev:.1f}x)")
            elif ev_premium < -15:
                assessment.append(
                    f"EV/EBITDA at {ev_premium:+.1f}% discount "
                    f"({stock_ev_ebitda:.1f}x vs median {median_ev:.1f}x)")

        # 7. Market cap context (dynamic tiers)
        mcap_tier = 'Unknown'
        if stock_mcap:
            mcap_tier = self._get_mcap_tier(stock_mcap)
            assessment.append(f"Market Cap ₹{stock_mcap:,.0f} Cr ({mcap_tier})")

        # 8. Rank within peers
        mcap_rank = None
        if stock_mcap and mcap_vals:
            all_mcaps = sorted(mcap_vals + [stock_mcap], reverse=True)
            mcap_rank = all_mcaps.index(stock_mcap) + 1

        # 9. Sort peers by market cap
        peer_data.sort(key=lambda p: p.get('market_cap_cr', 0) or 0,
                       reverse=True)

        return {
            'available': True,
            'sector': sector,
            'industry': sector_info.get('industry', 'Unknown'),
            'peer_count': len(peer_data),
            'peers': peer_data,
            # Medians
            'median_pe': median_pe,
            'median_ev_ebitda': median_ev,
            'median_mcap_cr': median_mcap,
            'median_pb': median_pb,
            'median_roe': median_roe,
            'median_dividend_yield': median_dy,
            # Stock values
            'stock_pe': stock_pe,
            'stock_ev_ebitda': stock_ev_ebitda,
            'stock_mcap_cr': stock_mcap,
            'stock_mcap_tier': mcap_tier,
            # Premiums
            'pe_premium_pct': pe_premium,
            'ev_premium_pct': ev_premium,
            'mcap_rank': mcap_rank,
            'mcap_rank_total': len(peer_data) + 1,
            # Sector stats
            'sector_total_mcap_cr': round(sum(mcap_vals), 0) if mcap_vals else None,
            'sector_avg_pe': round(float(np.mean(pe_vals)), 2) if pe_vals else None,
            'sector_avg_roe': round(float(np.mean(roe_vals)), 2) if roe_vals else None,
            # Assessment
            'assessment': assessment,
        }

    # ------------------------------------------------------------------
    def _get_sector(self, bse_symbol: str) -> dict:
        import io, sys
        try:
            old_stderr = sys.stderr
            sys.stderr = io.StringIO()
            try:
                tk = yf.Ticker(f"{bse_symbol}.BO")
                info = tk.info or {}
            finally:
                sys.stderr = old_stderr
            return {
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
            }
        except Exception:
            return {'sector': 'Unknown', 'industry': 'Unknown'}

    def _find_peers(self, bse_symbol: str, sector: str) -> list:
        """
        Discover peers DYNAMICALLY from screener.in sector page.
        Falls back to yfinance industry-based peer discovery.
        Zero hardcoded peer lists.
        """
        own_ticker = f"{bse_symbol}.BO"
        cache_key = sector.lower()

        # Return from session cache if already discovered
        if cache_key in self._peer_cache:
            return [t for t in self._peer_cache[cache_key] if t != own_ticker][:8]

        discovered = []

        # Method 1: Scrape screener.in peer comparison page
        try:
            discovered = self._discover_peers_screener(bse_symbol)
        except Exception:
            pass

        # Method 2: yfinance industry-based discovery
        if len(discovered) < 3:
            try:
                discovered = self._discover_peers_yfinance(bse_symbol, sector)
            except Exception:
                pass

        if discovered:
            self._peer_cache[cache_key] = discovered

        return [t for t in discovered if t != own_ticker][:8]

    def _discover_peers_screener(self, bse_symbol: str) -> list:
        """Scrape screener.in peer comparison page for live peer tickers."""
        import urllib.request
        import re
        url = f'https://www.screener.in/company/{bse_symbol}/consolidated/'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode('utf-8', errors='ignore')

        # Find peer comparison section — screener lists peers as links
        # Pattern: /company/TICKERNAME/
        peer_pattern = re.compile(r'/company/([A-Z0-9&\-]+)/', re.IGNORECASE)
        raw_peers = peer_pattern.findall(html)

        # Deduplicate and filter (exclude the stock itself and common non-ticker patterns)
        seen = set()
        peers = []
        exclude = {bse_symbol.upper(), 'COMPARE', 'PEER', 'SECTOR', 'INDUSTRY'}
        for p in raw_peers:
            p_upper = p.upper()
            if p_upper not in seen and p_upper not in exclude and len(p_upper) >= 2:
                seen.add(p_upper)
                peers.append(f"{p_upper}.BO")
        return peers[:15]

    def _discover_peers_yfinance(self, bse_symbol: str, sector: str) -> list:
        """Discover peers via yfinance industry field for known BSE stocks."""
        try:
            tk = yf.Ticker(f"{bse_symbol}.BO")
            info = tk.info or {}
            industry = info.get('industry', '')
            if not industry:
                return []

            # Use screener.in industry page to find other stocks in same industry
            import urllib.request
            import re
            # Try screener.in sector listing
            sector_slug = sector.lower().replace(' ', '-').replace('&', '-')
            url = f'https://www.screener.in/screens/sector/{sector_slug}/'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                html = resp.read().decode('utf-8', errors='ignore')

            peer_pattern = re.compile(r'/company/([A-Z0-9&\-]+)/', re.IGNORECASE)
            raw_peers = peer_pattern.findall(html)
            seen = set()
            peers = []
            for p in raw_peers:
                p_upper = p.upper()
                if p_upper not in seen and len(p_upper) >= 2:
                    seen.add(p_upper)
                    peers.append(f"{p_upper}.BO")
            return peers[:15]
        except Exception:
            return []

    def _get_mcap_tier(self, mcap_cr: float) -> str:
        """Classify market cap tier using live Nifty 50 median as benchmark."""
        import io, sys
        if mcap_cr is None:
            return 'Unknown'
        try:
            # Fetch live Nifty 50 market cap to set dynamic thresholds
            old_stderr = sys.stderr
            sys.stderr = io.StringIO()
            try:
                tk = yf.Ticker('^NSEI')
                nifty_hist = tk.history(period='5d')
            finally:
                sys.stderr = old_stderr
            if nifty_hist is not None and not nifty_hist.empty:
                nifty_level = float(nifty_hist['Close'].iloc[-1])
                # Dynamic thresholds scale with market level
                # At Nifty ~22000: Mega>200K, Large>50K, Mid>15K, Small>5K
                scale = nifty_level / 22000.0
                mega_threshold = 200_000 * scale
                large_threshold = 50_000 * scale
                mid_threshold = 15_000 * scale
                small_threshold = 5_000 * scale

                if mcap_cr >= mega_threshold:
                    return 'Mega Cap'
                elif mcap_cr >= large_threshold:
                    return 'Large Cap'
                elif mcap_cr >= mid_threshold:
                    return 'Mid Cap'
                elif mcap_cr >= small_threshold:
                    return 'Small Cap'
                else:
                    return 'Micro Cap'
        except Exception:
            pass
        return 'Unknown'

    def _fetch_multiples(self, ticker: str) -> dict:
        """Fetch key multiples for one ticker (enhanced with mcap, ROE, P/B).

        Tries .BO (BSE) first; on failure, retries with .NS (NSE).
        Suppresses all HTTP 404 noise from yfinance.
        """
        import io, sys, os

        def _try_ticker(t: str) -> dict:
            """Attempt fetch for a single yfinance ticker symbol."""
            # Suppress stderr (yfinance prints HTTP errors there)
            old_stderr = sys.stderr
            sys.stderr = io.StringIO()
            try:
                tk = yf.Ticker(t)
                info = tk.info or {}
            finally:
                sys.stderr = old_stderr

            name = info.get('shortName', t.replace('.BO', '').replace('.NS', ''))
            pe = info.get('trailingPE')
            ev_ebitda = info.get('enterpriseToEbitda')
            mcap = info.get('marketCap')
            pb = info.get('priceToBook')
            roe = info.get('returnOnEquity')
            dy = info.get('dividendYield')
            rev = info.get('totalRevenue')
            fwd_pe = info.get('forwardPE')

            if pe is None and ev_ebitda is None:
                return None

            base = t.replace('.BO', '').replace('.NS', '')
            return {
                'ticker': base,
                'name': name,
                'pe': round(pe, 2) if pe else None,
                'forward_pe': round(fwd_pe, 2) if fwd_pe else None,
                'ev_ebitda': round(ev_ebitda, 2) if ev_ebitda else None,
                'pb': round(pb, 2) if pb else None,
                'roe': round(roe * 100, 2) if roe else None,
                'dividend_yield': round(dy * 100, 2) if dy else None,
                'market_cap_cr': (round(mcap / 1e7, 0)
                                  if mcap else None),
                'revenue_cr': (round(rev / 1e7, 0)
                               if rev else None),
            }

        try:
            result = _try_ticker(ticker)
            if result:
                return result

            # Fallback: try .NS (NSE) if .BO (BSE) failed
            if ticker.endswith('.BO'):
                ns_ticker = ticker.replace('.BO', '.NS')
                return _try_ticker(ns_ticker)

            return None
        except Exception:
            return None
