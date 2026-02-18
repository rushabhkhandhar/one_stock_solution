"""
Sum-of-the-Parts (SOTP) Valuation
===================================
For conglomerates (Reliance, ITC, Tata Group, L&T) that operate in
disparate sectors, a single DCF is often insufficient.

SOTP Methodology:
  1. Identify business segments from segmental data
  2. Map each segment to a peer sector
  3. Pull sector-specific EV/EBITDA multiples from peer_comparables
  4. Apply: Segment_EV = Segment_EBIT × Sector_EV/EBITDA_Multiple
  5. Sum all segment EVs
  6. Subtract corporate overhead / unallocated losses
  7. Add holding-company discount if applicable

Formula:
  EV_Total = Σ(Segment_EBIT × Sector_Multiple) - Corporate_Overhead
  Equity_Value = EV_Total - Net_Debt
  Intrinsic_Value = Equity_Value / Shares_Outstanding
"""
import numpy as np
import re

try:
    import yfinance as yf
    _YF = True
except ImportError:
    _YF = False


# ─── Dynamic segment-to-sector mapping via yfinance industry lookup ───
# No hardcoded mapping — segments are classified by searching yfinance
# for the segment name and matching to its returned sector/industry.

# Holding company discount — derived from live conglomerate discount data
# rather than using a fixed table.


class SOTPModel:
    """
    Sum-of-the-Parts Valuation for conglomerate companies.
    Segment→sector mapping is done DYNAMICALLY via yfinance.

    Usage:
        sotp = SOTPModel()
        result = sotp.calculate(segmental_data, data, peer_cca)
    """

    def __init__(self):
        self._available = _YF
        self._segment_cache = {}  # Cache segment→sector mappings

    def calculate(self, segmental: dict, data: dict,
                  peer_cca: dict = None) -> dict:
        """
        Run SOTP valuation.

        Parameters
        ----------
        segmental : dict from SegmentalAnalysis.extract()
        data      : full data dict with balance_sheet, shares etc.
        peer_cca  : dict from PeerComparables.analyze() (for live multiples)

        Returns
        -------
        dict with segment valuations, total EV, equity value, intrinsic value
        """
        if not segmental or not segmental.get('available'):
            return {'available': False,
                    'reason': 'No segmental data available for SOTP'}

        segments = segmental.get('segments', [])
        if not segments:
            return {'available': False,
                    'reason': 'No segments found in segmental data'}

        # Check if any segment has EBIT data
        has_ebit = any(s.get('ebit') for s in segments)
        has_revenue = any(s.get('revenue') for s in segments)

        if not has_ebit and not has_revenue:
            return {'available': False,
                    'reason': 'Segments lack EBIT/revenue data for valuation'}

        # ── Step 1: Map each segment to a sector ────────────
        segment_valuations = []
        total_segment_ev = 0
        corporate_overhead = 0

        for seg in segments:
            seg_name = seg.get('name', 'Unknown')
            seg_ebit = seg.get('ebit', 0)
            seg_revenue = seg.get('revenue', 0)

            # Detect unallocated / corporate overhead
            if self._is_overhead(seg_name):
                corporate_overhead += abs(seg_ebit) if seg_ebit else 0
                continue

            # Map segment to sector
            mapped_sector = self._map_segment_to_sector(seg_name)

            # Get sector multiple (None if no live data available)
            multiple = self._get_sector_multiple(
                mapped_sector, peer_cca)

            # Calculate segment EV
            if multiple is not None and seg_ebit and seg_ebit > 0:
                segment_ev = seg_ebit * multiple
                method = 'EBIT × EV/EBITDA'
            elif seg_revenue and seg_revenue > 0:
                # Revenue multiple from live data only
                rev_multiple = self._revenue_multiple(mapped_sector, peer_cca)
                if rev_multiple is not None:
                    segment_ev = seg_revenue * rev_multiple
                    method = 'Revenue × EV/Sales'
                else:
                    segment_ev = 0
                    method = 'No live multiple available'
            else:
                segment_ev = 0
                method = 'No data'

            segment_valuations.append({
                'name': seg_name,
                'mapped_sector': mapped_sector,
                'ebit': seg_ebit,
                'revenue': seg_revenue,
                'multiple_applied': multiple if (multiple and seg_ebit) else (rev_multiple if (seg_revenue and 'Revenue' in method) else 0),
                'segment_ev': round(segment_ev, 2),
                'method': method,
                'ebit_margin': seg.get('ebit_margin'),
                'revenue_pct': seg.get('revenue_pct'),
            })
            total_segment_ev += segment_ev

        if total_segment_ev <= 0:
            return {'available': False,
                    'reason': 'SOTP yielded zero or negative EV'}

        # ── Step 2: Net adjustments ─────────────────────────
        total_ev = total_segment_ev - corporate_overhead

        # Holding discount — computed from live conglomerate trading data
        num_distinct_sectors = len(set(
            sv['mapped_sector'] for sv in segment_valuations
            if sv['mapped_sector'] != 'unknown'))
        holding_discount_pct = self._compute_live_holding_discount(num_distinct_sectors)
        if num_distinct_sectors >= 3:
            discount_type = 'conglomerate'
        elif num_distinct_sectors >= 2:
            discount_type = 'focused'
        else:
            discount_type = 'none'
        holding_discount_amount = total_ev * holding_discount_pct
        total_ev_after_discount = total_ev - holding_discount_amount

        # ── Step 3: Equity value and intrinsic value ────────
        net_debt = self._get_net_debt(data)
        # If net debt unknown, flag it but use 0 for calculation
        net_debt_estimated = net_debt is None
        net_debt_val = net_debt if net_debt is not None else 0.0
        equity_value = total_ev_after_discount - net_debt_val

        shares_cr = self._get_shares(data)
        intrinsic_value = (
            round(equity_value / shares_cr, 2)
            if shares_cr and shares_cr > 0 else None)

        current_price = self._get_current_price(data)
        upside_pct = (
            round((intrinsic_value - current_price) / current_price * 100, 2)
            if intrinsic_value and current_price and current_price > 0
            else None)

        return {
            'available': True,
            'segment_valuations': segment_valuations,
            'num_segments_valued': len(segment_valuations),
            'num_distinct_sectors': num_distinct_sectors,
            'total_segment_ev': round(total_segment_ev, 2),
            'corporate_overhead': round(corporate_overhead, 2),
            'holding_discount_type': discount_type,
            'holding_discount_pct': round(holding_discount_pct * 100, 1),
            'holding_discount_amount': round(holding_discount_amount, 2),
            'total_ev': round(total_ev_after_discount, 2),
            'net_debt': round(net_debt_val, 2),
            'net_debt_estimated': net_debt_estimated,
            'equity_value': round(equity_value, 2),
            'shares_cr': round(shares_cr, 2) if shares_cr else None,
            'intrinsic_value': intrinsic_value,
            'current_price': current_price,
            'upside_pct': upside_pct,
        }

    # ==================================================================
    # Segment → Sector Mapping (DYNAMIC via yfinance)
    # ==================================================================
    def _map_segment_to_sector(self, segment_name: str) -> str:
        """Map a segment name to a sector using yfinance industry lookup."""
        name_lower = segment_name.lower().strip()

        # Check cache first
        if name_lower in self._segment_cache:
            return self._segment_cache[name_lower]

        # Try to find a listed company or ETF matching the segment name
        # and use yfinance's sector classification
        if self._available:
            sector = self._classify_segment_live(name_lower)
            if sector != 'unknown':
                self._segment_cache[name_lower] = sector
                return sector

        self._segment_cache[name_lower] = 'unknown'
        return 'unknown'

    def _classify_segment_live(self, segment_name: str) -> str:
        """Use yfinance search to classify a segment into a sector."""
        try:
            import urllib.request
            import json

            # Use yfinance search to find companies matching segment name
            # and extract their sector classification
            query = segment_name.replace(' ', '+')
            url = f'https://query2.finance.yahoo.com/v1/finance/search?q={query}+india&quotesCount=5&newsCount=0'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            quotes = data.get('quotes', [])
            for quote in quotes:
                symbol = quote.get('symbol', '')
                if '.BO' in symbol or '.NS' in symbol:
                    try:
                        tk = yf.Ticker(symbol)
                        info = tk.info or {}
                        sector = info.get('sector', '')
                        if sector:
                            return sector.lower()
                    except Exception:
                        continue
        except Exception:
            pass
        return 'unknown'

    def _is_overhead(self, name: str) -> bool:
        """Check if a segment is corporate overhead / unallocated."""
        lower = name.lower()
        return any(kw in lower for kw in [
            'unallocated', 'corporate', 'elimination',
            'inter-segment', 'reconciliation', 'others',
            'adjustments', 'head office',
        ])

    # ==================================================================
    # Multiple Fetching
    # ==================================================================
    def _get_sector_multiple(self, sector: str,
                              peer_cca: dict = None) -> float:
        """
        Get EV/EBITDA multiple for a sector.
        Priority: peer_cca live data > yfinance fetch > None (unavailable)
        Returns None if no live multiple could be obtained.
        """
        # Try peer_cca data first (already fetched)
        if peer_cca and peer_cca.get('available'):
            # If the segment's sector matches the parent company's sector
            median_ev = peer_cca.get('median_ev_ebitda')
            if median_ev and median_ev > 0:
                # Only use if sector matches
                parent_sector = (peer_cca.get('sector', '') or '').lower()
                if sector.lower() in parent_sector or parent_sector in sector.lower():
                    return float(median_ev)

        # Try fetching live median from yfinance for the sector
        if self._available:
            live_multiple = self._fetch_live_sector_multiple(sector)
            if live_multiple:
                return live_multiple

        # No live data available — return None (do NOT use hardcoded fallback)
        return None

    def _fetch_live_sector_multiple(self, sector: str) -> float:
        """Fetch live median EV/EBITDA for a sector from yfinance using dynamic peer discovery."""
        # Use PeerComparables to dynamically discover peers
        try:
            peer_model = __import__('quant.peer_comparables', fromlist=['PeerComparables']).PeerComparables()
            # Find a representative ticker for this sector
            import urllib.request
            import json
            query = sector.replace(' ', '+') + '+india+stocks'
            url = f'https://query2.finance.yahoo.com/v1/finance/search?q={query}&quotesCount=8&newsCount=0'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            tickers = []
            for quote in data.get('quotes', []):
                sym = quote.get('symbol', '')
                if '.BO' in sym or '.NS' in sym:
                    tickers.append(sym if '.BO' in sym else sym.replace('.NS', '.BO'))

            if not tickers:
                return None

            ev_ebitda_vals = []
            for ticker in tickers[:5]:
                try:
                    tk = yf.Ticker(ticker)
                    info = tk.info or {}
                    ev_eb = info.get('enterpriseToEbitda')
                    if ev_eb and 0 < ev_eb < 100:
                        ev_ebitda_vals.append(ev_eb)
                except Exception:
                    continue

            if ev_ebitda_vals:
                return round(float(np.median(ev_ebitda_vals)), 2)
        except Exception:
            pass
        return None

    def _revenue_multiple(self, sector: str,
                          peer_cca: dict = None) -> float:
        """
        Revenue multiple (EV/Sales) from live peer data.
        Returns None if no live data available.
        """
        # Try peer_cca for EV/Sales
        if peer_cca and peer_cca.get('available'):
            ev_sales = peer_cca.get('median_ev_sales')
            if ev_sales and ev_sales > 0:
                parent_sector = (peer_cca.get('sector', '') or '').lower()
                if sector.lower() in parent_sector or parent_sector in sector.lower():
                    return float(ev_sales)

        # Try live fetch from yfinance via dynamic discovery
        if self._available:
            import urllib.request
            import json
            try:
                query = sector.replace(' ', '+') + '+india+stocks'
                url = f'https://query2.finance.yahoo.com/v1/finance/search?q={query}&quotesCount=8&newsCount=0'
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data_resp = json.loads(resp.read().decode('utf-8'))

                tickers = []
                for quote in data_resp.get('quotes', []):
                    sym = quote.get('symbol', '')
                    if '.BO' in sym or '.NS' in sym:
                        tickers.append(sym if '.BO' in sym else sym.replace('.NS', '.BO'))

                if tickers:
                    ev_sales_vals = []
                    for ticker in tickers[:5]:
                        try:
                            tk = yf.Ticker(ticker)
                            info = tk.info or {}
                            ev_s = info.get('enterpriseToRevenue')
                            if ev_s and 0 < ev_s < 50:
                                ev_sales_vals.append(ev_s)
                        except Exception:
                            continue
                    if ev_sales_vals:
                        return round(float(np.median(ev_sales_vals)), 2)
            except Exception:
                pass

        return None  # No live data — do NOT use hardcoded fallback

    def _compute_live_holding_discount(self, num_sectors: int) -> float:
        """
        Compute holding company discount from live conglomerate trading data.
        Compares average conglomerate P/E vs Nifty 50 P/E to derive discount.
        """
        if num_sectors < 2:
            return 0.0

        try:
            # Fetch Nifty 50 P/E as benchmark
            nifty = yf.Ticker('^NSEI')
            nifty_info = nifty.info or {}
            nifty_pe = nifty_info.get('trailingPE')

            if not nifty_pe or nifty_pe <= 0:
                # Cannot determine — use zero discount
                return 0.0

            # Fetch P/E of well-known Indian conglomerates for comparison
            conglom_tickers = ['RELIANCE.BO', 'ITC.BO', 'LT.BO', 'ADANIENT.BO']
            conglom_pes = []
            for ticker in conglom_tickers:
                try:
                    tk = yf.Ticker(ticker)
                    info = tk.info or {}
                    pe = info.get('trailingPE')
                    if pe and 0 < pe < 200:
                        conglom_pes.append(pe)
                except Exception:
                    continue

            if conglom_pes:
                avg_conglom_pe = np.mean(conglom_pes)
                # Discount = how much conglomerates trade below market
                discount = max(0, (nifty_pe - avg_conglom_pe) / nifty_pe)
                # Scale by number of sectors (more diversified = higher discount)
                if num_sectors >= 3:
                    return round(min(discount * 1.2, 0.25), 4)
                else:
                    return round(min(discount * 0.8, 0.15), 4)
        except Exception:
            pass
        return 0.0

    # ==================================================================
    # Helpers
    # ==================================================================
    def _get_net_debt(self, data: dict) -> float:
        """Get net debt from balance sheet."""
        from data.preprocessing import DataPreprocessor, get_value
        pp = DataPreprocessor()
        bs = data.get('balance_sheet')
        if bs is None or bs.empty:
            return None  # Unknown — do not fabricate zero debt
        borr = get_value(pp.get(bs, 'borrowings'))
        return float(borr) if not np.isnan(borr) else None

    def _get_shares(self, data: dict) -> float:
        """Get shares outstanding in Cr."""
        import pandas as pd
        shares = data.get('shares_outstanding')
        if isinstance(shares, pd.Series) and not shares.empty:
            val = float(shares.dropna().iloc[-1])
            if not np.isnan(val) and val > 0:
                return val
        # Fallback
        from data.preprocessing import DataPreprocessor, get_value
        pp = DataPreprocessor()
        pnl = data.get('pnl')
        if pnl is not None and not pnl.empty:
            np_val = get_value(pp.get(pnl, 'net_profit'))
            eps_val = get_value(pp.get(pnl, 'eps'))
            if not np.isnan(np_val) and not np.isnan(eps_val) and eps_val > 0:
                return np_val / eps_val
        return None  # No shares data — do not fabricate

    def _get_current_price(self, data: dict) -> float:
        """Get current market price."""
        import pandas as pd
        price_df = data.get('price', pd.DataFrame())
        if isinstance(price_df, pd.DataFrame) and not price_df.empty:
            if 'close' in price_df.columns:
                return float(price_df['close'].iloc[-1])
            return float(price_df.iloc[-1, 0])
        return None  # No price data — do not fabricate
