"""
Peer Comparable Analysis (CCA) — Enhanced
===========================================
Automatically fetches peer multiples (P/E, EV/EBITDA, Market Cap)
for the same BSE industry segment and determines if the stock
trades at a premium or discount to the sector median.

Features:
  • 16 sector peer groups (up from 8)
  • Market cap comparison (Large / Mid / Small cap context)
  • ROE, P/B, Dividend Yield comparison
  • Sector ranking by multiple metrics

Powered by yfinance — free, no API keys.
"""
import numpy as np

try:
    import yfinance as yf
    _YF = True
except ImportError:
    _YF = False


# ─── Top BSE peers by sector (manually curated for major sectors) ─────
# Key = sector keyword (lowercase); Value = list of BSE tickers (.BO)
SECTOR_PEERS = {
    'information technology': [
        'TCS.BO', 'INFY.BO', 'WIPRO.BO', 'HCLTECH.BO', 'TECHM.BO',
        'LTIM.BO', 'MPHASIS.BO', 'COFORGE.BO', 'PERSISTENT.BO',
    ],
    'financial': [
        'HDFCBANK.BO', 'ICICIBANK.BO', 'KOTAKBANK.BO', 'SBIN.BO',
        'AXISBANK.BO', 'INDUSINDBK.BO', 'BAJFINANCE.BO', 'BAJAJFINSV.BO',
        'BANDHANBNK.BO', 'PNB.BO',
    ],
    'consumer': [
        'HINDUNILVR.BO', 'ITC.BO', 'NESTLEIND.BO', 'BRITANNIA.BO',
        'DABUR.BO', 'MARICO.BO', 'GODREJCP.BO', 'COLPAL.BO',
        'TATACONSUM.BO', 'VBL.BO',
    ],
    'pharmaceutical': [
        'SUNPHARMA.BO', 'DRREDDY.BO', 'CIPLA.BO', 'DIVISLAB.BO',
        'APOLLOHOSP.BO', 'BIOCON.BO', 'LUPIN.BO', 'AUROPHARMA.BO',
        'TORNTPHARM.BO', 'ALKEM.BO',
    ],
    'energy': [
        'RELIANCE.BO', 'ONGC.BO', 'IOC.BO', 'BPCL.BO', 'NTPC.BO',
        'POWERGRID.BO', 'ADANIENT.BO', 'ADANIGREEN.BO', 'GAIL.BO',
        'TATAPOWER.BO',
    ],
    'automobile': [
        'TATAMOTORS.BO', 'M&M.BO', 'MARUTI.BO', 'BAJAJ-AUTO.BO',
        'HEROMOTOCO.BO', 'EICHERMOT.BO', 'ASHOKLEY.BO', 'TVSMOTOR.BO',
    ],
    'metal': [
        'TATASTEEL.BO', 'JSWSTEEL.BO', 'HINDALCO.BO', 'VEDL.BO',
        'COALINDIA.BO', 'NMDC.BO', 'SAIL.BO', 'NATIONALUM.BO',
    ],
    'real estate': [
        'DLF.BO', 'GODREJPROP.BO', 'OBEROIRLTY.BO', 'PRESTIGE.BO',
        'PHOENIXLTD.BO', 'BRIGADE.BO', 'SOBHA.BO',
    ],
    'cement': [
        'ULTRACEMCO.BO', 'AMBUJACEM.BO', 'SHREECEM.BO', 'ACC.BO',
        'DALMIACEME.BO', 'RAMCOCEM.BO', 'JKCEMENT.BO',
    ],
    'telecom': [
        'BHARTIARTL.BO', 'IDEA.BO', 'TATACOMM.BO', 'ROUTE.BO',
    ],
    'insurance': [
        'SBILIFE.BO', 'HDFCLIFE.BO', 'ICICIPRULI.BO', 'LICI.BO',
        'NIACL.BO', 'GICRE.BO', 'STARHEALTH.BO',
    ],
    'chemical': [
        'PIDILITIND.BO', 'UPL.BO', 'SRF.BO', 'ATUL.BO',
        'DEEPAKNTR.BO', 'NAVINFLUOR.BO', 'CLEAN.BO',
    ],
    'infrastructure': [
        'LT.BO', 'ADANIPORTS.BO', 'SIEMENS.BO', 'ABB.BO',
        'HAVELLS.BO', 'BEL.BO', 'IRCTC.BO',
    ],
    'media': [
        'ZEEL.BO', 'SUNTV.BO', 'PVR.BO', 'SAREGAMA.BO',
        'NAZARA.BO', 'NETWORK18.BO',
    ],
    'textile': [
        'PAGEIND.BO', 'TRENT.BO', 'ABFRL.BO', 'RAYMOND.BO',
        'ARVIND.BO', 'WELSPUNIND.BO',
    ],
    'utility': [
        'NTPC.BO', 'POWERGRID.BO', 'TATAPOWER.BO', 'ADANIGREEN.BO',
        'NHPC.BO', 'SJVN.BO', 'IREDA.BO',
    ],
}

# Market cap tiers (in ₹ Cr)
MCAP_TIERS = {
    'Mega Cap': 200_000,
    'Large Cap': 50_000,
    'Mid Cap': 15_000,
    'Small Cap': 5_000,
    'Micro Cap': 0,
}


class PeerComparables:
    """
    Comparable Company Analysis (CCA) for Indian equities.

    Usage:
        cca = PeerComparables()
        result = cca.analyze('TCS', stock_pe=28.5, stock_ev_ebitda=22.1)
    """

    def __init__(self):
        self._available = _YF

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

        # 7. Market cap context
        mcap_tier = 'Unknown'
        if stock_mcap:
            for tier, threshold in MCAP_TIERS.items():
                if stock_mcap >= threshold:
                    mcap_tier = tier
                    break
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
        try:
            tk = yf.Ticker(f"{bse_symbol}.BO")
            info = tk.info or {}
            return {
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
            }
        except Exception:
            return {'sector': 'Unknown', 'industry': 'Unknown'}

    def _find_peers(self, bse_symbol: str, sector: str) -> list:
        """Match sector to our peer list; exclude the stock itself."""
        sector_lower = sector.lower()
        own_ticker = f"{bse_symbol}.BO"

        for key, tickers in SECTOR_PEERS.items():
            if key in sector_lower:
                return [t for t in tickers if t != own_ticker][:8]

        # Fallback: check all sector groups
        for key, tickers in SECTOR_PEERS.items():
            if own_ticker in tickers:
                return [t for t in tickers if t != own_ticker][:8]

        return []

    def _fetch_multiples(self, ticker: str) -> dict:
        """Fetch key multiples for one ticker (enhanced with mcap, ROE, P/B)."""
        try:
            tk = yf.Ticker(ticker)
            info = tk.info or {}
            name = info.get('shortName', ticker.replace('.BO', ''))
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

            return {
                'ticker': ticker.replace('.BO', ''),
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
        except Exception:
            return None
