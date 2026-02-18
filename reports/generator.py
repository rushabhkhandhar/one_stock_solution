"""
Equity Research Report Generator — Goldman-Standard
=====================================================
Produces a professional, 360-degree Markdown report:
  1.  Executive Summary (Ticker, Rating, Target Price, Horizon)
  2.  Investment Thesis
  3.  Financial Summary Table
  4.  DCF Valuation + WACC Sensitivity Grid
  5.  CFO / EBITDA Quality Check
  6.  Peer Comparable Analysis (CCA)
  7.  Forensic Analysis (M-Score)
  8.  Financial Health (F-Score)
  9.  Shareholding Pattern
  10. Qualitative Intelligence (Sentiment, Mgmt Tracker)
  11. Predictive Model (Price Forecast)
  12. Market Correlation & Relative Strength
  13. Data Validation — Annual Report Cross-Check
  14. Macro Context
  15. Risk Factors & Red Flags
  16. SEBI Compliance Disclaimer (with data source citations)
"""
import datetime
import os
from compliance.safety import DISCLAIMER, stamp_source


class ReportGenerator:

    # ==================================================================
    @staticmethod
    def _smart_truncate(text: str, max_chars: int = 350) -> str:
        """Return only *complete* sentences that fit within *max_chars*.

        If no full sentence fits, take the first sentence (even if it
        exceeds the budget slightly) and append an ellipsis.
        This guarantees every snippet reads as a coherent thought.
        """
        import re as _re
        # Clean transcript noise before truncating
        try:
            from qualitative.rag_engine import RAGEngine
            text = RAGEngine.clean_transcript_noise(text)
        except Exception:
            pass
        text = text.strip()
        if len(text) <= max_chars:
            return text

        # Split into sentences (keep the delimiter attached)
        sentences = _re.split(r'(?<=[.!?])\s+', text)

        # Greedily collect whole sentences that fit
        result = ''
        for sent in sentences:
            candidate = (result + ' ' + sent).strip() if result else sent
            if len(candidate) <= max_chars:
                result = candidate
            else:
                break

        if result:
            return result

        # No complete sentence fits — take the first sentence, trim
        # at the nearest clause boundary inside the budget.
        first = sentences[0] if sentences else text
        if len(first) <= max_chars:
            return first
        window = first[:max_chars]
        for delim in ['. ', '; ', ', ']:
            idx = window.rfind(delim)
            if idx > max_chars * 0.35:
                return window[:idx + 1].strip()
        idx = window.rfind(' ')
        if idx > 0:
            return window[:idx].rstrip('.,;:!?') + ' \u2026'
        return window

    # ==================================================================
    def generate(self, symbol: str, data: dict, analysis: dict) -> str:
        now = datetime.datetime.now().strftime("%d %B %Y, %I:%M %p")

        ratios = analysis.get('ratios', {})
        dcf    = analysis.get('dcf', {})
        ms     = analysis.get('mscore', {})
        fs     = analysis.get('fscore', {})
        rating = analysis.get('rating', {})
        shp    = analysis.get('shareholding', {})
        trends = analysis.get('trends', {})
        tech   = analysis.get('technicals', {})
        text_intel = analysis.get('text_intel', {})

        lines = []
        a = lines.append

        # ── Header ───────────────────────────────────────────
        a(f"# 📊 Equity Research Report — {symbol}\n")
        a(f"| | |")
        a(f"|---|---|")
        a(f"| **Generated** | {now} |")
        a(f"| **BSE Token** | {data.get('token', 'N/A')} |")
        a(f"| **Analysis** | {'Consolidated' if True else 'Standalone'} |")
        a(f"| **Rating Confidence** | {rating.get('confidence', 'N/A')} |")
        a("")

        # ── Rating Box ───────────────────────────────────────
        is_suspended = rating.get('data_suspended', False)
        if rating:
            a(f"## 🏷️ Rating: {rating.get('recommendation', 'N/A')}\n")
            if is_suspended:
                a("> ⚠️ **RATING SUSPENDED** — Data Trust Score is below "
                  "the reliability threshold. All quantitative outputs "
                  "(DCF, ratios, forensics) may be inaccurate. "
                  "Manual review required before acting on this report.\n")
            if dcf.get('available') and not is_suspended:
                dcf_mismatch = dcf.get('dcf_ev_mismatch', False)
                a(f"| | |")
                a(f"|---|---|")
                if dcf_mismatch:
                    a(f"| **Target Price (DCF)** | ⚠️ N/A (see guardrail below) |")
                else:
                    a(f"| **Target Price (DCF)** | ₹{dcf['intrinsic_value']:,.2f} |")
                a(f"| **Current Price** | ₹{dcf['current_price']:,.2f} |")
                if not dcf_mismatch:
                    up = dcf.get('upside_pct')
                    if up is not None:
                        a(f"| **Upside / Downside** | {up:+.1f} % |")
                a(f"| **Investment Horizon** | {rating.get('horizon', 'N/A')} |")
            a("")

        # ── Investment Thesis ────────────────────────────────
        a("## 📌 Investment Thesis\n")
        for pt in rating.get('thesis', []):
            # Strip any stray $ / LaTeX artefacts from thesis bullets
            _pt = pt.replace('$', '')
            a(f"- {_pt}")
        a("")

        # ── Financial Summary Table ──────────────────────────
        a("## 📋 Financial Summary\n")
        a("| Metric | Value |")
        a("|--------|------:|")
        METRICS = [
            ('Current Price',         'current_price',   '₹{:,.2f}'),
            ('P/E Ratio (TTM)',       'pe_ratio',        '{:.2f}x'),
            ('EPS (Annual)',          'eps',             '₹{:.2f}'),
            ('EPS (TTM)',             'ttm_eps',         '₹{:.2f}'),
            ('ROE',                   'roe',             '{:.2f} %'),
            ('ROA',                   'roa',             '{:.2f} %'),
            ('ROCE',                  'roce',            '{:.2f} %'),
            ('PAT Margin',            'pat_margin',      '{:.2f} %'),
            ('Operating Margin',      'opm',             '{:.2f} %'),
            ('Debt / Equity',         'debt_to_equity',  '{:.2f}'),
            ('Interest Coverage',     'interest_coverage','{:.2f}x'),
            ('Current Ratio',         'current_ratio',   '{:.2f}'),
            ('Debtors Turnover',      'debtors_turnover','{:.2f}x'),
            ('Debtor Days',           'debtor_days',     '{:.0f} days'),
            ('Inventory Turnover',    'inventory_turnover','{:.2f}x'),
            ('Inventory Days',        'inventory_days',  '{:.0f} days'),
            ('Cash Conversion Cycle', 'cash_conversion_cycle', '{:.0f} days'),
            ('Revenue Growth (YoY)',  'revenue_growth',  '{:+.2f} %'),
            ('Revenue CAGR (3Y)',     'revenue_cagr_3y', '{:.2f} %'),
            ('Revenue CAGR (5Y)',     'revenue_cagr_5y', '{:.2f} %'),
            ('Profit Growth (YoY)',   'profit_growth',   '{:+.2f} %'),
            ('Dividend Yield',        'dividend_yield',  '{:.2f} %'),
        ]
        for label, key, fmt in METRICS:
            val = ratios.get(key)
            if val is None:
                continue
            if isinstance(val, str):
                display = val
            elif val == float('inf'):
                display = '∞'
            else:
                display = fmt.format(val)
            a(f"| {label} | {display} |")
        a("")

        # ── 5-Year Trend Analysis (NEW) ─────────────────────
        if trends.get('available'):
            a("## 📈 5-Year Trend Analysis\n")
            direction = trends.get('overall_direction', 'N/A')
            health = trends.get('health_score')
            dir_icon = {'IMPROVING': '🟢', 'STABLE': '🟡',
                        'DETERIORATING': '🔴'}.get(direction, '⚪')
            a(f"**{dir_icon} Overall Direction: {direction}** "
              f"| Health Score: {health if health is not None else 'N/A'}/10\n")

            metrics = trends.get('metrics', [])
            if metrics:
                a("| Metric | Latest | Direction | 5Y CAGR | Acceleration |")
                a("|--------|-------:|:---------:|--------:|:------------:|")
                for m in metrics:
                    arrow = {'UP': 'UP', 'DOWN': 'DOWN',
                             'FLAT': 'FLAT'}.get(m.get('direction', ''), 'FLAT')
                    cagr = f"{m['cagr_5y']:+.1f}%" if m.get('cagr_5y') is not None else 'N/A'
                    accel = m.get('acceleration', '—')
                    val = m.get('latest', 0)
                    if m.get('is_pure_ratio'):
                        # D/E etc. — show as multiple, not %
                        display = f"{val:.2f}x"
                    elif m.get('is_pct_decimal'):
                        # Derived decimal ratios (ROE, PAT Margin) — ×100
                        display = f"{val * 100:.2f} %"
                    elif m.get('is_ratio'):
                        # Already-percentage ratios (ROCE%, OPM%)
                        display = f"{val:.2f} %" if abs(val) < 100 else f"{val:.1f} %"
                    else:
                        # Absolute values — but EPS is per-share, not Cr
                        lbl = m.get('label', '')
                        if 'EPS' in lbl:
                            display = f"₹{val:.2f}"
                        elif abs(val) > 1:
                            display = f"₹{val:,.0f} Cr"
                        else:
                            display = f"{val:.2f}"
                    a(f"| {m['label']} | {display} | {arrow} | {cagr} | {accel} |")
                a("")

                # Historical data for key metrics
                key_metrics = [m for m in metrics
                               if m['label'] in ('Revenue', 'Net Profit (PAT)',
                                                  'EPS', 'Cash from Operations')]
                if key_metrics:
                    a("### Historical Values\n")
                    for m in key_metrics:
                        hist = m.get('history', [])
                        if hist:
                            years = [h['year'] for h in hist]
                            vals = [f"{h['value']:,.0f}" for h in hist]
                            a(f"**{m['label']}:** "
                              + " | ".join(f"{y}: Rs.{v}"
                                           for y, v in zip(years, vals)))
                    a("")

                # Projections
                a("### Linear Projections\n")
                a("| Metric | Proj. Y+1 | Proj. Y+2 |")
                a("|--------|----------:|----------:|")

                # Build revenue/op-profit projections for OPM re-calc
                _rev_p = {}
                _op_p = {}
                for m in metrics[:8]:
                    if m['label'] == 'Revenue':
                        _rev_p = {'p1': m.get('projection_1y'), 'p2': m.get('projection_2y')}
                    if m['label'] == 'Operating Profit':
                        _op_p = {'p1': m.get('projection_1y'), 'p2': m.get('projection_2y')}

                for m in metrics[:8]:
                    p1 = m.get('projection_1y')
                    p2 = m.get('projection_2y')
                    if p1 is None:
                        continue
                    lbl = m['label']

                    if lbl == 'OPM %' and _rev_p.get('p1') and _op_p.get('p1'):
                        # Recompute OPM dynamically from projected values
                        opm1 = (_op_p['p1'] / _rev_p['p1']) * 100 if _rev_p['p1'] else 0
                        opm2 = (_op_p['p2'] / _rev_p['p2']) * 100 if _rev_p['p2'] else 0
                        a(f"| {lbl} | {opm1:.1f} % | {opm2:.1f} % |")
                    elif m.get('is_pure_ratio'):
                        a(f"| {lbl} | {p1:.2f}x | {p2:.2f}x |")
                    elif m.get('is_pct_decimal'):
                        a(f"| {lbl} | {p1 * 100:.1f} % | {p2 * 100:.1f} % |")
                    elif m.get('is_ratio'):
                        a(f"| {lbl} | {p1:.1f} % | {p2:.1f} % |")
                    else:
                        # Absolute values — EPS is per-share, not Crores
                        if 'EPS' in lbl:
                            a(f"| {lbl} | ₹{p1:.2f} | ₹{p2:.2f} |")
                        else:
                            a(f"| {lbl} | ₹{p1:,.0f} | ₹{p2:,.0f} |")
                a("")
                a("> ⚠️ *Linear projections — actual results depend on "
                  "market conditions, management execution, and macro factors.*\n")

        # ── DCF Valuation ────────────────────────────────────
        a("## 💰 Valuation Analysis — DCF Model\n")
        if is_suspended:
            a("> ⚠️ **DCF BYPASSED** — Data Trust Score is below reliability "
              "threshold. DCF model generation is bypassed to prevent "
              "misleading valuation outputs.\n")
        elif dcf.get('available'):
            dcf_mismatch = dcf.get('dcf_ev_mismatch', False)

            # ── DCF Inputs ──
            a("### Model Inputs\n")
            a("| Parameter | Value |")
            a("|-----------|------:|")
            a(f"| WACC | {dcf['wacc']} % |")
            a(f"| Growth Rate (initial) | {dcf['growth_rate']} % |")
            a(f"| Terminal Growth | {dcf['terminal_growth']} % |")
            a(f"| Latest FCF | ₹{dcf['latest_fcf']:,.2f} Cr |")
            a(f"| Projection Period | {len(dcf.get('projected_fcf', []))} years |")
            a("")

            # ── 4-Step DCF Breakdown ──
            a("### 4-Step DCF Breakdown\n")
            a("| Step | Description | Value |")
            a("|:----:|-------------|------:|")
            _pv_fcf = dcf.get('pv_of_fcf')
            a(f"| 1 | PV of Projected FCFs | "
              f"{f'₹{_pv_fcf:,.2f} Cr' if _pv_fcf is not None else 'N/A'} |")
            _pv_tv = dcf.get('pv_of_terminal')
            _tv = dcf.get('terminal_value')
            a(f"| 2 | Terminal Value (Gordon) | "
              f"{f'₹{_tv:,.2f} Cr' if _tv is not None else 'N/A'} |")
            a(f"| 2b | PV of Terminal Value | "
              f"{f'₹{_pv_tv:,.2f} Cr' if _pv_tv is not None else 'N/A'} |")
            a(f"| 3 | **Enterprise Value (DCF)** | "
              f"**₹{dcf['enterprise_value']:,.2f} Cr** |")
            a(f"| 4a | − Net Debt | ₹{dcf['net_debt']:,.2f} Cr |")
            a(f"| 4b | = Equity Value | ₹{dcf['equity_value']:,.2f} Cr |")
            a(f"| 4c | ÷ Shares Outstanding | {dcf['shares_cr']:.2f} Cr |")
            if dcf_mismatch:
                a(f"| 4d | **Target Price / Share** | **⚠️ N/A** |")
            else:
                a(f"| 4d | **Target Price / Share** | "
                  f"**₹{dcf['intrinsic_value']:,.2f}** |")
            a("")

            # ── Market Comparison ──
            a("### Market Comparison\n")
            a("| Metric | Value |")
            a("|--------|------:|")
            a(f"| Current Market Price | Rs. {dcf['current_price']:,.2f} |")
            if dcf.get('market_cap') is not None:
                a(f"| Market Cap | Rs. {dcf['market_cap']:,.2f} Cr |")
            if dcf.get('market_ev') is not None:
                a(f"| Market Enterprise Value | Rs. {dcf['market_ev']:,.2f} Cr |")
            a(f"| DCF Enterprise Value | Rs. {dcf['enterprise_value']:,.2f} Cr |")
            _delta = dcf.get('ev_delta_pct')
            if _delta is not None:
                a(f"| EV Delta (DCF vs Market) | {_delta:.1f}% |")
            if not dcf_mismatch:
                up = dcf.get('upside_pct')
                if up is not None:
                    icon = "🟢" if up > 10 else ("🟡" if up > -10 else "🔴")
                    a(f"| Upside / Downside | {icon} {up:+.1f} % |")
            a("")

            # ── EV Mismatch Guardrail Warning ──
            if dcf_mismatch:
                from config import config as _cfg2
                a("> 🔴 **DCF GUARDRAIL TRIGGERED** — The DCF Enterprise Value "
                  f"(Rs. {dcf['enterprise_value']:,.0f} Cr) deviates from the "
                  f"Market Enterprise Value "
                  f"(Rs. {dcf.get('market_ev', 0):,.0f} Cr) by "
                  f"{_delta:.0f}%, which exceeds the "
                  f"{_cfg2.validation.dcf_ev_threshold_pct:.0f}% sanity threshold. "
                  "Target Price has been overridden to **N/A**. "
                  "Manual review of WACC and Growth Rate inputs is required.\n")
            a("")
            # Projected FCFs
            proj = dcf.get('projected_fcf', [])
            if proj:
                a("**Projected Free Cash Flows (₹ Cr):**\n")
                hdr = "| " + " | ".join(f"Y{i+1}" for i in range(len(proj))) + " |"
                sep = "|" + "|".join("---:" for _ in proj) + "|"
                val_row = "| " + " | ".join(f"{f:,.0f}" for f in proj) + " |"
                a(hdr); a(sep); a(val_row)
            a("")

            # WACC Sensitivity Grid
            sens = dcf.get('sensitivity', {})
            if sens.get('available') and not dcf_mismatch:
                a("### WACC Sensitivity Grid\n")
                a("> Intrinsic value per share (₹) under different WACC and "
                  "Terminal Growth Rate assumptions.\n")
                wacc_range = sens['wacc_range']
                tgr_range = sens['tgr_range']
                grid = sens['grid']

                hdr = "| WACC \\ TGR | " + " | ".join(
                    f"{t:.1f}%" for t in tgr_range) + " |"
                sep = "|---:|" + "|".join("---:" for _ in tgr_range) + "|"
                a(hdr); a(sep)
                for i, w in enumerate(wacc_range):
                    cells = []
                    for j in range(len(tgr_range)):
                        v = grid[i][j]
                        cells.append(f"₹{v:,.0f}" if v is not None else "N/A")
                    row = f"| **{w:.1f}%** | " + " | ".join(cells) + " |"
                    a(row)
                a("")
        else:
            a(f"> ⚠️ DCF not available — {dcf.get('reason', 'unknown')}\n")

        # ── SOTP Valuation ───────────────────────────────────
        sotp = analysis.get('sotp', {})
        if sotp.get('available'):
            a("## 🧩 Sum-of-the-Parts (SOTP) Valuation\n")
            a(f"**Method:** Segment-level EV/EBITDA valuation with "
              f"holding-company discount\n")

            seg_vals = sotp.get('segment_valuations', [])
            if seg_vals:
                a("| Segment | Revenue (₹ Cr) | EBITDA (₹ Cr) | "
                  "EV/EBITDA | Segment EV (₹ Cr) |")
                a("|---------|---------------:|-------------:|--------:|------------------:|")
                for sv in seg_vals:
                    rev = f"{sv['revenue']:,.0f}" if sv.get('revenue') is not None else 'N/A'
                    ebitda = f"{sv['ebitda']:,.0f}" if sv.get('ebitda') is not None else 'N/A'
                    mult = f"{sv['ev_ebitda_multiple']:.1f}x" if sv.get('ev_ebitda_multiple') is not None else 'N/A'
                    sev = f"{sv['segment_ev']:,.0f}" if sv.get('segment_ev') is not None else 'N/A'
                    a(f"| {sv.get('segment', '?')} "
                      f"| {rev} "
                      f"| {ebitda} "
                      f"| {mult} "
                      f"| {sev} |")
                a("")

            a("| SOTP Metric | Value |")
            a("|-------------|------:|")
            _total_ev = sotp.get('total_ev')
            a(f"| Sum of Segment EVs | {f'₹{_total_ev:,.0f} Cr' if _total_ev is not None else 'N/A'} |")
            disc = sotp.get('holding_company_discount')
            a(f"| Holding Company Discount | {f'{disc:.0f}%' if disc is not None else 'N/A'} |")
            _net_debt = sotp.get('net_debt')
            a(f"| Net Debt | {f'₹{_net_debt:,.0f} Cr' if _net_debt is not None else 'N/A'} |")
            _eq_val = sotp.get('equity_value')
            a(f"| SOTP Equity Value | {f'₹{_eq_val:,.0f} Cr' if _eq_val is not None else 'N/A'} |")
            _iv = sotp.get('intrinsic_value')
            a(f"| **SOTP Intrinsic Value / Share** | "
              f"**{f'₹{_iv:,.2f}' if _iv is not None else 'N/A'}** |")
            _cp = sotp.get('current_price')
            a(f"| Current Market Price | {f'₹{_cp:,.2f}' if _cp is not None else 'N/A'} |")
            sotp_up = sotp.get('upside_pct')
            if sotp_up is not None:
                icon = "🟢" if sotp_up > 10 else ("🟡" if sotp_up > -10 else "🔴")
                a(f"| SOTP Upside / Downside | {icon} {sotp_up:+.1f}% |")
            else:
                a("| SOTP Upside / Downside | N/A |")
            a("")

            a("> 💡 *SOTP is most useful for conglomerates with diverse business "
              "segments. Discount reflects limited market for controlling stake.*\n")

        # ── CFO / EBITDA Quality ─────────────────────────────
        cfo = analysis.get('cfo_ebitda_check', {})
        if cfo.get('available'):
            a("## 💵 Cash Flow Quality — CFO / EBITDA Check\n")
            flag_icon = "🔴" if cfo.get('is_red_flag') else "🟢"
            a(f"| Metric | Value |")
            a(f"|--------|------:|")
            a(f"| CFO / EBITDA Ratio | {flag_icon} {cfo.get('ratio', 'N/A')}% |")
            a(f"| Assessment | {cfo.get('interpretation', 'N/A')} |")
            hist = cfo.get('history', [])
            if hist:
                a(f"| 3-Year Trend | {', '.join(f'{h}%' for h in hist)} |")
            a("")

        # ── Peer Comparable Analysis (enhanced) ──────────────
        peer = analysis.get('peer_cca', {})
        if peer.get('available'):
            a("## 🏢 Peer Comparable Analysis (CCA)\n")
            a(f"**Sector:** {peer.get('sector', 'N/A')} "
              f"({peer.get('industry', 'N/A')}) — "
              f"{peer.get('peer_count', 0)} peers analyzed\n")

            # Market cap context
            mcap_tier = peer.get('stock_mcap_tier', '')
            stock_mcap = peer.get('stock_mcap_cr')
            if stock_mcap:
                a(f"**Market Cap:** ₹{stock_mcap:,.0f} Cr ({mcap_tier}) — "
                  f"Rank {peer.get('mcap_rank', '?')}"
                  f"/{peer.get('mcap_rank_total', '?')} in sector\n")

            # Assessment
            assessment = peer.get('assessment', [])
            for stmt in assessment:
                a(f"- {stmt}")
            if assessment:
                a("")

            # Comparison table
            a("| Metric | Stock | Sector Median | Sector Avg |")
            a("|--------|------:|--------------:|-----------:|")
            s_pe = peer.get('stock_pe')
            m_pe = peer.get('median_pe')
            avg_pe = peer.get('sector_avg_pe')
            a(f"| P/E | {f'{s_pe:.1f}x' if s_pe else 'N/A'} | "
              f"{f'{m_pe:.1f}x' if m_pe else 'N/A'} | "
              f"{f'{avg_pe:.1f}x' if avg_pe else 'N/A'} |")
            s_ev = peer.get('stock_ev_ebitda')
            m_ev = peer.get('median_ev_ebitda')
            a(f"| EV/EBITDA | {f'{s_ev:.1f}x' if s_ev else 'N/A'} | "
              f"{f'{m_ev:.1f}x' if m_ev else 'N/A'} | — |")
            m_pb = peer.get('median_pb')
            a(f"| P/B | — | {f'{m_pb:.1f}x' if m_pb else 'N/A'} | — |")
            m_roe = peer.get('median_roe')
            avg_roe = peer.get('sector_avg_roe')
            a(f"| ROE (%) | — | {f'{m_roe:.1f}%' if m_roe else 'N/A'} | "
              f"{f'{avg_roe:.1f}%' if avg_roe else 'N/A'} |")
            m_dy = peer.get('median_dividend_yield')
            a(f"| Div Yield | — | {f'{m_dy:.1f}%' if m_dy else 'N/A'} | — |")
            a("")

            # Sector total market cap
            sect_mcap = peer.get('sector_total_mcap_cr')
            if sect_mcap:
                a(f"**Sector Total Market Cap:** ₹{sect_mcap:,.0f} Cr\n")

            peers_detail = peer.get('peers', [])
            if peers_detail:
                a("**Peer Comparison Table:**\n")
                a("| Company | MCap (₹Cr) | P/E | EV/EBITDA | ROE % | Div Yield % |")
                a("|---------|----------:|----:|----------:|------:|------------:|")
                for p in peers_detail[:10]:
                    pe_v = f"{p['pe']:.1f}" if p.get('pe') else 'N/A'
                    ev_v = f"{p['ev_ebitda']:.1f}" if p.get('ev_ebitda') else 'N/A'
                    roe_v = f"{p['roe']:.1f}" if p.get('roe') else 'N/A'
                    dy_v = f"{p['dividend_yield']:.1f}" if p.get('dividend_yield') else 'N/A'
                    mcap_v = f"{p['market_cap_cr']:,.0f}" if p.get('market_cap_cr') else 'N/A'
                    name = p.get('name', p.get('ticker', '?'))
                    a(f"| {name} | {mcap_v} | {pe_v} | {ev_v} | {roe_v} | {dy_v} |")
                a("")

        # ── Forensic Analysis ────────────────────────────────
        a("## 🔍 Forensic Analysis — Beneish M-Score\n")
        if ms.get('available'):
            a(f"**M-Score: {ms['m_score']}**\n")
            a(f"**Assessment:** {ms['interpretation']}\n")
            a("| Component | Value | Description |")
            a("|-----------|------:|-------------|")
            DESC = {
                'DSRI': 'Days Sales in Receivables Index',
                'GMI':  'Gross Margin Index',
                'AQI':  'Asset Quality Index',
                'SGI':  'Sales Growth Index',
                'DEPI': 'Depreciation Index',
                'SGAI': 'SGA Expense Index',
                'TATA': 'Total Accruals / Total Assets',
                'LVGI': 'Leverage Index',
            }
            for k, v in ms.get('components', {}).items():
                a(f"| {k} | {v:.4f} | {DESC.get(k, '')} |")
            a("")
            a(f"> **Threshold:** M > {ms['thresholds']['manipulation_likely']}"
              f" → Likely manipulation  ·  "
              f"M < {ms['thresholds']['manipulation_unlikely']}"
              f" → Unlikely\n")
        else:
            a(f"> ⚠️ M-Score not available — {ms.get('reason', 'unknown')}\n")

        # ── Piotroski F-Score ────────────────────────────────
        a("## 🏥 Financial Health — Piotroski F-Score\n")
        if fs.get('available'):
            a(f"**F-Score: {fs['f_score']} / 9**\n")
            a(f"**Assessment:** {fs['interpretation']}\n")
            a("| # | Criterion | Result |")
            a("|--:|-----------|:------:|")
            LABELS = {
                'F1_ROA_positive':            'ROA > 0',
                'F2_CFO_positive':            'Operating Cash Flow > 0',
                'F3_ROA_improving':           'ROA Improving YoY',
                'F4_Accrual_quality':         'CFO > Net Income',
                'F5_Debt_decreasing':         'Leverage Decreasing',
                'F6_CurrentRatio_improving':  'Current Ratio Improving',
                'F7_No_dilution':             'No Share Dilution',
                'F8_GrossMargin_improving':   'Gross Margin Improving',
                'F9_AssetTurnover_improving': 'Asset Turnover Improving',
            }
            for i, (key, crit) in enumerate(fs.get('criteria', {}).items(), 1):
                label = LABELS.get(key, key)
                icon  = "✅" if crit.get('pass') else "❌"
                a(f"| {i} | {label} | {icon} |")
            a("")
        else:
            a(f"> ⚠️ F-Score not available — {fs.get('reason', 'unknown')}\n")

        # ── Shareholding ─────────────────────────────────────
        if shp:
            a("## 👥 Shareholding Pattern\n")
            a("| Category | Current (%) | Previous (%) | Δ |")
            a("|----------|------------:|-------------:|--:|")
            for cat, vals in shp.items():
                if cat == 'PromoterPledging':
                    continue  # Handled separately below
                # OCR cleanup: PDF parsers confuse I/l in FIIs/DIIs
                cat_display = (cat.replace('Flls', 'FIIs')
                                  .replace('Dils', 'DIIs')
                                  .replace('FlIs', 'FIIs')
                                  .replace('DlIs', 'DIIs')
                                  .replace('Flls', 'FIIs')
                                  .replace('FIls', 'FIIs')
                                  .replace('DIls', 'DIIs'))
                cur = vals.get('current', 'N/A')
                prv = vals.get('previous', 'N/A')
                if isinstance(cur, (int, float)) and isinstance(prv, (int, float)):
                    delta = f"{cur - prv:+.2f}"
                else:
                    delta = "—"
                a(f"| {cat_display} | {cur} | {prv} | {delta} |")
            a("")

            # Promoter Pledging
            pledge = shp.get('PromoterPledging', {})
            if pledge:
                a("### 🔒 Promoter Pledging\n")
                sev = pledge.get('severity', 'UNKNOWN')
                sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠",
                            "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                a(f"| Metric | Value |")
                a(f"|--------|------:|")
                a(f"| Current Pledging | {sev_icon} {pledge.get('current', 'N/A')}% |")
                a(f"| Previous | {pledge.get('previous', 'N/A')}% |")
                a(f"| Severity | {sev} |")
                if pledge.get('is_red_flag'):
                    a(f"\n> ⚠️ **Red Flag:** Promoter pledging exceeds 20% — "
                      f"risk of forced liquidation in market downturn.\n")
                a("")

        # ── Forensic Deep Dive (RPT, Contingent, Auditor) ────
        rpt = analysis.get('rpt', {})
        contingent = analysis.get('contingent', {})
        auditor_analysis = analysis.get('auditor_analysis', {})

        if any(x.get('available') for x in [rpt, contingent, auditor_analysis]):
            a("## 🔬 Forensic Deep Dive\n")

            # RPT
            if rpt.get('available'):
                a("### Related Party Transactions (RPT)\n")
                a(f"| Metric | Value |")
                a(f"|--------|------:|")
                if rpt.get('total_rpt_amount'):
                    a(f"| Total RPT Amount | ₹{rpt['total_rpt_amount']:,.0f} Cr |")
                if rpt.get('rpt_as_pct_revenue') is not None:
                    a(f"| RPT as % of Revenue | {rpt['rpt_as_pct_revenue']}% |")
                a(f"| Severity | {rpt.get('severity', 'N/A')} |")
                a(f"\n{rpt.get('flag', '')}\n")
                cats = rpt.get('categories', [])
                if cats:
                    a(f"**RPT Categories:** {', '.join(cats)}\n")

            # Contingent Liabilities
            if contingent.get('available'):
                a("### Contingent Liabilities\n")
                a(f"| Metric | Value |")
                a(f"|--------|------:|")
                if contingent.get('total_contingent'):
                    a(f"| Total Contingent | ₹{contingent['total_contingent']:,.0f} Cr |")
                if contingent.get('contingent_as_pct_networth') is not None:
                    a(f"| As % of Net Worth | {contingent['contingent_as_pct_networth']}% |")
                a(f"| Severity | {contingent.get('severity', 'N/A')} |")
                a("")

            # Auditor Analysis
            if auditor_analysis.get('available'):
                a("### Auditor Observations\n")
                a(f"**{auditor_analysis.get('summary', 'N/A')}**\n")
                flags = auditor_analysis.get('flags', [])
                if flags:
                    a("| Severity | Type | Observation |")
                    a("|:--------:|------|-------------|")
                    for fl in flags[:8]:
                        sev = fl.get('severity', 'LOW')
                        sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡",
                                    "LOW": "🟢"}.get(sev, "⚪")
                        a(f"| {sev_icon} {sev} | {fl.get('type', '')} "
                          f"| {fl.get('observation', '')[:200]} |")
                    a("")

        # ── Segmental Performance ────────────────────────────
        segmental = analysis.get('segmental', {})
        if segmental.get('available') and segmental.get('segments'):
            a("## 📊 Segmental Performance\n")
            if segmental.get('concentration_risk'):
                a(f"**Concentration Risk:** {segmental['concentration_risk']} "
                  f"(Dominant: {segmental.get('dominant_segment', 'N/A')} at "
                  f"{segmental.get('dominant_pct', 0):.1f}%)\n")

            a("| Segment | Revenue (₹ Cr) | EBIT (₹ Cr) | EBIT Margin | Revenue % |")
            a("|---------|---------------:|------------:|------------:|----------:|")
            for seg in segmental['segments']:
                rev = f"{seg.get('revenue', 0):,.0f}" if seg.get('revenue') else 'N/A'
                ebit = f"{seg.get('ebit', 0):,.0f}" if seg.get('ebit') else 'N/A'
                margin = f"{seg.get('ebit_margin', 0):.1f}%" if seg.get('ebit_margin') else 'N/A'
                pct = f"{seg.get('revenue_pct', 0):.1f}%" if seg.get('revenue_pct') else 'N/A'
                a(f"| {seg.get('name', '?')} | {rev} | {ebit} | {margin} | {pct} |")
            a("")

        # ── Forensic Dashboard (Unified) ────────────────────
        forensic_db = analysis.get('forensic_dashboard', {})
        if forensic_db.get('available'):
            a("## 🔬 Forensic Earnings Quality Dashboard\n")
            quality = forensic_db.get('quality_rating', 'N/A')
            f_score = forensic_db.get('forensic_score')
            q_icon = {'EXCELLENT': '🟢', 'GOOD': '🟢',
                      'AVERAGE': '🟡', 'POOR': '🔴',
                      'VERY_POOR': '🔴'}.get(quality, '⚪')
            a(f"**{q_icon} Forensic Score: {f_score if f_score is not None else 'N/A'}/10 — {quality}**\n")
            a(f"Passed: {forensic_db.get('num_passed', 0)} / "
              f"{forensic_db.get('num_checks', 0)} checks\n")

            checks = forensic_db.get('checks', [])
            if checks:
                a("| # | Check | Result | Details |")
                a("|--:|-------|:------:|---------|")
                for i, chk in enumerate(checks, 1):
                    status = chk.get('status', 'N/A')
                    # No emoji prefix — just the status word to prevent
                    # PDF text wrapping "PAS S" in narrow columns
                    a(f"| {i} | {chk.get('name', '?')} "
                      f"| {status} "
                      f"| {chk.get('detail', '')[:150]} |")
                a("")

            red_flags = forensic_db.get('red_flags', [])
            if red_flags:
                a("### 🚩 Red Flags\n")
                for rf in red_flags:
                    sev = rf.get('severity', 'MEDIUM')
                    sev_icon = {'HIGH': '🔴', 'MEDIUM': '🟡',
                                'LOW': '🟢'}.get(sev, '⚪')
                    a(f"- {sev_icon} **[{sev}] {rf.get('category', '')}:** "
                      f"{rf.get('detail', '')}")
                a("")

        # ── Governance Dashboard ─────────────────────────────
        governance = analysis.get('governance', {})
        if governance.get('available'):
            a("## 🏛️ Corporate Governance Dashboard\n")
            a(f"**Governance Score: {governance.get('governance_score', 'N/A')}/10**\n")

            board = governance.get('board_composition', {})
            meetings = governance.get('board_meetings', {})
            remuneration = governance.get('promoter_remuneration', {})

            a("| Metric | Value |")
            a("|--------|------:|")
            if board.get('total_directors'):
                a(f"| Board Size | {board['total_directors']} directors |")
            if board.get('independent_pct') is not None:
                a(f"| Independent Directors | {board['independent_pct']}% |")
            if meetings.get('count'):
                a(f"| Board Meetings (Year) | {meetings['count']} |")
            if meetings.get('attendance_pct'):
                a(f"| Average Attendance | {meetings['attendance_pct']}% |")
            if remuneration.get('total_cr'):
                a(f"| KMP Remuneration | ₹{remuneration['total_cr']} Cr |")
            if remuneration.get('as_pct_profit') is not None:
                a(f"| Remuneration as % PAT | {remuneration['as_pct_profit']}% |")
            a("")

            gov_flags = governance.get('flags', [])
            if gov_flags:
                a("**Governance Flags:**\n")
                for fl in gov_flags:
                    sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡",
                                "LOW": "🟢"}.get(fl['severity'], "⚪")
                    a(f"- {sev_icon} {fl['flag']}")
                a("")

        # ── Competitive Moat ─────────────────────────────────
        moat = analysis.get('moat', {})
        if moat.get('available'):
            a("## 🏰 Competitive Moat Analysis\n")
            a(f"**Moat Score: {moat.get('moat_score', 'N/A')}/10** "
              f"| Dominant: **{moat.get('dominant_moat', 'None')}**\n")

            advantages = moat.get('competitive_advantages', [])
            if advantages:
                a("### Detected Competitive Advantages\n")
                for adv in advantages:
                    a(f"- {adv}")
                a("")

            if moat.get('r_and_d_pct') is not None:
                a(f"| R&D as % Revenue | {moat['r_and_d_pct']}% |")
            if moat.get('patent_mentions'):
                a(f"| Patent Mentions | {moat['patent_mentions']} "
                  f"({moat.get('patent_grants', 0)} grants) |")

            claims = moat.get('market_share_claims', [])
            if claims:
                a("\n**Market Share Claims:**\n")
                for cl in claims[:5]:
                    a(f"> {cl}\n")

        # ── Say-Do Ratio ─────────────────────────────────────
        say_do = analysis.get('say_do', {})
        if say_do.get('available'):
            a("## 🤝 Say-Do Ratio — Management Credibility\n")
            _n_tracked = say_do.get('num_promises_tracked', 0)
            _n_delivered = say_do.get('num_delivered', 0)
            # Force recalculate: ratio = delivered / tracked (never trust cached value)
            if _n_tracked > 0:
                sd_ratio = round(_n_delivered / _n_tracked, 2)
            else:
                sd_ratio = None
            cred = say_do.get('credibility_rating', 'N/A')
            # Guard: if zero promises tracked, ratio is meaningless
            if _n_tracked == 0:
                sd_ratio = None
                cred = 'INSUFFICIENT_DATA' if cred not in ('INSUFFICIENT_DATA',) else cred
            cred_icon = {'EXCELLENT': '🟢', 'GOOD': '🟢',
                         'FAIR': '🟡', 'POOR': '🔴',
                         'VERY_POOR': '🔴'}.get(cred, '⚪')
            a(f"**{cred_icon} Say-Do Ratio: {f'{sd_ratio:.2f}' if sd_ratio is not None else 'N/A'} — {cred}**\n")
            a(f"Promises Tracked: {_n_tracked} | "
              f"Delivered: {say_do.get('num_delivered', 0)} | "
              f"Missed: {say_do.get('num_missed', 0)}\n")

            if say_do.get('is_governance_risk'):
                a("> 🔴 **GOVERNANCE RISK:** Management consistently misses "
                  "its own guidance — credibility below acceptable threshold.\n")

            comparisons = say_do.get('comparisons', [])
            if comparisons:
                a("| Topic | Promise | Actual | Status |")
                a("|-------|---------|--------|:------:|")
                for comp in comparisons[:10]:
                    status = comp.get('status', 'N/A')
                    s_icon = {'DELIVERED': '✅', 'MISSED': '❌',
                              'PARTIAL': '⚠️', 'PENDING': '⏳'}.get(status, '?')
                    a(f"| {comp.get('topic', '?')} "
                      f"| {comp.get('promise', '?')[:60]} "
                      f"| {comp.get('actual', '?')[:60]} "
                      f"| {s_icon} {status} |")
                a("")

            from config import config as _cfg_sd
            _sd_t = _cfg_sd.validation.say_do_threshold
            a(f"> 💡 *Say-Do Ratio > 1.0 means management over-delivers; "
              f"< {_sd_t} indicates persistent over-promising.*\n")

        # ── ESG / BRSR ──────────────────────────────────────
        esg = analysis.get('esg', {})
        if esg.get('available'):
            a("## 🌱 ESG / BRSR Intelligence\n")
            a(f"**ESG Score: {esg.get('esg_score', 'N/A')}/10** "
              f"| BRSR: {'✅ Found' if esg.get('brsr_found') else '❌ Not found'}\n")

            metrics = esg.get('metrics', {})
            if metrics:
                a("| ESG Metric | Value |")
                a("|------------|------:|")
                METRIC_LABELS = {
                    'energy_intensity': 'Energy Intensity',
                    'ghg_scope1': 'GHG Scope 1 (tCO2)',
                    'ghg_scope2': 'GHG Scope 2 (tCO2)',
                    'water_consumption': 'Water Consumption',
                    'waste_generated': 'Waste Generated',
                    'women_employees_pct': 'Women Employees (%)',
                    'women_board_pct': 'Women on Board (%)',
                    'safety_incidents': 'LTIFR',
                    'renewable_energy_pct': 'Renewable Energy (%)',
                    'csr_spend': 'CSR Spend (₹ Cr)',
                }
                for key, val in metrics.items():
                    label = METRIC_LABELS.get(key, key.replace('_', ' ').title())
                    a(f"| {label} | {val:,.2f} |")
                a("")

            targets = esg.get('carbon_targets', [])
            if targets:
                a("### 🎯 Carbon Targets\n")
                for t in targets:
                    a(f"> {t}\n")

            principles = esg.get('principles', [])
            if principles:
                a("### BRSR Principles\n")
                for p in principles:
                    a(f"- **P{p['number']}:** {p['description']}")
                a("")

        # ── Qualitative Intelligence ─────────────────────────
        qual = analysis.get('qualitative', {})
        if qual.get('available'):
            a("## 🧠 Qualitative Intelligence\n")

            # Sentiment
            sent = qual.get('sentiment', {})
            if sent.get('available'):
                a("### Management Sentiment\n")
                tone = sent.get('tone', sent.get('label', 'N/A'))
                sc = sent.get('overall_score', sent.get('score', 'N/A'))
                a(f"| Metric | Value |")
                a(f"|--------|------:|")
                a(f"| Overall Tone | **{tone}** |")
                a(f"| Sentiment Score | {sc} |")
                num_c = sent.get('num_chunks')
                if num_c:
                    a(f"| Chunks Analyzed | {num_c} |")
                # Q&A section sentiment (3x more predictive)
                qa_score = sent.get('qa_score')
                qa_tone = sent.get('qa_tone')
                if qa_score is not None:
                    a(f"| Q&A Section Score | {qa_score} |")
                    a(f"| Q&A Tone | **{qa_tone}** |")
                    a(f"| Q&A Chunks | {sent.get('qa_num_chunks', 'N/A')} |")
                mgmt_score = sent.get('mgmt_score')
                mgmt_tone = sent.get('mgmt_tone')
                if mgmt_score is not None:
                    a(f"| Mgmt Remarks Score | {mgmt_score} |")
                    a(f"| Mgmt Remarks Tone | **{mgmt_tone}** |")
                a("")
                if qa_score is not None and mgmt_score is not None:
                    a("> 💡 *Analyst Q&A sentiment is typically 3x more predictive "
                      "of future stock performance than prepared management remarks.*\n")

            # Management Delta
            mgmt = qual.get('management_delta', {})
            if mgmt.get('available'):
                a("### Management Guidance Delta\n")
                a(f"**{mgmt.get('summary', 'N/A')}**\n")
                comps = mgmt.get('comparisons', [])
                if comps:
                    # ── Aggregate duplicates: one row per topic ─────
                    from collections import defaultdict, Counter
                    _topic_agg = defaultdict(lambda: {'cur': [], 'pri': [], 'delta': [], 'sev': []})
                    for c in comps:
                        t = c.get('topic', '?')
                        _topic_agg[t]['cur'].append(c.get('current_sentiment', 0))
                        _topic_agg[t]['pri'].append(c.get('prior_sentiment', 0))
                        d = c.get('delta', {})
                        _topic_agg[t]['delta'].append(d.get('delta', 0))
                        _topic_agg[t]['sev'].append(d.get('severity', '?'))

                    a("| Topic | Current Sent. | Prior Sent. | Δ | Signal |")
                    a("|-------|:------------:|:-----------:|--:|--------|")
                    for topic, vals in _topic_agg.items():
                        _cur = sum(vals['cur']) / len(vals['cur'])
                        _pri = sum(vals['pri']) / len(vals['pri'])
                        _dlt = sum(vals['delta']) / len(vals['delta'])
                        _sev = Counter(vals['sev']).most_common(1)[0][0]
                        a(f"| {topic} | "
                          f"{_cur:.2f} | "
                          f"{_pri:.2f} | "
                          f"{_dlt:+.2f} | "
                          f"{_sev} |")
                    a("")

            # Key Themes
            themes = qual.get('key_themes', {})
            if themes:
                a("### Key Themes from Transcripts\n")
                for query, snippets in themes.items():
                    a(f"**{query.title()}:**")
                    for s in snippets[:2]:
                        snip = self._smart_truncate(s, 600)
                        a(f"> {snip}\n")
                a("")

        # ── Text Intelligence (NEW) ──────────────────────────
        if text_intel.get('available'):
            a("## 📄 Text Intelligence Summary\n")
            a(f"**Sources Analyzed:** {text_intel.get('num_sources', 0)} "
              f"| Overall Tone: **{text_intel.get('overall_tone', 'N/A')}**\n")

            src = text_intel.get('source_breakdown', {})
            a(f"- Concall transcripts: {src.get('concall', 0)}")
            a(f"- Annual report sections: {src.get('annual_report', 0)}")
            a(f"- Announcements: {src.get('announcement', 0)}")
            a("")

            # Key insights
            insights = text_intel.get('insights', [])
            if insights:
                a("### Key Insights\n")
                for ins in insights[:10]:
                    a(f"- {ins}")
                a("")

            # Company status, plans, risks
            status = text_intel.get('company_status', [])
            if status:
                a("### Company Status\n")
                for s in status[:5]:
                    a(f"> {self._smart_truncate(s, 500)}\n")

            plans = text_intel.get('plans', [])
            if plans:
                a("### Plans & Strategy\n")
                for p in plans[:5]:
                    a(f"> {self._smart_truncate(p, 500)}\n")

            risks = text_intel.get('risks', [])
            if risks:
                a("### Risk Signals (from text)\n")
                for r in risks[:5]:
                    a(f"> ⚠️ {self._smart_truncate(r, 500)}\n")

            opps = text_intel.get('opportunities', [])
            if opps:
                a("### Opportunities\n")
                for o in opps[:5]:
                    a(f"> 🟢 {self._smart_truncate(o, 500)}\n")

            # Forward-looking statements
            fwd = text_intel.get('forward_looking', [])
            if fwd:
                a("### Forward-Looking Statements\n")
                for f_stmt in fwd[:5]:
                    a(f"- {self._smart_truncate(f_stmt, 600)}")
                a("")

            # Topic breakdown with sentiment
            topic_analysis = text_intel.get('topic_analysis', {})
            if topic_analysis:
                a("### Topic Sentiment Breakdown\n")
                a("| Topic | Mentions | Coverage | Sentiment |")
                a("|-------|--------:|:--------:|:---------:|")
                for topic, info in sorted(
                        topic_analysis.items(),
                        key=lambda x: -x[1].get('mention_count', 0)):
                    count = info.get('mention_count', 0)
                    coverage = info.get('coverage', '—')
                    tone = info.get('sentiment_tone', '—')
                    tone_icon = {'POSITIVE': '🟢', 'NEGATIVE': '🔴',
                                 'NEUTRAL': '🟡'}.get(tone, '⚪')
                    a(f"| {topic} | {count} | {coverage} | "
                      f"{tone_icon} {tone} |")
                a("")

        # ── Predictive Model ─────────────────────────────────
        pred = analysis.get('prediction', {})
        if pred.get('available'):
            garch_name = pred.get('garch_model', 'N/A')
            if garch_name and garch_name != 'N/A':
                a(f"## 📈 Price Forecast (ARIMA-ETS + {garch_name} Volatility)\n")
            else:
                a("## 📈 Price Forecast (30-Day ARIMA-ETS Ensemble)\n")
            a("| Metric | Value |")
            a("|--------|------:|")
            _lp = pred.get('last_price')
            a(f"| Last Close | {f'₹{_lp:,.2f}' if _lp is not None else 'N/A'} |")
            _ep = pred.get('end_price')
            a(f"| 30-Day Target | {f'₹{_ep:,.2f}' if _ep is not None else 'N/A'} |")
            _pc = pred.get('pct_change_30d')
            a(f"| Expected Move | {f'{_pc:+.1f}%' if _pc is not None else 'N/A'} |")
            a(f"| Trend Signal | **{pred.get('trend', 'N/A')}** |")

            # GARCH volatility metrics
            _vol_regime = pred.get('vol_regime')
            if _vol_regime and _vol_regime != 'Unknown':
                regime_icon = {'LOW': '🟢', 'MEDIUM': '🟡', 'HIGH': '🔴'}.get(
                    _vol_regime, '⚪')
                a(f"| Volatility Regime | {regime_icon} **{_vol_regime}** |")
            _ann_vol = pred.get('annualised_vol_pct')
            if _ann_vol is not None:
                a(f"| Annualised Volatility | {_ann_vol:.1f}% |")
            _cond_vol = pred.get('conditional_vol_pct')
            if _cond_vol is not None:
                a(f"| Current Conditional σ | {_cond_vol:.2f}% (daily) |")
            if garch_name and garch_name != 'N/A':
                a(f"| Volatility Model | {garch_name} (Student-t) |")
            a("")

            # Confidence interval endpoints
            ci_lo = pred.get('ci_lower', [])
            ci_hi = pred.get('ci_upper', [])
            if ci_lo and ci_hi:
                a(f"> 95% Confidence Band (Day 30): "
                  f"Rs. {ci_lo[-1]:,.2f} - Rs. {ci_hi[-1]:,.2f}\n")

            a("> ⚠️ *Statistical model — not investment advice. "
              "Past patterns may not persist.*\n")

        # ── Technical Analysis (NEW) ─────────────────────────
        if tech.get('available'):
            a("## 🔧 Technical Analysis\n")

            # Composite signal
            sig = tech.get('overall_signal', {})
            signal = sig.get('signal', 'NEUTRAL')
            sig_icon = {'STRONG_BULLISH': '🟢🟢', 'MILDLY_BULLISH': '🟢',
                        'STRONG_BEARISH': '🔴🔴', 'MILDLY_BEARISH': '🔴',
                        'NEUTRAL': '🟡'}.get(signal, '🟡')
            a(f"### Overall Signal: {sig_icon} {signal} "
              f"(Confidence: {sig.get('confidence', 'N/A')})\n")
            a(f"Bull signals: {sig.get('bull_count', 0)} | "
              f"Bear signals: {sig.get('bear_count', 0)} | "
              f"Total: {sig.get('total', 0)}\n")

            # Trend
            trend_t = tech.get('trend', {})
            if trend_t.get('available'):
                a("### Moving Averages & Trend\n")
                a("| Indicator | Value |")
                a("|-----------|------:|")
                if trend_t.get('dma_50'):
                    icon = '✅' if trend_t.get('above_50dma') else '❌'
                    a(f"| 50-DMA | ₹{trend_t['dma_50']:,.2f} ({icon} Above) |")
                if trend_t.get('dma_200'):
                    icon = '✅' if trend_t.get('above_200dma') else '❌'
                    a(f"| 200-DMA | ₹{trend_t['dma_200']:,.2f} ({icon} Above) |")
                if trend_t.get('pct_from_50dma') is not None:
                    a(f"| % from 50-DMA | {trend_t['pct_from_50dma']:+.2f}% |")
                if trend_t.get('pct_from_200dma') is not None:
                    a(f"| % from 200-DMA | {trend_t['pct_from_200dma']:+.2f}% |")
                cross = trend_t.get('cross_signal')
                if cross:
                    a(f"| Cross Signal | {cross} |")
                direction = trend_t.get('short_term_direction')
                if direction:
                    a(f"| 20-Day Direction | {direction} |")
                a("")

            # Momentum
            mom = tech.get('momentum', {})
            if mom.get('available'):
                a("### Momentum Indicators\n")
                a("| Indicator | Value | Signal |")
                a("|-----------|------:|--------|")
                if mom.get('rsi') is not None:
                    rsi = mom['rsi']
                    rsi_icon = '🔴' if rsi > 70 else ('🟢' if rsi < 30 else '🟡')
                    a(f"| RSI (14) | {rsi:.1f} | {rsi_icon} {mom.get('rsi_signal', '')} |")
                if mom.get('macd') is not None:
                    cross_sig = mom.get('macd_crossover', '')
                    a(f"| MACD | {mom['macd']:.4f} | {cross_sig} |")
                if mom.get('roc_20d') is not None:
                    a(f"| ROC (20d) | {mom['roc_20d']:+.2f}% | — |")
                if mom.get('high_52w') is not None:
                    a(f"| 52W High | ₹{mom['high_52w']:,.2f} "
                      f"({mom.get('pct_from_52w_high', 0):+.1f}%) | — |")
                if mom.get('low_52w') is not None:
                    a(f"| 52W Low | ₹{mom['low_52w']:,.2f} "
                      f"({mom.get('pct_from_52w_low', 0):+.1f}%) | — |")
                a("")

            # Volume
            vol = tech.get('volume_analysis', {})
            if vol.get('available'):
                a("### Volume Analysis\n")
                a("| Metric | Value |")
                a("|--------|------:|")
                if vol.get('latest_volume'):
                    a(f"| Latest Volume | {vol['latest_volume']:,} |")
                if vol.get('avg_volume_20d'):
                    a(f"| 20-Day Avg Volume | {vol['avg_volume_20d']:,} |")
                if vol.get('relative_volume') is not None:
                    rv = vol['relative_volume']
                    rv_icon = '🔥' if rv > 1.5 else ('📉' if rv < 0.5 else '📊')
                    a(f"| Relative Volume | {rv_icon} {rv:.2f}x |")
                if vol.get('volume_trend'):
                    a(f"| Volume Trend | {vol['volume_trend']} |")
                if vol.get('obv_trend'):
                    obv_icon = '🟢' if vol['obv_trend'] == 'ACCUMULATION' else '🔴'
                    a(f"| OBV Trend | {obv_icon} {vol['obv_trend']} |")
                a("")
                div_sig = vol.get('divergence_signal')
                if div_sig and vol.get('divergence', 'NONE') != 'NONE':
                    a(f"> {div_sig}\n")

            # Volatility
            volatility = tech.get('volatility', {})
            if volatility.get('available'):
                a("### Volatility\n")
                a("| Metric | Value |")
                a("|--------|------:|")
                if volatility.get('atr_14'):
                    a(f"| ATR (14) | ₹{volatility['atr_14']:,.2f} "
                      f"({volatility.get('atr_pct', 0):.2f}%) |")
                if volatility.get('bb_width_pct'):
                    a(f"| Bollinger Band Width | {volatility['bb_width_pct']:.1f}% |")
                if volatility.get('bb_position'):
                    a(f"| BB Position | {volatility['bb_position']} |")
                if volatility.get('hist_volatility_20d'):
                    a(f"| Hist. Volatility (20d, ann.) | {volatility['hist_volatility_20d']:.1f}% |")
                a("")

        # ── Market Correlation ───────────────────────────────
        fc = analysis.get('flow_corr', {})
        if fc.get('available'):
            a("## 🔗 Market Correlation & Relative Strength\n")
            a("| Metric | Value |")
            a("|--------|------:|")
            a(f"| Correlation with Nifty50 (30d) | "
              f"{fc.get('current_corr_with_market', 'N/A')} |")
            a(f"| Average Correlation | {fc.get('avg_corr', 'N/A')} |")
            a(f"| Regime | {fc.get('regime', 'N/A')} |")
            a(f"| Relative Strength | **{fc.get('relative_strength_trend', 'N/A')}** |")
            a(f"| RS 30d Ratio | {fc.get('rs_30d_ratio', 'N/A')} |")
            sect_corr = fc.get('current_corr_with_sector')
            if sect_corr is not None:
                a(f"| Sector Correlation | {sect_corr} |")
            a("")

        # ── Macro-Correlation Engine ─────────────────────────
        macro_corr = analysis.get('macro_corr', {})
        if macro_corr.get('available'):
            a("## 🌐 Macro-Correlation Engine (ARDL)\n")

            # ARDL summary
            ardl = macro_corr.get('ardl', {})
            if ardl:
                a(f"**ARDL Model R²: {ardl.get('r_squared', 0):.3f}** "
                  f"| Significant Macro Factors: "
                  f"{len(ardl.get('significant_factors', []))}\n")
                sig_factors = ardl.get('significant_factors', [])
                coefficients = ardl.get('coefficients', {})
                if sig_factors:
                    a("| Factor | Lag | Coeff | p-value |")
                    a("|--------|----:|------:|--------:|")
                    for sf in sig_factors:
                        if isinstance(sf, dict):
                            # Already a dict with full details
                            a(f"| {sf.get('factor', '?')} "
                              f"| {sf.get('lag', 0)} "
                              f"| {sf.get('coefficient', 0):.4f} "
                              f"| {sf.get('p_value', 1):.4f} |")
                        else:
                            # sf is a string key like 'crude_oil_lag1'
                            info = coefficients.get(sf, {})
                            # Parse lag from name (e.g. 'crude_oil_lag5' → 5)
                            lag_num = ''
                            if '_lag' in str(sf):
                                lag_num = str(sf).rsplit('_lag', 1)[-1]
                            a(f"| {sf} "
                              f"| {lag_num} "
                              f"| {info.get('coefficient', 0):.4f} "
                              f"| {info.get('p_value', 1):.4f} |")
                    a("")

            # Correlations table
            correlations = macro_corr.get('correlations', {})
            if correlations:
                a("### Macro Correlations (Lag 0 / 5-day / 20-day)\n")
                a("| Macro Variable | Lag-0 | Lag-5 | Lag-20 |")
                a("|----------------|------:|------:|-------:|")
                for var_name, corr_data in correlations.items():
                    lags = corr_data.get('lags', corr_data)
                    l0 = lags.get('lag_0d', lags.get('lag_0', 'N/A'))
                    l5 = lags.get('lag_5d', lags.get('lag_5', 'N/A'))
                    l20 = lags.get('lag_20d', lags.get('lag_20', 'N/A'))
                    if isinstance(l0, float):
                        l0 = f"{l0:.3f}"
                    if isinstance(l5, float):
                        l5 = f"{l5:.3f}"
                    if isinstance(l20, float):
                        l20 = f"{l20:.3f}"
                    a(f"| {var_name} | {l0} | {l5} | {l20} |")
                a("")

            # Sector sensitivity
            sect_sens = macro_corr.get('sector_sensitivity', {})
            if sect_sens:
                a("### Sector Macro Sensitivity\n")
                sector_name = sect_sens.get('sector',
                              sect_sens.get('matched_sector',
                              macro_corr.get('sector', 'N/A')))
                a(f"**Sector:** {sector_name.title()}\n")
                # Try key_indicators list format first
                indicators = sect_sens.get('key_indicators', [])
                if indicators:
                    for ind in indicators:
                        level = ind.get('sensitivity', 'LOW')
                        l_icon = {'HIGH': '🔴', 'MEDIUM': '🟡',
                                  'LOW': '🟢'}.get(level, '⚪')
                        a(f"- {l_icon} **{ind.get('indicator', '?')}** "
                          f"— Sensitivity: {level}")
                    a("")
                # Fallback: profile dict format
                elif sect_sens.get('profile'):
                    profile = sect_sens['profile']
                    for indicator, level in profile.items():
                        l_icon = {'HIGH': '🔴', 'MEDIUM': '🟡',
                                  'LOW': '🟢'}.get(level, '⚪')
                        a(f"- {l_icon} **{indicator.replace('_', ' ').title()}** "
                          f"— Sensitivity: {level}")
                    a("")

            # Signals
            signals = macro_corr.get('signals', [])
            if signals:
                a("### Macro Signals\n")
                for sig in signals:
                    a(f"- {sig}")
                a("")

        # ── Macro Context ────────────────────────────────────
        macro = data.get('macro', {})
        if macro.get('available'):
            a("## 🌍 Macro Context\n")
            a("| Indicator | Value |")
            a("|-----------|------:|")
            for key in ['nifty50', 'crude_oil_usd', 'usdinr', 'gold_usd', 'india_vix']:
                v = macro.get(key)
                if v is not None:
                    label = key.replace('_', ' ').title()
                    if isinstance(v, float):
                        a(f"| {label} | {v:,.2f} |")
                    else:
                        a(f"| {label} | {v} |")
            a("")

            beta_info = data.get('beta_info', {})
            if beta_info.get('available'):
                a(f"| Beta (vs Nifty50) | {beta_info.get('beta', 'N/A')} |")
                a(f"| R² | {beta_info.get('r_squared', 'N/A')} |")
                a("")

        # ── Validation & Trust Score ─────────────────────────
        validation = analysis.get('validation', {})
        if validation and validation.get('available', True) is not False:
            a("## 📋 Data Validation — Annual Report Cross-Check\n")

            ts = validation.get('trust_score')
            tl = validation.get('trust_label', '')
            if ts is not None:
                from config import config as _cfg
                _v = _cfg.validation
                icon = ("🟢" if ts >= _v.trust_high
                        else ("🟡" if ts >= _v.trust_moderate else "🔴"))
                a(f"**{icon} Trust Score: {ts} / 100 — {tl}**\n")
                a("> The Trust Score measures how closely the scraped financial "
                  "data matches the official Annual Report. A high score means "
                  "the numbers used in this analysis are verifiable.\n")

            checks = validation.get('checks', [])
            if checks:
                a("### Check Results\n")
                a("| # | Metric | Scraper | Annual Report | Status |")
                a("|--:|--------|--------:|--------------:|:------:|")
                for i, chk in enumerate(checks, 1):
                    # Use plain text status — emojis garble in PDF
                    status_text = chk.get('status', 'N/A')
                    scraper_val = chk.get('scraper_value', 'N/A')
                    ar_val = chk.get('ar_value', 'N/A')
                    if isinstance(scraper_val, float):
                        scraper_val = f"{scraper_val:,.2f}"
                    if isinstance(ar_val, float):
                        # Show normalised (Crore) value when unit-adjusted
                        if chk.get('unit_adjusted') and chk.get('ar_value_normalised') is not None:
                            ar_val = f"{chk['ar_value_normalised']:,.2f} (Cr)"
                        else:
                            ar_val = f"{ar_val:,.2f}"
                    a(f"| {i} | {chk.get('metric', '?')} "
                      f"| {scraper_val} | {ar_val} | {status_text} |")
                a("")

            fn_flags = validation.get('footnote_flags', [])
            if fn_flags:
                a("### 📝 Footnote Flags\n")
                a("| Severity | Flag | Impact |")
                a("|:--------:|------|--------|")
                for fl in fn_flags:
                    sev = fl.get('severity', 'LOW')
                    sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠",
                                "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                    flag_label = (fl.get('title') or
                                  fl.get('type', '').replace('_', ' ').title())
                    a(f"| {sev_icon} {sev} | {flag_label} "
                      f"| {fl.get('impact', '')} |")
                a("")

            auditor = validation.get('auditor_flags', [])
            if auditor:
                a("### 🔎 Auditor Observations\n")
                for obs in auditor:
                    sev = obs.get('severity', 'LOW')
                    sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠",
                                "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                    a(f"- {sev_icon} **{sev}**: {obs.get('observation', '')}")
                a("")

            cl = validation.get('contingent_flag')
            if cl and cl.get('available'):
                a("### ⚡ Contingent Liabilities\n")
                sev = cl.get('severity', 'LOW')
                sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠",
                            "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                a(f"- {sev_icon} {cl.get('detail', 'N/A')}")
                a("")
        elif validation and validation.get('reason'):
            a("## 📋 Data Validation\n")
            a(f"> ⏭️ Validation skipped — {validation['reason']}\n")

        # ── Risks ────────────────────────────────────────────
        a("## ⚠️ Risk Factors & Red Flags\n")
        risks = self._identify_risks(ratios, dcf, ms, fs, analysis)
        for r in risks:
            a(f"- {r}")
        if not risks:
            a("- No major red flags identified.")
        a("")

        # ── Data Sources ─────────────────────────────────────
        a("## 📚 Data Sources\n")
        a("| Source | Usage |")
        a("|-------|-------|")
        a("| Screener.in | Financial statements, ratios, shareholding |")
        a("| BSE India | Annual reports, corporate announcements |")
        a("| Yahoo Finance (yfinance) | Market prices, beta, macro indicators, peer multiples |")
        a("| Company Annual Report (PDF) | Cross-validation of scraped data |")
        a("")

        # ── Compliance Disclaimer ────────────────────────────
        a("---\n")
        a("## ⚖️ Disclaimer (SEBI Compliance)\n")
        a(f"> {DISCLAIMER}\n")
        a(f"> {stamp_source('Report generated by automated equity-research system.')}")
        a("")
        a("> **SEBI (Research Analysts) Regulations, 2014 — Reg 16(4):** "
          "This is an AI-generated research report. The system and its "
          "operators do not hold any position in the security analyzed. "
          "No compensation has been received for this report. "
          "All data is from publicly available sources.\n")

        return "\n".join(lines)

    # ==================================================================
    # Risk identification (enhanced)
    # ==================================================================
    def _identify_risks(self, ratios, dcf, ms, fs, analysis=None) -> list:
        risks = []
        analysis = analysis or {}

        if ms.get('available') and ms.get('risk_level') == 'HIGH':
            risks.append("🔴 **Earnings Manipulation Alert** — "
                         "Beneish M-Score indicates high probability of manipulation.")

        if fs.get('available') and fs.get('strength') == 'WEAK':
            risks.append("🔴 **Weak Financials** — "
                         "Piotroski F-Score signals poor financial health.")

        de = ratios.get('debt_to_equity')
        # D/E threshold: compare to peer median if available, else flag extremes only
        peer = analysis.get('peer_cca', {})
        peer_de_median = peer.get('sector_de_median')
        if de is not None and isinstance(de, (int, float)):
            if peer_de_median is not None and de > peer_de_median * 2:
                risks.append(f"🟡 **High Leverage** — D/E of {de:.2f} is {de/peer_de_median:.1f}× "
                             f"the sector median ({peer_de_median:.2f}).")
            elif peer_de_median is None and de > 3.0:
                # Without peer context, only flag genuinely extreme leverage
                risks.append(f"🟡 **High Leverage** — D/E of {de:.2f} (no peer comparison available).")

        pg = ratios.get('profit_growth')
        if pg is not None and pg < 0:
            # Flag any declining profits — severity is proportional to the decline
            risks.append(f"🟡 **Declining Profits** — YoY profit growth {pg:+.1f} %.")

        pe = ratios.get('pe_ratio')
        # P/E threshold: compare to peer/sector P/E if available
        sector_pe = peer.get('sector_pe_median')
        if pe is not None and isinstance(pe, (int, float)):
            if sector_pe is not None and pe > sector_pe * 2:
                risks.append(f"🟡 **Rich Valuation** — P/E {pe:.1f}x is {pe/sector_pe:.1f}× "
                             f"the sector median ({sector_pe:.1f}x).")
            elif sector_pe is None and pe > 100:
                # Without sector context, only flag extreme P/E
                risks.append(f"🟡 **Rich Valuation** — P/E {pe:.1f}x (no peer comparison available).")

        if dcf.get('available'):
            dcf_guardrail = dcf.get('dcf_ev_mismatch', False)
            _data_suspended = analysis.get('rating', {}).get('data_suspended', False)
            up = dcf.get('upside_pct')
            # Only reference DCF valuation if neither guardrail nor
            # data-suspension was triggered.
            if up is not None and up < 0 and not dcf_guardrail and not _data_suspended:
                risks.append(f"🔴 **Overvalued per DCF** — "
                             f"Stock appears {abs(up):.1f} % overvalued.")
            elif dcf_guardrail:
                from config import config as _cfg
                _ev_d = dcf.get('ev_delta_pct', '?')
                _ev_t = _cfg.validation.dcf_ev_threshold_pct
                risks.append(
                    f"EV deviation {_ev_d}% exceeds {_ev_t:.0f}% threshold; DCF valuation suppressed."
                )

        ic = ratios.get('interest_coverage')
        if ic is not None and isinstance(ic, (int, float)) and ic < 1:
            risks.append(f"🟡 **Low Interest Coverage** — {ic:.2f}x; "
                         "earnings do not cover interest expense.")

        # CFO/EBITDA red flag
        cfo = analysis.get('cfo_ebitda_check', {})
        if cfo.get('available') and cfo.get('is_red_flag'):
            risks.append(f"🔴 **Cash Flow Quality Concern** — CFO/EBITDA at "
                         f"{cfo.get('ratio', '?')}%; profits may not be cash-backed.")

        # Sentiment red flag
        sent = analysis.get('sentiment', {})
        if sent.get('available') and sent.get('tone') in ('BEARISH', 'CAUTIOUS'):
            risks.append(f"🟡 **Negative Management Tone** — "
                         f"Sentiment: {sent.get('tone')}")

        # Prediction red flag
        pred = analysis.get('prediction', {})
        if pred.get('available') and pred.get('trend') in ('BEARISH', 'MILDLY BEARISH'):
            risks.append(f"🟡 **Bearish Technical Signal** — "
                         f"30-day model: {pred.get('trend')} "
                         f"({pred.get('pct_change_30d', 0):+.1f}%)")

        # RPT red flag
        rpt = analysis.get('rpt', {})
        if rpt.get('available') and rpt.get('severity') in ('HIGH', 'CRITICAL'):
            risks.append(f"🔴 **High Related Party Exposure** — {rpt.get('flag', '')}")

        # Contingent liabilities red flag
        contingent = analysis.get('contingent', {})
        if contingent.get('available') and contingent.get('severity') in ('HIGH', 'CRITICAL'):
            risks.append(f"🔴 **Large Contingent Liabilities** — "
                         f"{contingent.get('detail', '')}")

        # Auditor red flag
        aud = analysis.get('auditor_analysis', {})
        if aud.get('available') and aud.get('has_critical_flags'):
            risks.append(f"🔴 **Auditor Qualification/Concern** — "
                         f"{aud.get('summary', '')}")

        # Pledging red flag
        shp = analysis.get('shareholding', {})
        pledge = shp.get('PromoterPledging', {}) if isinstance(shp, dict) else {}
        if pledge.get('is_red_flag'):
            risks.append(f"🔴 **High Promoter Pledging** — "
                         f"{pledge.get('current', 'N/A')}% pledged. "
                         f"Risk of forced liquidation.")

        # Governance red flag
        governance = analysis.get('governance', {})
        if governance.get('available'):
            _gs = governance.get('governance_score')
            if _gs is not None and _gs < 5:
                risks.append(f"🟡 **Governance Concerns** — "
                             f"Score {_gs}/10")

        # Technical signal red flag
        tech = analysis.get('technicals', {})
        if tech.get('available'):
            sig = tech.get('overall_signal', {}).get('signal', '')
            if sig in ('STRONG_BEARISH', 'MILDLY_BEARISH'):
                risks.append(f"🟡 **Bearish Technical Setup** — {sig}")
            vol = tech.get('volume_analysis', {})
            if vol.get('divergence') == 'BEARISH_DIVERGENCE':
                risks.append("🟡 **Bearish Volume Divergence** — "
                             "Price rising on declining volume")

        # 5Y Trend deterioration
        trends = analysis.get('trends', {})
        if trends.get('available') and trends.get('overall_direction') == 'DETERIORATING':
            _th = trends.get('health_score')
            risks.append(f"🔴 **Deteriorating 5Y Trends** — "
                         f"Health score {_th if _th is not None else 'N/A'}/10")

        # Forensic Dashboard red flags
        forensic_db = analysis.get('forensic_dashboard', {})
        if forensic_db.get('available'):
            for rf in forensic_db.get('red_flags', []):
                if rf.get('severity') == 'HIGH':
                    risks.append(f"🔴 **Forensic: {rf.get('category', '')}** — "
                                 f"{rf.get('detail', '')[:100]}")

        # Say-Do governance risk
        say_do = analysis.get('say_do', {})
        if say_do.get('available') and say_do.get('is_governance_risk'):
            from config import config as _cfg
            _sdr = say_do.get('say_do_ratio')
            risks.append(f"🔴 **Management Credibility Risk** — "
                         f"Say-Do Ratio {f'{_sdr:.2f}' if _sdr is not None else 'N/A'} "
                         f"(below {_cfg.validation.say_do_threshold} threshold)")

        # Macro headwinds
        macro_corr = analysis.get('macro_corr', {})
        if macro_corr.get('available'):
            headwinds = [s for s in macro_corr.get('signals', [])
                         if 'headwind' in s.lower()]
            if len(headwinds) >= 2:
                risks.append(f"🟡 **Multiple Macro Headwinds** — "
                             f"{len(headwinds)} adverse macro factors detected")

        return risks

    # ==================================================================
    # Save to disk
    # ==================================================================
    def save(self, report: str, symbol: str,
             output_dir: str = "./output") -> str:
        os.makedirs(output_dir, exist_ok=True)
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        fname    = f"{symbol}_Research_{date_str}.md"
        fpath    = os.path.join(output_dir, fname)
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(report)
        return fpath
