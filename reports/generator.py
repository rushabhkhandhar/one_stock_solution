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
  10. Text Intelligence (Document Extraction)
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
            from qualitative.text_intelligence import clean_transcript_noise
            text = clean_transcript_noise(text)
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
        # Tier 1 features
        qshp   = analysis.get('quarterly_shareholding', {})

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
            ('PEG Ratio',             'peg_ratio',       '{:.2f}'),
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

        # Show EPS corporate-action adjustment note if detected
        if ratios.get('eps_adjusted'):
            a(f"> ℹ️ **EPS Adjusted:** {ratios.get('eps_adjustment_reason', 'Corporate action detected.')}\n")

        # PEG ratio interpretation
        peg = ratios.get('peg_ratio')
        if peg is not None:
            peg_growth_used = ratios.get('peg_growth_used', 'Earnings Growth')
            if peg < 1:
                a(f"> 📊 **PEG Ratio {peg:.2f}** (using {peg_growth_used}) — "
                  f"Stock appears undervalued relative to its growth rate.\n")
            elif peg > 2:
                a(f"> 📊 **PEG Ratio {peg:.2f}** (using {peg_growth_used}) — "
                  f"Stock appears overvalued relative to its growth rate.\n")
            else:
                a(f"> 📊 **PEG Ratio {peg:.2f}** (using {peg_growth_used}) — "
                  f"Fairly valued relative to growth.\n")

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
                    if accel == 'DECELERATING_CORP_ACTION':
                        accel = '⚠️ CORP ACTION'
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

            # Corporate-action context note
            if trends.get('corp_action_detected'):
                _ca_yr = trends.get('corp_action_year', '?')
                _ca_metrics = [m for m in metrics
                               if m.get('acceleration') == 'DECELERATING_CORP_ACTION']
                if _ca_metrics:
                    _ca_names = ', '.join(m['label'] for m in _ca_metrics[:4])
                    a(f"> ℹ️ *Corporate Action Detected ({_ca_yr}): "
                      f"Shares outstanding expanded >80%, indicating "
                      f"a likely stock split, bonus issue, or merger. "
                      f"{_ca_names} show deceleration that is partially "
                      f"or wholly attributable to per-share dilution "
                      f"rather than fundamental business deterioration. "
                      f"Evaluate absolute revenue / profit growth "
                      f"alongside per-share metrics.*\n")

            # Deceleration analyst context
            _decel_metrics = [m for m in metrics
                              if m.get('acceleration') == 'DECELERATING']
            if len(_decel_metrics) >= 2:
                _decel_names = ', '.join(
                    m['label'] for m in _decel_metrics[:4])
                # Check if revenue is large (> ₹50,000 Cr = mature base)
                _rev_m = next((m for m in metrics
                               if m.get('label') == 'Revenue'), {})
                _rev_latest = _rev_m.get('latest', 0)
                if _rev_latest > 50000:
                    a(f"> 💡 *Analyst Note: {_decel_names} show "
                      f"decelerating growth — this is mathematically "
                      f"expected for a company at ₹{_rev_latest:,.0f} Cr "
                      f"annual revenue. Maintaining historical hyper-"
                      f"growth CAGRs becomes physically impossible at "
                      f"this scale (base effect). Investors should "
                      f"recalibrate: the transition from high-growth "
                      f"disruptor to steady-state cash-generating "
                      f"compounder is a sign of maturity, not "
                      f"deterioration.*\n")
                else:
                    a(f"> ⚠️ *{_decel_names} show decelerating "
                      f"growth. If this deceleration persists across "
                      f"multiple quarters, it may signal competitive "
                      f"pressure or demand softening rather than a "
                      f"temporary blip.*\n")

        # ── Tier 2: DuPont Decomposition ────────────────────
        dupont = analysis.get('dupont', {})
        if dupont.get('available'):
            a("## 🔬 DuPont Decomposition (5-Factor ROE Breakdown)\n")
            a("| Factor | Value | Interpretation |")
            a("|--------|------:|:---------------|")
            factor_labels = {
                'tax_burden': ('Tax Burden', 'Net Income / PBT — higher = less tax drag'),
                'interest_burden': ('Interest Burden', 'PBT / EBIT — higher = less interest cost'),
                'ebit_margin': ('EBIT Margin', 'EBIT / Revenue — core operating efficiency'),
                'asset_turnover': ('Asset Turnover', 'Revenue / Total Assets — asset utilisation'),
                'equity_multiplier': ('Equity Multiplier', 'Total Assets / Equity — leverage'),
            }
            for key, (label, interp) in factor_labels.items():
                val = dupont.get(key)
                if val is not None:
                    a(f"| {label} | {val:.3f} | {interp} |")
            a("")
            roe_dp = dupont.get('roe_dupont')
            if roe_dp is not None:
                a(f"**Computed ROE (DuPont):** {roe_dp:.2f}%\n")
            weakest = dupont.get('weakest_factor')
            strongest = dupont.get('strongest_factor')
            if weakest:
                a(f"> ⚠️ **Weakest Factor:** {weakest} — this is the primary "
                  f"drag on ROE and the area management should prioritise.\n")
            if strongest:
                a(f"> ✅ **Strongest Factor:** {strongest} — competitive "
                  f"advantage embedded here.\n")

            history = dupont.get('history', [])
            if history:
                a("### DuPont Factor History\n")
                a("| Year | Tax Burden | Interest Burden | EBIT Margin | Asset T/O | Eq. Multiplier | ROE |")
                a("|------|----------:|----------------:|------------:|----------:|---------------:|----:|")
                for h in history:
                    a(f"| {h.get('year', '')} "
                      f"| {h.get('tax_burden', 0):.3f} "
                      f"| {h.get('interest_burden', 0):.3f} "
                      f"| {h.get('ebit_margin', 0):.3f} "
                      f"| {h.get('asset_turnover', 0):.3f} "
                      f"| {h.get('equity_multiplier', 0):.3f} "
                      f"| {h.get('roe', 0):.2f}% |")
                a("")

        # ── Tier 2: Altman Z-Score ───────────────────────────
        altman = analysis.get('altman_z', {})
        if altman.get('available'):
            a("## ⚠️ Altman Z-Score — Bankruptcy Risk Assessment\n")
            z_val = altman.get('z_score')
            zone = altman.get('zone', '')
            zone_icon = {'Safe': '🟢', 'Grey': '🟡', 'Distress': '🔴'}.get(zone, '⚪')
            a(f"**{zone_icon} Z-Score: {z_val:.2f}** — **{zone} Zone**\n")
            interp = altman.get('interpretation', '')
            if interp:
                a(f"> {interp}\n")

            components = altman.get('components', {})
            weighted = altman.get('weighted', {})
            if components:
                a("| Component | Raw Value | Weight | Weighted |")
                a("|-----------|----------:|-------:|---------:|")
                comp_labels = {
                    'wc_ta': ('Working Capital / Total Assets', 1.2),
                    're_ta': ('Retained Earnings / Total Assets', 1.4),
                    'ebit_ta': ('EBIT / Total Assets', 3.3),
                    'mcap_tl': ('Market Cap / Total Liabilities', 0.6),
                    'sales_ta': ('Sales / Total Assets', 1.0),
                }
                for key, (label, wt) in comp_labels.items():
                    raw = components.get(key)
                    w = weighted.get(key)
                    if raw is not None and w is not None:
                        a(f"| {label} | {raw:.4f} | {wt:.1f} | {w:.4f} |")
                a("")

            a("> 📌 *Z > 2.99 = Safe | 1.81 – 2.99 = Grey Zone | Z < 1.81 = Distress. "
              "Original Altman (1968) model for manufacturing firms; "
              "interpret with caution for financial-sector or asset-light companies.*\n")
        elif altman.get('sector_skip'):
            a("## ⚠️ Altman Z-Score — Bankruptcy Risk Assessment\n")
            a(f"> ℹ️ **Altman Z-Score Skipped** — {altman.get('reason', 'Not applicable for this sector.')}\n")
            a("> 💡 *For banks, NBFCs, and insurance companies, the Altman Z-Score "
              "is structurally inapplicable because deposits and float are "
              "operational liabilities, not financial distress indicators. "
              "Use CAMEL ratings, NPA ratios, or Capital Adequacy (CAR) "
              "for bank-specific risk assessment.*\n")

        # ── Tier 2: Working Capital Cycle Trend ──────────────
        wcc = analysis.get('wcc_trend', {})
        if wcc.get('available'):
            a("## 🔄 Working Capital Cycle — Multi-Year Trend\n")
            overall = wcc.get('overall', 'N/A')
            ov_icon = {'IMPROVING': '🟢', 'STABLE': '🟡',
                       'WORSENING': '🔴'}.get(overall, '⚪')
            a(f"**{ov_icon} Overall Trend: {overall}**\n")

            wcc_metrics = wcc.get('metrics', [])
            if wcc_metrics:
                a("| Metric | Latest (days) | Previous (days) | YoY Change | Trend |")
                a("|--------|-------------:|-----------------:|-----------:|:-----:|")
                for m in wcc_metrics:
                    latest = m.get('latest')
                    prev = m.get('previous')
                    yoy = m.get('yoy_change')
                    trend = m.get('trend', '')
                    t_icon = {'IMPROVING': '🟢', 'STABLE': '🟡',
                              'WORSENING': '🔴'}.get(trend, '⚪')
                    lat_s = f"{latest:.1f}" if latest is not None else 'N/A'
                    prv_s = f"{prev:.1f}" if prev is not None else 'N/A'
                    yoy_s = f"{yoy:+.1f}" if yoy is not None else 'N/A'
                    a(f"| {m.get('label', '')} | {lat_s} | {prv_s} | {yoy_s} | {t_icon} {trend} |")
                a("")

                # History sub-tables for each metric
                for m in wcc_metrics:
                    hist = m.get('history', [])
                    if hist and len(hist) > 2:
                        a(f"**{m.get('label', '')} — History:**\n")
                        # hist entries are (year, value) tuples
                        a("| " + " | ".join(str(h[0]) for h in hist) + " |")
                        a("| " + " | ".join("---:" for _ in hist) + " |")
                        a("| " + " | ".join(
                            f"{h[1]:.1f}" for h in hist) + " |")
                        a("")
        elif wcc.get('sector_skip'):
            a("## 🔄 Working Capital Cycle — Multi-Year Trend\n")
            a(f"> ℹ️ **Working Capital Cycle Skipped** — "
              f"{wcc.get('reason', 'Not applicable for this sector.')}\n")
            a("> 💡 *For banks, NBFCs, and insurance companies, traditional "
              "working capital metrics (Inventory Days, Debtor Days, "
              "Creditor Days, Cash Conversion Cycle) are not applicable. "
              "Use NPA ratios, CASA ratio, and Net Interest Margin (NIM) "
              "for operational efficiency assessment.*\n")

        # ── Tier 2: Historical Valuation Band ────────────────
        vband = analysis.get('valuation_band', {})
        if vband.get('available'):
            a("## 📊 Historical Valuation Band\n")

            pe_band = vband.get('pe_band', {})
            if pe_band:
                a("### P/E Valuation Band\n")
                a("| Statistic | Value |")
                a("|-----------|------:|")
                for stat_key, stat_label in [
                    ('min_pe', 'Minimum P/E'),
                    ('max_pe', 'Maximum P/E'),
                    ('median_pe', 'Median P/E'),
                    ('avg_pe', 'Average P/E'),
                    ('current_pe', 'Current P/E'),
                ]:
                    v = pe_band.get(stat_key)
                    if v is not None:
                        a(f"| {stat_label} | {v:.2f}x |")
                a("")

                pe_hist = pe_band.get('history', [])
                if pe_hist:
                    a("| Year | EPS | Price | P/E |")
                    a("|------|----:|------:|----:|")
                    for h in pe_hist:
                        a(f"| {h.get('year', '')} "
                          f"| ₹{h.get('eps', 0):.2f} "
                          f"| ₹{h.get('avg_price', 0):,.0f} "
                          f"| {h.get('pe', 0):.2f}x |")
                    a("")

            pb_band = vband.get('pb_band', {})
            if pb_band:
                a("### P/B Valuation Band\n")
                a("| Statistic | Value |")
                a("|-----------|------:|")
                for stat_key, stat_label in [
                    ('min_pb', 'Minimum P/B'),
                    ('max_pb', 'Maximum P/B'),
                    ('median_pb', 'Median P/B'),
                    ('avg_pb', 'Average P/B'),
                    ('current_pb', 'Current P/B'),
                ]:
                    v = pb_band.get(stat_key)
                    if v is not None:
                        a(f"| {stat_label} | {v:.2f}x |")
                a("")

                pb_hist = pb_band.get('history', [])
                if pb_hist:
                    a("| Year | BVPS | Price | P/B |")
                    a("|------|-----:|------:|----:|")
                    for h in pb_hist:
                        a(f"| {h.get('year', '')} "
                          f"| ₹{h.get('bvps', 0):.2f} "
                          f"| ₹{h.get('avg_price', 0):,.0f} "
                          f"| {h.get('pb', 0):.2f}x |")
                    a("")

            pe_pct = vband.get('pe_percentile')
            pe_zone = vband.get('pe_zone', '')
            if pe_pct is not None:
                zone_icon = {'UNDERVALUED': '🟢', 'FAIRLY_VALUED': '🟡',
                             'OVERVALUED': '🔴'}.get(pe_zone, '⚪')
                a(f"> {zone_icon} Current P/E is at the **{pe_pct:.0f}th percentile** "
                  f"of its historical range — **{pe_zone.replace('_', ' ')}**\n")

        # ── Tier 2: Quarterly Performance Matrix ─────────────
        qmat = analysis.get('qtr_matrix', {})
        if qmat.get('available'):
            a("## 📅 Quarterly Performance Matrix\n")

            quarters = qmat.get('quarters', [])
            if quarters:
                a("| Quarter | Revenue (Cr) | Net Profit (Cr) | OPM % "
                  "| Rev QoQ | Rev YoY | Profit QoQ | Profit YoY |")
                a("|---------|-------------:|----------------:|------:"
                  "|--------:|--------:|-----------:|-----------:|")
                for q in quarters:
                    rev = q.get('revenue')
                    np_ = q.get('net_profit')
                    opm = q.get('opm')
                    rqoq = q.get('revenue_qoq')
                    ryoy = q.get('revenue_yoy')
                    pqoq = q.get('profit_qoq')
                    pyoy = q.get('profit_yoy')
                    rev_s = f"₹{rev:,.0f}" if rev is not None else 'N/A'
                    np_s = f"₹{np_:,.0f}" if np_ is not None else 'N/A'
                    opm_s = f"{opm:.1f}%" if opm is not None else 'N/A'
                    rqoq_s = f"{rqoq:+.1f}%" if rqoq is not None else '—'
                    ryoy_s = f"{ryoy:+.1f}%" if ryoy is not None else '—'
                    pqoq_s = f"{pqoq:+.1f}%" if pqoq is not None else '—'
                    pyoy_s = f"{pyoy:+.1f}%" if pyoy is not None else '—'
                    a(f"| {q.get('quarter', '')} | {rev_s} | {np_s} | {opm_s} "
                      f"| {rqoq_s} | {ryoy_s} | {pqoq_s} | {pyoy_s} |")
                a("")

            rev_mom = qmat.get('revenue_momentum', '')
            margin_tr = qmat.get('margin_trend', '')
            if rev_mom:
                mom_icon = {'ACCELERATING': '🟢', 'DECELERATING': '🔴',
                            'STABLE': '🟡'}.get(rev_mom, '⚪')
                a(f"> {mom_icon} **Revenue Momentum:** {rev_mom}\n")
            if margin_tr:
                mtr_icon = {'EXPANDING': '🟢', 'CONTRACTING': '🔴',
                            'STABLE': '🟡'}.get(margin_tr, '⚪')
                a(f"> {mtr_icon} **Margin Trend:** {margin_tr}\n")

        # ── DCF Valuation ────────────────────────────────────
        a("## 💰 Valuation Analysis — DCF Model\n")
        if is_suspended:
            a("> ⚠️ **DCF BYPASSED** — Data Trust Score is below reliability "
              "threshold. DCF model generation is bypassed to prevent "
              "misleading valuation outputs.\n")
        elif dcf.get('sector_skip'):
            a(f"> ℹ️ **DCF Model Skipped** — {dcf.get('reason', 'Financial-sector company detected.')}\n")
            a("> 💡 *For banks, NBFCs, and insurance companies, standard "
              "FCFF/FCFE-based DCF is structurally inapplicable because "
              "deposits and float constitute operational liabilities, not "
              "financing. Use Price/Book, Residual Income (excess-ROE), "
              "or Dividend Discount Model (DDM) for intrinsic valuation.*\n")
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
            a(f"| 4a | - Net Debt | ₹{dcf['net_debt']:,.2f} Cr |")
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
                a("> 💡 *Analyst Note: While the DCF model may fail to "
                  "capture non-linear growth pivots (e.g. manufacturing "
                  "hyper-scaling), an extreme EV mismatch also signals "
                  "that the equity is priced for perfection with little "
                  "margin of safety. Investors should consider P/E, P/B, "
                  "and EV/EBITDA multiples relative to the sector median "
                  "to assess whether current premiums are justified.*\n")
                # Additional context: CapEx cycle and SOTP
                _sotp_avail = analysis.get('sotp', {}).get('available', False)
                if _sotp_avail:
                    a("> 💡 *For diversified conglomerates in a "
                      "peak capital-expenditure cycle, linear DCF "
                      "models systematically undervalue the business "
                      "because elevated CapEx depresses current Free "
                      "Cash Flow — the very baseline the algorithm "
                      "extrapolates. The market typically values such "
                      "companies using a Sum-of-the-Parts (SOTP) "
                      "methodology, pricing each vertical on future "
                      "earnings potential rather than present "
                      "depressed FCF. Refer to the SOTP section for "
                      "a more appropriate valuation framework.*\n")
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

            # Peak CapEx warning
            if dcf.get('peak_capex'):
                _cr = dcf.get('capex_ocf_ratio', 0)
                a(f"> ⚠️ **Peak CapEx Cycle Detected** — CapEx/OCF ratio "
                  f"at {_cr:.0%}, indicating the company is investing "
                  f"a disproportionate share of operating cash flow. "
                  f"The DCF baseline is structurally depressed; "
                  f"as capacity comes online and CapEx normalises, "
                  f"FCF should expand materially. Treat the current "
                  f"intrinsic value as a floor estimate.\n")

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

        # ── Price Target Reconciliation ──────────────────────
        recon = analysis.get('price_target_recon', {})
        if recon.get('available') and recon.get('methods'):
            a("## 🎯 Price Target Reconciliation\n")
            a("| Valuation Method | Fair Value | Upside/Downside |")
            a("|------------------|----------:|:---------:|")
            for m in recon['methods']:
                up = m.get('upside_pct', 0)
                icon = '🟢' if up > 10 else ('🟡' if up > -10 else '🔴')
                a(f"| {m['method']} | ₹{m['fair_value']:,.2f} | "
                  f"{icon} {up:+.1f}% |")
            a("")
            a(f"| **Consensus (Average)** | "
              f"**₹{recon['avg_fair_value']:,.2f}** | "
              f"**{recon['avg_upside_pct']:+.1f}%** |")
            a(f"| Range | ₹{recon['min_fair_value']:,.2f} — "
              f"₹{recon['max_fair_value']:,.2f} | — |")
            a("")
            if len(recon['methods']) >= 2:
                spread = recon['max_fair_value'] - recon['min_fair_value']
                avg = recon['avg_fair_value']
                if avg > 0:
                    spread_pct = round(spread / avg * 100, 1)
                    if spread_pct > 50:
                        a(f"> ⚠️ *High valuation spread ({spread_pct:.1f}%) — "
                          f"methods disagree significantly. Apply wider "
                          f"margin of safety.*\n")
                    elif spread_pct < 15:
                        a(f"> ✅ *Tight valuation convergence ({spread_pct:.1f}%) "
                          f"— methods broadly agree on fair value.*\n")
                    else:
                        a(f"> 📊 *Moderate valuation spread ({spread_pct:.1f}%) "
                          f"— consider the method most relevant to the "
                          f"company's stage and sector.*\n")
        elif recon.get('reason'):
            a("## 🎯 Price Target Reconciliation\n")
            a(f"> ⚠️ **Reconciliation Unavailable** — "
              f"{recon['reason']}\n")
            a("> 💡 *This may occur when DCF is skipped for financial-sector "
              "companies and peer comparable data is temporarily unavailable. "
              "Refer to the Historical Valuation Band section above for "
              "an alternative fair-value reference.*\n")

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
                _v_str = f"{v:.4f}" if isinstance(v, (int, float)) else 'N/A'
                a(f"| {k} | {_v_str} | {DESC.get(k, '')} |")
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

            # ── Institutional concentration analysis ─────────
            # Detect "smart money" vacuum and flag retail-heavy float
            _fii_cur = None
            _dii_cur = None
            _retail_cur = None
            for cat, vals in shp.items():
                if cat == 'PromoterPledging':
                    continue
                _c = vals.get('current')
                if not isinstance(_c, (int, float)):
                    continue
                _cat_lc = cat.lower()
                if 'fii' in _cat_lc or 'fpi' in _cat_lc:
                    _fii_cur = _c
                elif 'dii' in _cat_lc:
                    _dii_cur = _c
                elif 'public' in _cat_lc or 'retail' in _cat_lc:
                    _retail_cur = _c
            if (_fii_cur is not None and _dii_cur is not None
                    and _fii_cur + _dii_cur < 5
                    and _retail_cur is not None and _retail_cur > 30):
                a("> ⚠️ *Institutional Concentration Alert:* FII "
                  f"({_fii_cur:.1f}%) + DII ({_dii_cur:.1f}%) combined "
                  f"is under 5%, while retail float sits at "
                  f"{_retail_cur:.1f}%. This 'smart money' vacuum "
                  "increases susceptibility to high-beta volatility "
                  "during broader market corrections.\n")

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
                    a("> 💡 *Analyst Note: Pledged promoter shares "
                      "introduce asymmetric downside risk. In a severe "
                      "market correction, margin calls on pledged shares "
                      "can force involuntary liquidations, accelerating "
                      "downward price pressure in a self-reinforcing "
                      "cycle. Monitor pledge levels quarterly.*\n")
                a("")

        # ── Quarterly Shareholding Tracker ────────────────────
        qshp = analysis.get('quarterly_shareholding', {})
        if qshp.get('available') and qshp.get('flows'):
            a("## 📊 Institutional Flow Tracker (Quarterly SHP)\n")
            a("| Category | Latest (%) | QoQ Δ | Trend |")
            a("|----------|----------:|---------:|:-----:|")
            for cat, flow_data in qshp['flows'].items():
                cat_display = (cat.replace('Flls', 'FIIs')
                                  .replace('Dils', 'DIIs')
                                  .replace('FlIs', 'FIIs')
                                  .replace('DlIs', 'DIIs'))
                latest = flow_data.get('latest', 'N/A')
                qoq = flow_data.get('qoq_change', 0)
                trend = flow_data.get('trend', 'N/A')
                trend_icon = {'INCREASING': '🟢', 'DECREASING': '🔴',
                              'STABLE': '🟡'}.get(trend, '⚪')
                a(f"| {cat_display} | {latest} | {qoq:+.2f} | "
                  f"{trend_icon} {trend} |")
            a("")

            # QoQ detail table (if multiple quarters available)
            quarters = qshp.get('quarters', [])
            if len(quarters) >= 2:
                # Show last 4-6 quarters
                display_qtrs = quarters[-6:] if len(quarters) > 6 else quarters
                hdr = "| Category | " + " | ".join(str(q)[:7] for q in display_qtrs) + " |"
                sep = "|----------|" + "|".join("------:" for _ in display_qtrs) + "|"
                a("### Quarter-by-Quarter Breakdown\n")
                a(hdr)
                a(sep)
                for cat, flow_data in qshp['flows'].items():
                    cat_display = (cat.replace('Flls', 'FIIs')
                                      .replace('Dils', 'DIIs')
                                      .replace('FlIs', 'FIIs')
                                      .replace('DlIs', 'DIIs'))
                    vals = flow_data.get('values', [])
                    # Align to the displayed quarters
                    display_vals = vals[-len(display_qtrs):] if len(vals) >= len(display_qtrs) else vals
                    cells = [f"{v:.1f}" for v in display_vals]
                    a(f"| {cat_display} | " + " | ".join(cells) + " |")
                a("")

            # Smart money flow alert
            fii_flow = qshp['flows'].get('FIIs', {})
            dii_flow = qshp['flows'].get('DIIs', {})
            if fii_flow and dii_flow:
                fii_qoq = fii_flow.get('qoq_change', 0)
                dii_qoq = dii_flow.get('qoq_change', 0)
                if fii_qoq > 0.5 and dii_qoq > 0.5:
                    a("> 🟢 **Both FII and DII increasing stakes** — "
                      "strong institutional conviction.\n")
                elif fii_qoq < -0.5 and dii_qoq < -0.5:
                    a("> 🔴 **Both FII and DII reducing stakes** — "
                      "institutional exit signal.\n")
                elif fii_qoq > 0.5 and dii_qoq < -0.5:
                    a("> 🟡 **FII buying while DII selling** — "
                      "foreign capital inflow, domestic rotation out.\n")
                elif fii_qoq < -0.5 and dii_qoq > 0.5:
                    a("> 🟡 **DII buying while FII selling** — "
                      "domestic institutions absorbing FII selling.\n")

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
                # Analyst context: RPTs in multi-subsidiary groups
                # are often standard operational flows, not tunneling.
                rpt_pct = rpt.get('rpt_as_pct_revenue')
                if rpt_pct is not None and rpt_pct <= 25:
                    a("> 💡 *Analyst Note: RPTs at this level in "
                      "multi-subsidiary groups typically represent "
                      "standard inter-company service agreements "
                      "monitored by the Audit Committee on an "
                      "arm's-length basis, rather than wealth "
                      "tunneling.*\n")
                elif rpt_pct is not None and rpt_pct > 50:
                    # Detect conglomerate / holding-company structure:
                    # SOTP available OR multiple business segments.
                    _sotp_avail = analysis.get('sotp', {}).get('available', False)
                    _seg_count = len(analysis.get('sotp', {}).get(
                        'segment_valuations', []))
                    _segmental = analysis.get('segmental', {})
                    if not _seg_count:
                        _seg_count = len(_segmental.get('segments', []))
                    if _sotp_avail or _seg_count >= 3:
                        a("> 💡 *Analyst Note: For diversified holding "
                          "companies with multiple operating "
                          "subsidiaries, gross standalone intra-group "
                          "transactions (e.g. parent company "
                          "buying/selling to wholly-owned subs) "
                          "aggregate to seemingly large RPT figures. "
                          "In audited **consolidated** financials, "
                          "these inter-company transactions are "
                          "eliminated on consolidation and do not "
                          "represent wealth-tunneling. Evaluate "
                          "RPT quality by reviewing the Audit "
                          "Committee's arm's-length certification "
                          "in the Annual Report.*\n")

            # Contingent Liabilities
            if contingent.get('available'):
                a("### Contingent Liabilities\n")
                # ── Data-quality guard: if the extractor flagged the
                #    figure as implausible (e.g. >150 % of net worth),
                #    surface the warning rather than the raw number.
                if contingent.get('data_quality_issue'):
                    a("> ⚠️ **DATA QUALITY ISSUE** — The automated text "
                      "extractor returned an implausibly large contingent "
                      f"liability figure (₹{contingent.get('total_contingent', 0):,.0f} Cr). "
                      "This is almost certainly a parsing artefact. "
                      "Cross-check against audited filings before relying "
                      "on this figure.\n")
                else:
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
                a("> 💡 *Analyst Note: The Say-Do Ratio is a lagging "
                  "indicator that reflects historical promise fulfilment "
                  "across multiple years and leadership regimes. If the "
                  "company has undergone a recent strategic pivot or "
                  "management change, recent quarterly results may "
                  "materially outperform the historical track record. "
                  "Cross-check the latest 2-3 quarters before relying "
                  "solely on this metric.*\n")
                # NLP blindspot context for large/diversified companies
                _sdr_val = say_do.get('say_do_ratio')
                _sotp_sd = analysis.get('sotp', {}).get('available', False)
                _seg_sd = len(analysis.get('sotp', {}).get(
                    'segment_valuations', []))
                if not _seg_sd:
                    _seg_sd = len(analysis.get('segmental', {}).get(
                        'segments', []))
                if (_sdr_val is not None and _sdr_val < 0.15
                        and (_sotp_sd or _seg_sd >= 3)):
                    a("> ⚠️ *NLP Limitation: For large diversified "
                      "companies, the automated tracker captures "
                      "keyword-level short-term guidance (margin "
                      "targets, quarterly timelines) that may "
                      "fluctuate with macro volatility. It often "
                      "fails to credit successful multi-year "
                      "structural execution (e.g. new business "
                      "verticals reaching scale, capacity buildouts, "
                      "tariff hike pass-throughs). A near-zero score "
                      "for a company with demonstrable long-term "
                      "execution track record warrants manual "
                      "verification against actual delivered results.*\n")

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

            # Time-decay transparency
            if say_do.get('time_decay_applied'):
                _uw = say_do.get('unweighted_ratio')
                _uw_str = f'{_uw:.2f}' if _uw is not None else 'N/A'
                a(f"> 📐 *Time-Decay Applied (λ=0.5): recent quarters "
                  f"carry exponentially higher weight. "
                  f"Unweighted ratio: {_uw_str} → "
                  f"Weighted ratio: {f'{sd_ratio:.2f}' if sd_ratio is not None else 'N/A'}. "
                  f"This prevents legacy misses under prior management "
                  f"from permanently depressing the score.*\n")

            from config import config as _cfg_sd
            _sd_t = _cfg_sd.validation.say_do_threshold
            a(f"> 💡 *Say-Do Ratio > 1.0 means management over-delivers; "
              f"< {_sd_t} indicates persistent over-promising.*\n")

        # ── ESG / BRSR ──────────────────────────────────────
        esg = analysis.get('esg', {})
        if esg.get('available'):
            a("## 🌱 ESG / BRSR Intelligence\n")
            _esg_sc = esg.get('esg_score')
            a(f"**ESG Score: {_esg_sc if _esg_sc is not None else 'N/A'}/10** "
              f"| BRSR: {'✅ Found' if esg.get('brsr_found') else '❌ Not found'}\n")

            # Rule 6: Transition-phase modifier
            if esg.get('transition_phase'):
                _green_kw = esg.get('green_transition_keywords', [])
                a(f"> 🔄 **ESG Transition Phase** — {esg.get('transition_reason', 'Green transition detected.')}\n")
                if _green_kw:
                    a(f"> Green keywords detected: *{', '.join(_green_kw[:4])}*\n")
                a("> 💡 *The +1 score uplift has been applied to "
                  "reflect forward-looking decarbonisation intent. "
                  "Investors should track annual BRSR disclosures "
                  "for evidence of execution against these green "
                  "commitments.*\n")

            # Analyst context: very low ESG score
            if _esg_sc is not None and _esg_sc <= 2:
                a("> ⚠️ *Bottom-decile ESG score. Institutional mandates "
                  "(pension funds, sovereign wealth, ESG-screened ETFs) "
                  "increasingly require minimum sustainability thresholds. "
                  "A persistent low score may limit foreign institutional "
                  "inflows, raise the cost of capital, and trigger "
                  "exclusion from ESG-aligned indices.*\n")
            elif _esg_sc is not None and _esg_sc <= 4:
                # Check for green-transition indicators: carbon
                # targets, renewable energy %, or ESG improvement.
                _has_targets = bool(esg.get('carbon_targets'))
                _renew_pct = esg.get('metrics', {}).get(
                    'renewable_energy_pct')
                if _has_targets or (_renew_pct is not None and _renew_pct > 0):
                    a("> 💡 *Analyst Note: A low ESG score in a "
                      "company with stated carbon-reduction targets "
                      "or renewable energy investments often reflects "
                      "the legacy carbon-intensive footprint rather "
                      "than forward-looking intent. The automated "
                      "screener penalises historical emissions and "
                      "may not credit ongoing green-transition "
                      "capital expenditure (solar, battery, green H₂). "
                      "As transition capacity comes online, ESG "
                      "scores should improve structurally. Monitor "
                      "annual BRSR disclosures for year-on-year "
                      "trajectory rather than absolute level.*\n")
                else:
                    a("> ⚠️ *Low ESG score with no visible green-"
                      "transition roadmap. Institutional mandates "
                      "increasingly screen for minimum ESG thresholds; "
                      "sustained poor scores may limit foreign "
                      "institutional inflows and raise cost of capital.*\n")

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

        # ── (Qualitative RAG section removed — using document extraction only) ──

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
                    _r_lower = r.lower()
                    a(f"> ⚠️ {self._smart_truncate(r, 500)}\n")
                    # Analyst context: attrition risk
                    if 'attrition' in _r_lower:
                        a("> 💡 *Analyst Note: Elevated attrition is an "
                          "operational risk — it raises recruitment costs, "
                          "disrupts institutional knowledge continuity, "
                          "and can bottleneck growth execution.*\n")
                    # Analyst context: tariff / geopolitical exposure
                    if any(kw in _r_lower for kw in
                           ('tariff', 'geopolitical', 'trade polic',
                            'cross-border', 'sanction')):
                        a("> 💡 *Analyst Note: Geopolitical and trade-policy "
                          "exposure requires continuous monitoring — "
                          "even if management denies immediate impact, "
                          "regulatory shifts or sanctions can create "
                          "sudden cost or revenue headwinds.*\n")

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

            # Analyst note for bearish setups
            if signal in ('STRONG_BEARISH', 'MILDLY_BEARISH'):
                a("> 💡 *Analyst Note: Bearish technical signals "
                  "reflect genuine short-term price microstructure "
                  "and should be taken at face value for any "
                  "6-to-12-month investment horizon. However, "
                  "technicals are trailing indicators — they capture "
                  "current momentum, not future catalysts. If "
                  "fundamental re-rating triggers are imminent "
                  "(tariff hikes, new vertical revenue, margin "
                  "expansion), the technical setup can reverse "
                  "rapidly. Use this signal for entry timing, not "
                  "thesis invalidation.*\n")

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

            # Delivery Volume Analysis
            delivery = tech.get('delivery_analysis', {})
            if delivery.get('available'):
                a("### 📦 Delivery Volume Analysis\n")
                a("| Metric | Value |")
                a("|--------|------:|")
                if delivery.get('latest_delivery_pct') is not None:
                    a(f"| Latest Delivery % | {delivery['latest_delivery_pct']:.1f}% |")
                if delivery.get('avg_delivery_20d') is not None:
                    a(f"| 20-Day Avg Delivery % | {delivery['avg_delivery_20d']:.1f}% |")
                if delivery.get('avg_delivery_50d') is not None:
                    a(f"| 50-Day Avg Delivery % | {delivery['avg_delivery_50d']:.1f}% |")
                if delivery.get('avg_delivery_200d') is not None:
                    a(f"| 200-Day Avg Delivery % | {delivery['avg_delivery_200d']:.1f}% |")
                if delivery.get('delivery_trend'):
                    trend_icon = {'RISING': '🟢', 'FALLING': '🔴',
                                  'STABLE': '🟡'}.get(delivery['delivery_trend'], '⚪')
                    a(f"| Delivery Trend | {trend_icon} {delivery['delivery_trend']} |")
                if delivery.get('relative_delivery') is not None:
                    a(f"| Relative Delivery | {delivery['relative_delivery']:.2f}x |")
                a("")

                # Smart money signal
                smart_detail = delivery.get('smart_money_detail')
                if smart_detail:
                    a(f"> {smart_detail}\n")

                # Delivery spike
                if delivery.get('delivery_spike'):
                    a(f"> 🔥 {delivery.get('delivery_spike_detail', 'Delivery spike detected')}\n")

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
                if cl.get('data_quality_issue'):
                    a("- ⚠️ **DATA QUALITY:** Extracted figure is "
                      "implausibly large — likely a text-extraction "
                      "artefact. Verify against audited filings.")
                else:
                    sev = cl.get('severity', 'LOW')
                    sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠",
                                "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                    a(f"- {sev_icon} {cl.get('detail', 'N/A')}")
                a("")
        elif validation and validation.get('reason'):
            a("## 📋 Data Validation\n")
            a(f"> ⏭️ Validation skipped — {validation['reason']}\n")

        # ── Upcoming Results Calendar ─────────────────────────
        upcoming = analysis.get('upcoming_results', [])
        if upcoming:
            a("## 📅 Upcoming Results Calendar\n")
            a("| Detail | Value |")
            a("|--------|-------|")
            for entry in upcoming:
                # BSE Corpforthresults API returns:
                #   scrip_Code, short_name, Long_Name, meeting_date, URL
                if isinstance(entry, dict):
                    company = (entry.get('Long_Name')
                               or entry.get('short_name', ''))
                    meeting = (entry.get('meeting_date')
                               or entry.get('DT_TM', ''))
                    if company:
                        a(f"| Company | {company} |")
                    if meeting:
                        a(f"| Board Meeting Date | {meeting} |")
                    url = entry.get('URL', '')
                    if url:
                        a(f"| BSE Filing | [Link]({url}) |")
            a("")
            a("> 📌 *Dates sourced from BSE India filings. "
              "Subject to change per company announcements.*\n")
        else:
            a("## 📅 Upcoming Results Calendar\n")
            a("> ℹ️ No upcoming board meetings / results dates found in "
              "BSE India filings for this company at the time of report "
              "generation. This typically means the next result date has "
              "not yet been announced by the company.\n")

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

        if dcf.get('sector_skip'):
            pass  # DCF intentionally skipped for financial sector — not a risk
        elif dcf.get('available'):
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
                _sotp_avail_r = analysis.get('sotp', {}).get('available', False)
                if _sotp_avail_r:
                    risks.append(
                        f"🟡 **DCF Guardrail Triggered (SOTP Available)** — "
                        f"EV deviation {_ev_d}% exceeds {_ev_t:.0f}% threshold. "
                        f"DCF may undervalue peak-CapEx conglomerates; "
                        f"refer to SOTP valuation for a segment-level view.")
                else:
                    risks.append(
                        f"🔴 **DCF Guardrail Triggered** — "
                        f"EV deviation {_ev_d}% exceeds {_ev_t:.0f}% threshold; "
                        f"DCF target price suppressed. Current valuation "
                        f"premium leaves minimal margin of safety.")

        ic = ratios.get('interest_coverage')
        if ic is not None and isinstance(ic, (int, float)) and ic < 1:
            risks.append(f"🟡 **Low Interest Coverage** — {ic:.2f}x; "
                         "earnings do not cover interest expense.")

        # CFO/EBITDA red flag
        cfo = analysis.get('cfo_ebitda_check', {})
        if cfo.get('available') and cfo.get('is_red_flag'):
            risks.append(f"🔴 **Cash Flow Quality Concern** — CFO/EBITDA at "
                         f"{cfo.get('ratio', '?')}%; profits may not be cash-backed.")

        # Sentiment red flag (disabled — RAG/FinBERT removed)

        # Prediction red flag
        pred = analysis.get('prediction', {})
        if pred.get('available') and pred.get('trend') in ('BEARISH', 'MILDLY BEARISH'):
            risks.append(f"🟡 **Bearish Technical Signal** — "
                         f"30-day model: {pred.get('trend')} "
                         f"({pred.get('pct_change_30d', 0):+.1f}%)")

        # RPT red flag
        rpt = analysis.get('rpt', {})
        if rpt.get('available') and rpt.get('severity') in ('HIGH', 'CRITICAL'):
            _rpt_pct = rpt.get('rpt_as_pct_revenue')
            _sotp_avail = analysis.get('sotp', {}).get('available', False)
            _seg_n = len(analysis.get('sotp', {}).get(
                'segment_valuations', []))
            if not _seg_n:
                _seg_n = len(analysis.get('segmental', {}).get(
                    'segments', []))
            # For conglomerates, very high RPT% is typically intra-group
            if (_rpt_pct is not None and _rpt_pct > 50
                    and (_sotp_avail or _seg_n >= 3)):
                risks.append(
                    f"🟡 **Elevated RPT ({_rpt_pct:.0f}% of Revenue)** "
                    f"— Likely reflects intra-group accounting in a "
                    f"diversified holding structure ({_seg_n} segments). "
                    f"Consolidated accounts eliminate these flows; "
                    f"review Audit Committee RPT certification.")
            else:
                risks.append(f"🔴 **High Related Party Exposure** — {rpt.get('flag', '')}")

        # Contingent liabilities red flag
        contingent = analysis.get('contingent', {})
        if contingent.get('available'):
            if contingent.get('data_quality_issue'):
                risks.append(
                    "🟡 **Contingent Liabilities (Data Quality Issue)** — "
                    "Automated extraction returned an implausible figure; "
                    "cross-check against audited filings required.")
            elif contingent.get('severity') in ('HIGH', 'CRITICAL'):
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
                         f"Margin calls during corrections can force "
                         f"liquidations and accelerate price decline.")

        # Institutional exodus / retail-heavy float
        if isinstance(shp, dict):
            _fii_r = _dii_r = _ret_r = None
            for _cat, _v in shp.items():
                if _cat == 'PromoterPledging':
                    continue
                _cv = _v.get('current') if isinstance(_v, dict) else None
                if not isinstance(_cv, (int, float)):
                    continue
                _cl = _cat.lower()
                if 'fii' in _cl or 'fpi' in _cl:
                    _fii_r = _cv
                elif 'dii' in _cl:
                    _dii_r = _cv
                elif 'public' in _cl or 'retail' in _cl:
                    _ret_r = _cv
            if (_fii_r is not None and _dii_r is not None
                    and _fii_r + _dii_r < 5
                    and _ret_r is not None and _ret_r > 30):
                risks.append(
                    f"🟡 **Institutional Exodus** — FII ({_fii_r:.1f}%) "
                    f"+ DII ({_dii_r:.1f}%) < 5% combined; retail at "
                    f"{_ret_r:.1f}%. Retail-heavy float amplifies "
                    "technical volatility in corrections.")

        # ESG risk
        _esg_r = analysis.get('esg', {})
        if _esg_r.get('available'):
            _esg_sc_r = _esg_r.get('esg_score')
            if _esg_sc_r is not None and _esg_sc_r <= 3:
                _has_tgt = bool(_esg_r.get('carbon_targets'))
                _renew = _esg_r.get('metrics', {}).get('renewable_energy_pct')
                if _has_tgt or (_renew is not None and _renew > 0):
                    risks.append(
                        f"🟡 **Low ESG Score ({_esg_sc_r}/10)** — "
                        f"Legacy carbon footprint drives the score; "
                        f"green-transition CapEx is underway but not "
                        f"yet reflected in metrics.")
                else:
                    risks.append(
                        f"🔴 **Poor ESG Score ({_esg_sc_r}/10)** — "
                        f"No visible green-transition roadmap; may "
                        f"limit institutional inflows.")

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
        if trends.get('available'):
            if trends.get('overall_direction') == 'DETERIORATING':
                _th = trends.get('health_score')
                risks.append(f"🔴 **Deteriorating 5Y Trends** — "
                             f"Health score {_th if _th is not None else 'N/A'}/10")
            else:
                # Check metric-level deceleration
                _t_metrics = trends.get('metrics', [])
                _t_decel = [m for m in _t_metrics
                            if m.get('acceleration') == 'DECELERATING']
                _t_corp = [m for m in _t_metrics
                           if m.get('acceleration') == 'DECELERATING_CORP_ACTION']
                if _t_corp:
                    _ca_yr = trends.get('corp_action_year', '?')
                    risks.append(
                        f"🟡 **Deceleration (Corporate Action {_ca_yr})** — "
                        f"{len(_t_corp)} metrics show dilution-driven "
                        f"deceleration from stock split / bonus / merger; "
                        f"evaluate absolute (not per-share) growth.")
                if len(_t_decel) >= 3:
                    _rev_m_r = next(
                        (m for m in _t_metrics if m.get('label') == 'Revenue'),
                        {})
                    _rev_l_r = _rev_m_r.get('latest', 0)
                    if _rev_l_r > 50000:
                        risks.append(
                            f"🟡 **Growth Decelerating (Base Effect)** — "
                            f"{len(_t_decel)} metrics decelerating at "
                            f"₹{_rev_l_r:,.0f} Cr revenue scale; "
                            f"transition to steady-state compounder.")
                    else:
                        risks.append(
                            f"🟡 **Growth Decelerating** — "
                            f"{len(_t_decel)} of {len(_t_metrics)} "
                            f"metrics show decelerating momentum.")

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
            _sotp_sd_r = analysis.get('sotp', {}).get('available', False)
            _seg_sd_r = len(analysis.get('sotp', {}).get(
                'segment_valuations', []))
            if not _seg_sd_r:
                _seg_sd_r = len(analysis.get('segmental', {}).get(
                    'segments', []))
            if (_sdr is not None and _sdr < 0.15
                    and (_sotp_sd_r or _seg_sd_r >= 3)):
                risks.append(
                    f"🟡 **Management Credibility (NLP Caveat)** — "
                    f"Say-Do Ratio {_sdr:.2f} reflects keyword-level "
                    f"short-term tracking; large conglomerates' multi-year "
                    f"structural execution is poorly captured by automated "
                    f"NLP. Verify against actual delivered milestones.")
            else:
                risks.append(
                    f"🔴 **Management Credibility Risk** — "
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
