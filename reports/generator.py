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
        if rating:
            a(f"## 🏷️ Rating: {rating.get('recommendation', 'N/A')}\n")
            if dcf.get('available'):
                a(f"| | |")
                a(f"|---|---|")
                a(f"| **Target Price (DCF)** | ₹{dcf['intrinsic_value']:,.2f} |")
                a(f"| **Current Price** | ₹{dcf['current_price']:,.2f} |")
                up = dcf.get('upside_pct')
                if up is not None:
                    a(f"| **Upside / Downside** | {up:+.1f} % |")
                a(f"| **Investment Horizon** | {rating.get('horizon', '12–18 months')} |")
            a("")

        # ── Investment Thesis ────────────────────────────────
        a("## 📌 Investment Thesis\n")
        for pt in rating.get('thesis', []):
            a(f"- {pt}")
        a("")

        # ── Financial Summary Table ──────────────────────────
        a("## 📋 Financial Summary\n")
        a("| Metric | Value |")
        a("|--------|------:|")
        METRICS = [
            ('Current Price',         'current_price',   '₹{:,.2f}'),
            ('P/E Ratio',             'pe_ratio',        '{:.2f}x'),
            ('EPS',                   'eps',             '₹{:.2f}'),
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
            direction = trends.get('overall_direction', 'STABLE')
            health = trends.get('health_score', 0)
            dir_icon = {'IMPROVING': '🟢', 'STABLE': '🟡',
                        'DETERIORATING': '🔴'}.get(direction, '⚪')
            a(f"**{dir_icon} Overall Direction: {direction}** "
              f"| Health Score: {health}/10\n")

            metrics = trends.get('metrics', [])
            if metrics:
                a("| Metric | Latest | Direction | 5Y CAGR | Acceleration |")
                a("|--------|-------:|:---------:|--------:|:------------:|")
                for m in metrics:
                    arrow = {'UP': '↑ 🟢', 'DOWN': '↓ 🔴',
                             'FLAT': '→ 🟡'}.get(m.get('direction', ''), '→')
                    cagr = f"{m['cagr_5y']:+.1f}%" if m.get('cagr_5y') is not None else 'N/A'
                    accel = m.get('acceleration', '—')
                    val = m.get('latest', 0)
                    if m.get('is_ratio'):
                        display = f"{val:.2f}%"  if abs(val) < 10 else f"{val:.1f}%"
                    else:
                        display = f"₹{val:,.0f} Cr" if val > 1 else f"{val:.2f}"
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
                              + " → ".join(f"{y}: ₹{v}" for y, v in
                                           zip(years, vals)))
                    a("")

                # Projections
                a("### Linear Projections\n")
                a("| Metric | Proj. Y+1 | Proj. Y+2 |")
                a("|--------|----------:|----------:|")
                for m in metrics[:6]:
                    p1 = m.get('projection_1y')
                    p2 = m.get('projection_2y')
                    if p1 is not None:
                        a(f"| {m['label']} | ₹{p1:,.0f} | ₹{p2:,.0f} |")
                a("")
                a("> ⚠️ *Linear projections — actual results depend on "
                  "market conditions, management execution, and macro factors.*\n")

        # ── DCF Valuation ────────────────────────────────────
        a("## 💰 Valuation Analysis — DCF Model\n")
        if dcf.get('available'):
            a("| Parameter | Value |")
            a("|-----------|------:|")
            a(f"| WACC | {dcf['wacc']} % |")
            a(f"| Growth Rate (initial) | {dcf['growth_rate']} % |")
            a(f"| Terminal Growth | {dcf['terminal_growth']} % |")
            a(f"| Latest FCF | ₹{dcf['latest_fcf']:,.2f} Cr |")
            a(f"| Enterprise Value | ₹{dcf['enterprise_value']:,.2f} Cr |")
            a(f"| Net Debt | ₹{dcf['net_debt']:,.2f} Cr |")
            a(f"| Equity Value | ₹{dcf['equity_value']:,.2f} Cr |")
            a(f"| Shares Outstanding | {dcf['shares_cr']:.2f} Cr |")
            a(f"| **Intrinsic Value / Share** | **₹{dcf['intrinsic_value']:,.2f}** |")
            a(f"| Current Market Price | ₹{dcf['current_price']:,.2f} |")
            up = dcf.get('upside_pct')
            if up is not None:
                icon = "🟢" if up > 10 else ("🟡" if up > -10 else "🔴")
                a(f"| Upside / Downside | {icon} {up:+.1f} % |")
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
            if sens.get('available'):
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
                    row = f"| **{w:.1f}%** | " + " | ".join(
                        f"₹{grid[i][j]:,.0f}" for j in range(len(tgr_range))
                    ) + " |"
                    a(row)
                a("")
        else:
            a(f"> ⚠️ DCF not available — {dcf.get('reason', 'unknown')}\n")

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
                cur = vals.get('current', 'N/A')
                prv = vals.get('previous', 'N/A')
                if isinstance(cur, (int, float)) and isinstance(prv, (int, float)):
                    delta = f"{cur - prv:+.2f}"
                else:
                    delta = "—"
                a(f"| {cat} | {cur} | {prv} | {delta} |")
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
            a(f"**Moat Score: {moat.get('moat_score', 0)}/10** "
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

        # ── ESG / BRSR ──────────────────────────────────────
        esg = analysis.get('esg', {})
        if esg.get('available'):
            a("## 🌱 ESG / BRSR Intelligence\n")
            a(f"**ESG Score: {esg.get('esg_score', 0)}/10** "
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
                    a("| Topic | Current Sent. | Prior Sent. | Δ | Signal |")
                    a("|-------|:------------:|:-----------:|--:|--------|")
                    for c in comps[:8]:
                        d = c.get('delta', {})
                        a(f"| {c.get('topic', '?')} | "
                          f"{c.get('current_sentiment', 0):.2f} | "
                          f"{c.get('prior_sentiment', 0):.2f} | "
                          f"{d.get('delta', 0):+.2f} | "
                          f"{d.get('severity', '?')} |")
                    a("")

            # Key Themes
            themes = qual.get('key_themes', {})
            if themes:
                a("### Key Themes from Transcripts\n")
                for query, snippets in themes.items():
                    a(f"**{query.title()}:**")
                    for s in snippets[:2]:
                        a(f"> {s[:200]}…\n")
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
                    a(f"> {s[:300]}\n")

            plans = text_intel.get('plans', [])
            if plans:
                a("### Plans & Strategy\n")
                for p in plans[:5]:
                    a(f"> {p[:300]}\n")

            risks = text_intel.get('risks', [])
            if risks:
                a("### Risk Signals (from text)\n")
                for r in risks[:5]:
                    a(f"> ⚠️ {r[:300]}\n")

            opps = text_intel.get('opportunities', [])
            if opps:
                a("### Opportunities\n")
                for o in opps[:5]:
                    a(f"> 🟢 {o[:300]}\n")

            # Forward-looking statements
            fwd = text_intel.get('forward_looking', [])
            if fwd:
                a("### Forward-Looking Statements\n")
                for f_stmt in fwd[:5]:
                    a(f"- {f_stmt[:350]}")
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
            a("## 📈 Price Forecast (30-Day ARIMA-ETS Ensemble)\n")
            a("| Metric | Value |")
            a("|--------|------:|")
            a(f"| Last Close | ₹{pred.get('last_price', 0):,.2f} |")
            a(f"| 30-Day Target | ₹{pred.get('end_price', 0):,.2f} |")
            a(f"| Expected Move | {pred.get('pct_change_30d', 0):+.1f}% |")
            a(f"| Trend Signal | **{pred.get('trend', 'N/A')}** |")
            a("")

            # Confidence interval endpoints
            ci_lo = pred.get('ci_lower', [])
            ci_hi = pred.get('ci_upper', [])
            if ci_lo and ci_hi:
                a(f"> 95% Confidence Band (Day 30): "
                  f"₹{ci_lo[-1]:,.2f} — ₹{ci_hi[-1]:,.2f}\n")

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
                icon = "🟢" if ts >= 80 else ("🟡" if ts >= 60 else ("🟠" if ts >= 40 else "🔴"))
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
                    status_icon = {"MATCH": "✅", "MISMATCH": "❌",
                                   "PARTIAL": "⚠️", "SKIPPED": "⏭️"
                                   }.get(chk.get('status', ''), '?')
                    scraper_val = chk.get('scraper_value', 'N/A')
                    ar_val = chk.get('ar_value', 'N/A')
                    if isinstance(scraper_val, float):
                        scraper_val = f"{scraper_val:,.2f}"
                    if isinstance(ar_val, float):
                        ar_val = f"{ar_val:,.2f}"
                    a(f"| {i} | {chk.get('metric', '?')} "
                      f"| {scraper_val} | {ar_val} | {status_icon} |")
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
                    a(f"| {sev_icon} {sev} | {fl.get('flag', '')} "
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
        if de is not None and isinstance(de, (int, float)) and de > 1.5:
            risks.append(f"🟡 **High Leverage** — D/E of {de:.2f} exceeds "
                         "comfortable range.")

        pg = ratios.get('profit_growth')
        if pg is not None and pg < -10:
            risks.append(f"🟡 **Declining Profits** — YoY profit growth {pg:+.1f} %.")

        pe = ratios.get('pe_ratio')
        if pe is not None and isinstance(pe, (int, float)) and pe > 50:
            risks.append(f"🟡 **Rich Valuation** — P/E {pe:.1f}x is well above "
                         "market average.")

        if dcf.get('available'):
            up = dcf.get('upside_pct')
            if up is not None and up < -20:
                risks.append(f"🔴 **Overvalued per DCF** — "
                             f"Stock appears {abs(up):.1f} % overvalued.")

        ic = ratios.get('interest_coverage')
        if ic is not None and isinstance(ic, (int, float)) and ic < 2:
            risks.append(f"🟡 **Low Interest Coverage** — {ic:.2f}x; "
                         "may struggle to service debt.")

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
        if governance.get('available') and governance.get('governance_score', 10) < 5:
            risks.append(f"🟡 **Governance Concerns** — "
                         f"Score {governance.get('governance_score')}/10")

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
            risks.append(f"🔴 **Deteriorating 5Y Trends** — "
                         f"Health score {trends.get('health_score', 0)}/10")

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
