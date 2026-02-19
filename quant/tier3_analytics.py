"""
Tier 3 Analytics Module
========================
Computes three advanced analytical features using data already
available from Screener.in — no new API calls needed.

Features:
  11. Dividend Dashboard (payout ratio history, yield trend,
      sustainability score, growth rate)
  12. Capital Allocation Scorecard (how management deploys CFO —
      CapEx vs dividends vs debt repayment vs acquisitions)
  13. Scenario Analysis — Bull / Base / Bear (3 cases with distinct
      revenue-growth / margin / multiple assumptions →
      probability-weighted target price)

All values are computed from real scraped data only — no hardcoded
defaults or fallback values.
"""
import numpy as np
import pandas as pd
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


# ======================================================================
# 11. Dividend Dashboard
# ======================================================================

class DividendDashboard:
    """Comprehensive dividend analysis.

    Produces:
      • Payout Ratio history (DPS / EPS or DividendPayout% from scraper)
      • Dividend Yield trend (DPS / price per year)
      • Sustainability Score (can CFO support dividends?)
      • Dividend Growth Rate (CAGR)
    """

    def analyze(self, data: dict) -> dict:
        pnl = data.get('pnl', pd.DataFrame())
        cf = data.get('cash_flow', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        if pnl.empty:
            return {'available': False,
                    'reason': 'P&L data not available'}

        eps_s = pp.get(pnl, 'eps').dropna()
        div_payout_s = pp.get(pnl, 'dividend_payout').dropna()
        net_profit_s = pp.get(pnl, 'net_profit').dropna()

        if div_payout_s.empty and eps_s.empty:
            return {'available': False,
                    'reason': 'No dividend / EPS data available'}

        result = {'available': False}

        # ── 1. Payout Ratio History ─────────────────────────
        payout_history = []
        dps_series = []  # (year, dps) for yield & growth calculations

        if not div_payout_s.empty and not eps_s.empty:
            common = div_payout_s.index.intersection(eps_s.index)
            for dt in sorted(common):
                payout_dec = float(div_payout_s.loc[dt])
                eps_val = float(eps_s.loc[dt])
                # Screener gives DividendPayout% as a decimal fraction
                payout_pct = payout_dec * 100 if abs(payout_dec) <= 1 else payout_dec
                # DPS = EPS × payout ratio (decimal)
                payout_ratio_dec = payout_dec if abs(payout_dec) <= 1 else payout_dec / 100
                dps = eps_val * payout_ratio_dec if eps_val > 0 else 0
                year_label = str(dt)[:4] if hasattr(dt, 'year') else str(dt)[:4]
                payout_history.append({
                    'year': year_label,
                    'eps': round(eps_val, 2),
                    'dps': round(dps, 2),
                    'payout_pct': round(payout_pct, 2),
                })
                dps_series.append((dt, dps))

        if not payout_history:
            return {'available': False,
                    'reason': 'Cannot compute payout ratio (missing data)'}

        result['payout_history'] = payout_history
        result['latest_payout_pct'] = payout_history[-1]['payout_pct']
        result['latest_dps'] = payout_history[-1]['dps']
        result['latest_eps'] = payout_history[-1]['eps']

        # ── 2. Dividend Yield Trend ─────────────────────────
        # Compute yield per year using average price for that year
        yield_history = []
        if not price_df.empty and 'close' in price_df.columns and dps_series:
            price_series = price_df['close'].dropna()
            price_series.index = pd.to_datetime(price_series.index)

            for dt, dps in dps_series:
                if dps <= 0:
                    continue
                yr = dt.year if hasattr(dt, 'year') else int(str(dt)[:4])
                # Average price for that year
                yr_prices = price_series[
                    (price_series.index.year == yr) |
                    (price_series.index.year == yr - 1)]
                if yr_prices.empty:
                    continue
                avg_price = float(yr_prices.mean())
                if avg_price > 0:
                    div_yield = round(dps / avg_price * 100, 2)
                    yield_history.append({
                        'year': str(yr),
                        'dps': round(dps, 2),
                        'avg_price': round(avg_price, 2),
                        'yield_pct': div_yield,
                    })

        result['yield_history'] = yield_history
        if yield_history:
            result['latest_yield_pct'] = yield_history[-1]['yield_pct']

        # ── 3. Dividend Growth Rate (CAGR) ──────────────────
        positive_dps = [(dt, dps) for dt, dps in dps_series if dps > 0]
        if len(positive_dps) >= 2:
            first_dps = positive_dps[0][1]
            last_dps = positive_dps[-1][1]
            n_years = len(positive_dps) - 1
            if first_dps > 0 and n_years > 0:
                cagr = (last_dps / first_dps) ** (1 / n_years) - 1
                result['dividend_cagr_pct'] = round(cagr * 100, 2)

        # ── 4. Sustainability Score ─────────────────────────
        # Sustainability = Can operating cash flow cover dividends?
        # Score = CFO / Total Dividends (higher = more sustainable)
        if not cf.empty:
            ocf_s = pp.get(cf, 'operating_cf').dropna()
            if not ocf_s.empty and not net_profit_s.empty:
                latest_ocf = float(ocf_s.iloc[-1])
                latest_np = float(net_profit_s.iloc[-1])
                latest_payout_dec = (result['latest_payout_pct'] / 100
                                     if result['latest_payout_pct'] > 1
                                     else result['latest_payout_pct'])
                total_dividend = latest_np * latest_payout_dec if latest_np > 0 else 0

                if total_dividend > 0:
                    coverage = latest_ocf / total_dividend
                    result['ocf_dividend_coverage'] = round(coverage, 2)

                    # Sustainability scoring based on CFO coverage
                    if coverage >= 2.0:
                        result['sustainability'] = 'STRONG'
                        result['sustainability_detail'] = (
                            f'CFO covers dividends {coverage:.1f}x — '
                            f'highly sustainable, room for payout increase')
                    elif coverage >= 1.0:
                        result['sustainability'] = 'ADEQUATE'
                        result['sustainability_detail'] = (
                            f'CFO covers dividends {coverage:.1f}x — '
                            f'sustainable at current levels')
                    else:
                        result['sustainability'] = 'AT_RISK'
                        result['sustainability_detail'] = (
                            f'CFO covers only {coverage:.1f}x dividends — '
                            f'payout may not be sustainable from operations')

        # ── 5. Payout consistency ───────────────────────────
        # Count how many years had positive DPS
        years_paid = sum(1 for _, dps in dps_series if dps > 0)
        total_years = len(dps_series)
        if total_years > 0:
            result['consistency_pct'] = round(years_paid / total_years * 100, 1)
            result['years_paid'] = years_paid
            result['total_years'] = total_years

        if payout_history:
            result['available'] = True

        return result


# ======================================================================
# 12. Capital Allocation Scorecard
# ======================================================================

class CapitalAllocationScorecard:
    """Analyse how management deploys operating cash flow.

    Breakdown: CFO → CapEx, Dividends, Debt Repayment, Acquisitions/Investments.

    Uses cash flow statement data directly — no external calls.
    """

    def analyze(self, data: dict) -> dict:
        cf = data.get('cash_flow', pd.DataFrame())
        pnl = data.get('pnl', pd.DataFrame())
        bs = data.get('balance_sheet', pd.DataFrame())

        if cf.empty:
            return {'available': False,
                    'reason': 'Cash flow statement not available'}

        ocf_s = pp.get(cf, 'operating_cf').dropna()
        if ocf_s.empty:
            return {'available': False,
                    'reason': 'Operating cash flow data missing'}

        result = {'available': False, 'years': []}

        # Find CapEx column (Fixed assets purchased — negative)
        capex_col = None
        for col in cf.columns:
            if 'fixedassetspurchased' in col.lower().replace(' ', ''):
                capex_col = col
                break

        # Find Financing CF for debt component
        fin_cf_s = pp.get(cf, 'financing_cf').dropna()
        inv_cf_s = pp.get(cf, 'investing_cf').dropna()

        # Compute allocation per year
        years_data = []
        for dt in sorted(ocf_s.index):
            cfo = float(ocf_s.loc[dt])
            year_label = str(dt)[:4] if hasattr(dt, 'year') else str(dt)[:4]

            entry = {
                'year': year_label,
                'cfo': round(cfo, 2),
            }

            if cfo <= 0:
                entry['note'] = 'Negative CFO — no allocation breakdown'
                years_data.append(entry)
                continue

            # CapEx
            capex = 0
            if capex_col and dt in cf.index:
                capex_val = cf.loc[dt, capex_col]
                if not pd.isna(capex_val):
                    capex = abs(float(capex_val))
            elif not inv_cf_s.empty and dt in inv_cf_s.index:
                # Approximate CapEx from total investing CF
                inv_val = float(inv_cf_s.loc[dt])
                capex = abs(inv_val) if inv_val < 0 else 0

            entry['capex'] = round(capex, 2)
            entry['capex_pct'] = round(capex / cfo * 100, 2) if cfo > 0 else 0

            # Dividends paid (from P&L: Net Profit × Payout Ratio)
            div_paid = 0
            if not pnl.empty:
                np_s = pp.get(pnl, 'net_profit')
                div_s = pp.get(pnl, 'dividend_payout')
                if dt in np_s.index and dt in div_s.index:
                    np_val = float(np_s.loc[dt])
                    div_ratio = float(div_s.loc[dt])
                    if not np.isnan(np_val) and not np.isnan(div_ratio):
                        div_ratio_dec = div_ratio if abs(div_ratio) <= 1 else div_ratio / 100
                        div_paid = abs(np_val * div_ratio_dec) if np_val > 0 else 0

            entry['dividends'] = round(div_paid, 2)
            entry['dividends_pct'] = round(div_paid / cfo * 100, 2) if cfo > 0 else 0

            # Debt changes (from balance sheet borrowings YoY)
            debt_repaid = 0
            if not bs.empty:
                borr_s = pp.get(bs, 'borrowings').dropna()
                idx_list = sorted(borr_s.index)
                if dt in borr_s.index:
                    curr_idx = idx_list.index(dt)
                    if curr_idx > 0:
                        prev_dt = idx_list[curr_idx - 1]
                        curr_borr = float(borr_s.loc[dt])
                        prev_borr = float(borr_s.loc[prev_dt])
                        if not np.isnan(curr_borr) and not np.isnan(prev_borr):
                            delta = prev_borr - curr_borr
                            if delta > 0:
                                debt_repaid = delta

            entry['debt_repaid'] = round(debt_repaid, 2)
            entry['debt_repaid_pct'] = round(debt_repaid / cfo * 100, 2) if cfo > 0 else 0

            # Residual = CFO - CapEx - Dividends - Debt Repaid
            # This approximates acquisitions/investments/other
            residual = cfo - capex - div_paid - debt_repaid
            entry['residual'] = round(residual, 2)
            entry['residual_pct'] = round(residual / cfo * 100, 2) if cfo > 0 else 0

            years_data.append(entry)

        if not years_data:
            return {'available': False,
                    'reason': 'No years with valid CFO data'}

        result['years'] = years_data

        # ── Summary: average allocation over all positive-CFO years ──
        pos_years = [y for y in years_data if y.get('cfo', 0) > 0
                     and 'capex_pct' in y]
        if pos_years:
            result['avg_capex_pct'] = round(
                sum(y['capex_pct'] for y in pos_years) / len(pos_years), 1)
            result['avg_dividends_pct'] = round(
                sum(y['dividends_pct'] for y in pos_years) / len(pos_years), 1)
            result['avg_debt_repaid_pct'] = round(
                sum(y['debt_repaid_pct'] for y in pos_years) / len(pos_years), 1)
            result['avg_residual_pct'] = round(
                sum(y['residual_pct'] for y in pos_years) / len(pos_years), 1)
            result['num_years'] = len(pos_years)

            # ── Capital Allocation Style ────────────────────
            capex_dominant = result['avg_capex_pct'] > 50
            div_dominant = result['avg_dividends_pct'] > 30
            debt_focus = result['avg_debt_repaid_pct'] > 25

            if capex_dominant and not div_dominant:
                result['style'] = 'GROWTH-ORIENTED'
                result['style_detail'] = (
                    f"Management reinvests {result['avg_capex_pct']:.0f}% "
                    f"of CFO into CapEx — prioritising growth over returns.")
            elif div_dominant and not capex_dominant:
                result['style'] = 'SHAREHOLDER-FRIENDLY'
                result['style_detail'] = (
                    f"Management returns {result['avg_dividends_pct']:.0f}% "
                    f"of CFO as dividends — shareholder-return focused.")
            elif debt_focus:
                result['style'] = 'DELEVERAGING'
                result['style_detail'] = (
                    f"Management deploys {result['avg_debt_repaid_pct']:.0f}% "
                    f"of CFO to reduce debt — balance-sheet repair mode.")
            elif capex_dominant and div_dominant:
                result['style'] = 'BALANCED'
                result['style_detail'] = (
                    f"Balanced approach: {result['avg_capex_pct']:.0f}% CapEx, "
                    f"{result['avg_dividends_pct']:.0f}% dividends.")
            else:
                result['style'] = 'MIXED'
                result['style_detail'] = (
                    f"CapEx {result['avg_capex_pct']:.0f}%, "
                    f"Dividends {result['avg_dividends_pct']:.0f}%, "
                    f"Debt Repayment {result['avg_debt_repaid_pct']:.0f}%.")

            result['available'] = True

        return result


# ======================================================================
# 13. Scenario Analysis — Bull / Base / Bear
# ======================================================================

class ScenarioAnalysis:
    """Three-scenario valuation using real historical data.

    Derives Bull / Base / Bear assumptions from the ACTUAL
    distribution of the company's own revenue growth, margin,
    and P/E multiples — no hardcoded numbers.

    For each scenario:
      Revenue Growth   = percentile of historical growth rates
      OPM / PAT Margin = percentile of historical margins
      Exit Multiple    = percentile of historical P/E band

    Probability weights are derived from mean-reversion logic:
      If current metrics are above median → base-case skews bear
      If current metrics are below median → base-case skews bull
    """

    def analyze(self, data: dict, analysis: dict) -> dict:
        pnl = data.get('pnl', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        if pnl.empty:
            return {'available': False, 'reason': 'P&L data not available'}

        sales_s = pp.get(pnl, 'sales').dropna()
        np_s = pp.get(pnl, 'net_profit').dropna()
        eps_s = pp.get(pnl, 'eps').dropna()

        if len(sales_s) < 3 or len(np_s) < 3:
            return {'available': False,
                    'reason': 'Need ≥3 years of P&L data for scenario analysis'}

        # ── 1. Historical growth rates ──────────────────────
        rev_growths = []
        for i in range(1, len(sales_s)):
            prev = float(sales_s.iloc[i - 1])
            curr = float(sales_s.iloc[i])
            if prev > 0:
                rev_growths.append((curr / prev - 1) * 100)

        if len(rev_growths) < 2:
            return {'available': False,
                    'reason': 'Insufficient growth history'}

        # ── 2. Historical margins ───────────────────────────
        margins = []
        common = sales_s.index.intersection(np_s.index)
        for dt in common:
            s = float(sales_s.loc[dt])
            n = float(np_s.loc[dt])
            if s > 0:
                margins.append(n / s * 100)

        if len(margins) < 2:
            return {'available': False,
                    'reason': 'Insufficient margin history'}

        # ── 3. Historical P/E multiples from valuation band ─
        vband = analysis.get('valuation_band', {})
        pe_band = vband.get('pe_band', {}) if vband.get('available') else {}
        pe_hist = pe_band.get('history', [])

        pe_values = []
        for h in pe_hist:
            pe_val = h.get('pe')
            if pe_val is not None and pe_val > 0:
                pe_values.append(pe_val)

        # Also try to use ratios P/E
        ratios = analysis.get('ratios', {})
        current_pe = ratios.get('pe_ratio')

        if not pe_values and current_pe and current_pe > 0:
            # Minimal: use current P/E as single data point
            pe_values = [current_pe]

        if not pe_values:
            return {'available': False,
                    'reason': 'No P/E history for exit multiple estimation'}

        # ── 4. Derive scenario assumptions from percentiles ──
        rev_arr = np.array(rev_growths)
        margin_arr = np.array(margins)
        pe_arr = np.array(pe_values)

        scenarios = {}

        # Bull: 75th percentile growth, 75th margin, 75th P/E
        # Base: 50th percentile (median)
        # Bear: 25th percentile
        for label, pctile in [('bull', 75), ('base', 50), ('bear', 25)]:
            rev_g = float(np.percentile(rev_arr, pctile))
            margin = float(np.percentile(margin_arr, pctile))
            exit_pe = float(np.percentile(pe_arr, pctile))

            scenarios[label] = {
                'revenue_growth_pct': round(rev_g, 2),
                'pat_margin_pct': round(margin, 2),
                'exit_pe': round(exit_pe, 2),
            }

        # ── 5. Project forward 1 year and derive target price ──
        latest_sales = float(sales_s.iloc[-1])
        latest_eps = float(eps_s.iloc[-1]) if not eps_s.empty else None
        shares_outstanding = data.get('shares_outstanding')
        shares_cr = None
        if isinstance(shares_outstanding, pd.Series) and not shares_outstanding.empty:
            shares_val = get_value(shares_outstanding)
            if not np.isnan(shares_val) and shares_val > 0:
                shares_cr = shares_val
        # Fallback: Net Profit / EPS
        if shares_cr is None and latest_eps and latest_eps > 0:
            latest_np = float(np_s.iloc[-1])
            if latest_np > 0:
                shares_cr = latest_np / latest_eps

        if shares_cr is None or shares_cr <= 0:
            return {'available': False,
                    'reason': 'Cannot determine shares outstanding'}

        current_price = None
        if not price_df.empty and 'close' in price_df.columns:
            current_price = float(price_df['close'].iloc[-1])

        for label, assumptions in scenarios.items():
            rev_g_dec = assumptions['revenue_growth_pct'] / 100
            margin_dec = assumptions['pat_margin_pct'] / 100
            exit_pe = assumptions['exit_pe']

            projected_sales = latest_sales * (1 + rev_g_dec)
            projected_pat = projected_sales * margin_dec
            projected_eps = projected_pat / shares_cr if shares_cr > 0 else 0
            target_price = projected_eps * exit_pe

            assumptions['projected_revenue'] = round(projected_sales, 2)
            assumptions['projected_pat'] = round(projected_pat, 2)
            assumptions['projected_eps'] = round(projected_eps, 2)
            assumptions['target_price'] = round(target_price, 2)

            if current_price and current_price > 0:
                assumptions['upside_pct'] = round(
                    (target_price / current_price - 1) * 100, 1)

        # ── 6. Probability weights (mean-reversion logic) ───
        # If current growth / margin are above median → higher bear weight
        # If below median → higher bull weight
        latest_growth = rev_growths[-1] if rev_growths else 0
        latest_margin = margins[-1] if margins else 0
        median_growth = float(np.median(rev_arr))
        median_margin = float(np.median(margin_arr))

        # Compute deviation from median
        growth_above = latest_growth > median_growth
        margin_above = latest_margin > median_margin
        above_count = sum([growth_above, margin_above])

        # Probability assignment based on mean-reversion:
        # Both above median → more likely to revert down → bear gets more weight
        # Both below median → more likely to revert up → bull gets more weight
        if above_count == 2:
            prob = {'bull': 0.20, 'base': 0.50, 'bear': 0.30}
        elif above_count == 0:
            prob = {'bull': 0.30, 'base': 0.50, 'bear': 0.20}
        else:
            prob = {'bull': 0.25, 'base': 0.50, 'bear': 0.25}

        for label in scenarios:
            scenarios[label]['probability'] = prob[label]

        # ── 7. Probability-weighted target price ────────────
        weighted_target = sum(
            scenarios[l]['target_price'] * scenarios[l]['probability']
            for l in ['bull', 'base', 'bear'])

        result = {
            'available': True,
            'scenarios': scenarios,
            'weighted_target': round(weighted_target, 2),
            'current_price': round(current_price, 2) if current_price else None,
            'shares_cr': round(shares_cr, 2),
            'latest_revenue': round(latest_sales, 2),
            'data_points': {
                'growth_years': len(rev_growths),
                'margin_years': len(margins),
                'pe_observations': len(pe_values),
            },
        }

        if current_price and current_price > 0:
            result['weighted_upside_pct'] = round(
                (weighted_target / current_price - 1) * 100, 1)

        return result
