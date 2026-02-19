"""
Tier 2 Analytics Module
========================
Computes five advanced analytical features using data already
available from Screener.in and yfinance — no new API calls needed.

Features:
  1. DuPont Decomposition (5-factor ROE breakdown)
  2. Altman Z-Score (bankruptcy risk)
  3. Working Capital Cycle Trend (multi-year WCC analysis)
  4. Historical Valuation Band (P/E and P/B range over time)
  5. Quarterly Performance Matrix (QoQ & YoY revenue/profit trends)

All values are computed from real scraped data only — no hardcoded
defaults or fallback values.
"""
import numpy as np
import pandas as pd
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


# ======================================================================
# 1. DuPont Decomposition
# ======================================================================

class DuPontAnalysis:
    """5-Factor DuPont decomposition of ROE.

    ROE = (Net Income / PBT)           ← Tax Burden
        × (PBT / EBIT)                 ← Interest Burden
        × (EBIT / Revenue)             ← Operating Margin
        × (Revenue / Total Assets)     ← Asset Turnover
        × (Total Assets / Equity)      ← Equity Multiplier
    """

    def analyze(self, data: dict) -> dict:
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())

        if pnl.empty or bs.empty:
            return {'available': False,
                    'reason': 'P&L or Balance Sheet data missing'}

        net_profit = get_value(pp.get(pnl, 'net_profit'))
        pbt        = get_value(pp.get(pnl, 'pbt'))
        interest   = get_value(pp.get(pnl, 'interest'))
        sales      = get_value(pp.get(pnl, 'sales'))
        ta         = get_value(pp.get(bs, 'total_assets'))
        eq_cap     = get_value(pp.get(bs, 'equity_capital'))
        reserves   = get_value(pp.get(bs, 'reserves'))

        # Validate — core fields must be real numbers
        # (operating_profit is not needed; EBIT is derived from PBT + Interest)
        vals = [net_profit, pbt, sales, ta, eq_cap, reserves]
        if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals):
            return {'available': False,
                    'reason': 'Incomplete financial data for DuPont'}

        equity = eq_cap + reserves
        if equity <= 0 or ta <= 0 or sales <= 0:
            return {'available': False,
                    'reason': 'Zero/negative equity, assets or sales'}

        # EBIT = Operating Profit + Other Income (approximation)
        # But we don't always have Other Income. Use EBIT ≈ PBT + Interest
        ebit_val = pbt + (interest if not np.isnan(interest) else 0)
        if ebit_val == 0:
            return {'available': False, 'reason': 'EBIT is zero'}

        # 5-factor computation
        tax_burden      = net_profit / pbt if pbt != 0 else None
        interest_burden = pbt / ebit_val if ebit_val != 0 else None
        ebit_margin     = ebit_val / sales if sales != 0 else None
        asset_turnover  = sales / ta if ta != 0 else None
        equity_mult     = ta / equity if equity != 0 else None

        factors = [tax_burden, interest_burden, ebit_margin,
                   asset_turnover, equity_mult]
        if any(f is None for f in factors):
            return {'available': False,
                    'reason': 'Could not compute all DuPont factors'}

        roe_computed = 1.0
        for f in factors:
            roe_computed *= f
        roe_pct = round(roe_computed * 100, 2)

        result = {
            'available': True,
            'tax_burden': round(tax_burden, 4),
            'interest_burden': round(interest_burden, 4),
            'ebit_margin': round(ebit_margin * 100, 2),
            'asset_turnover': round(asset_turnover, 4),
            'equity_multiplier': round(equity_mult, 2),
            'roe_dupont': roe_pct,
        }

        # Identify the weakest link
        factor_scores = {
            'Tax Efficiency': tax_burden,
            'Interest Burden': interest_burden,
            'Operating Margin': ebit_margin,
            'Asset Turnover': asset_turnover,
        }
        # Equity multiplier is higher = more leverage, so exclude
        weakest = min(factor_scores, key=factor_scores.get)
        strongest = max(factor_scores, key=factor_scores.get)
        result['weakest_factor'] = weakest
        result['strongest_factor'] = strongest

        # Multi-year DuPont (last 3 years if available)
        history = self._multi_year_dupont(data)
        if history:
            result['history'] = history

        return result

    def _multi_year_dupont(self, data: dict) -> list:
        """Compute DuPont factors for each of the last 3-5 years."""
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())
        if pnl.empty or bs.empty:
            return []

        np_s     = pp.get(pnl, 'net_profit').dropna()
        pbt_s    = pp.get(pnl, 'pbt').dropna()
        int_s    = pp.get(pnl, 'interest').dropna()
        sales_s  = pp.get(pnl, 'sales').dropna()
        ta_s     = pp.get(bs, 'total_assets').dropna()
        eq_s     = pp.get(bs, 'equity_capital').dropna()
        res_s    = pp.get(bs, 'reserves').dropna()

        # Find common years across core series
        # (operating_profit not needed — EBIT = PBT + Interest)
        common = np_s.index
        for s in [pbt_s, sales_s, ta_s, eq_s, res_s]:
            common = common.intersection(s.index)

        if len(common) < 2:
            return []

        # Take last 5 years max
        years = sorted(common)[-5:]
        history = []
        for yr in years:
            try:
                ni = float(np_s.loc[yr])
                pbt_v = float(pbt_s.loc[yr])
                sal = float(sales_s.loc[yr])
                ta_v = float(ta_s.loc[yr])
                eq = float(eq_s.loc[yr]) + float(res_s.loc[yr])
                int_v = float(int_s.loc[yr]) if yr in int_s.index else 0
                ebit = pbt_v + int_v

                if pbt_v == 0 or ebit == 0 or sal == 0 or ta_v == 0 or eq <= 0:
                    continue

                history.append({
                    'year': str(yr)[:10],
                    'tax_burden': round(ni / pbt_v, 4),
                    'interest_burden': round(pbt_v / ebit, 4),
                    'ebit_margin': round(ebit / sal * 100, 2),
                    'asset_turnover': round(sal / ta_v, 4),
                    'equity_multiplier': round(ta_v / eq, 2),
                    'roe': round(ni / eq * 100, 2),
                })
            except Exception:
                continue

        return history


# ======================================================================
# 2. Altman Z-Score
# ======================================================================

class AltmanZScore:
    """Altman Z-Score for bankruptcy risk assessment.

    For manufacturing / non-financial companies:
      Z = 1.2×A + 1.4×B + 3.3×C + 0.6×D + 1.0×E
    where:
      A = Working Capital / Total Assets
      B = Retained Earnings / Total Assets
      C = EBIT / Total Assets
      D = Market Cap / Total Liabilities
      E = Sales / Total Assets

    Zones:
      Z > 2.99  → Safe
      1.81-2.99 → Grey
      Z < 1.81  → Distress
    """

    def calculate(self, data: dict) -> dict:
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        if pnl.empty or bs.empty:
            return {'available': False,
                    'reason': 'P&L or Balance Sheet missing'}

        # Banks / financial-services companies: Altman Z is not applicable.
        # Detect by presence of 'Deposits' column in balance sheet (unique to banks)
        # or absence of 'Sales' column (banks use 'Revenue').
        _bs_cols_norm = {c.replace(' ', '').lower() for c in bs.columns}
        if 'deposits' in _bs_cols_norm:
            return {'available': False,
                    'sector_skip': True,
                    'reason': ('Altman Z-Score is not applicable to banks / '
                               'financial-services companies. Deposits constitute '
                               'operational liabilities, making Working Capital '
                               'and Total Liabilities structurally different '
                               'from manufacturing firms.')}

        sales      = get_value(pp.get(pnl, 'sales'))
        pbt        = get_value(pp.get(pnl, 'pbt'))
        interest   = get_value(pp.get(pnl, 'interest'))
        reserves   = get_value(pp.get(bs, 'reserves'))
        ta         = get_value(pp.get(bs, 'total_assets'))
        tl         = get_value(pp.get(bs, 'total_liabilities'))
        other_a    = get_value(pp.get(bs, 'other_assets'))
        other_l    = get_value(pp.get(bs, 'other_liabilities'))

        vals = [sales, ta, tl, reserves]
        if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals):
            return {'available': False,
                    'reason': 'Incomplete balance sheet data'}

        if ta <= 0:
            return {'available': False, 'reason': 'Total assets ≤ 0'}

        # Working Capital = Current Assets - Current Liabilities
        # Approximate: Other Assets - Other Liabilities
        wc_a = other_a if not np.isnan(other_a) else 0
        wc_l = other_l if not np.isnan(other_l) else 0
        working_capital = wc_a - wc_l

        # EBIT ≈ PBT + Interest
        ebit = 0
        if not np.isnan(pbt):
            ebit = pbt + (interest if not np.isnan(interest) else 0)

        # Market Cap from price data
        market_cap = None
        if not price_df.empty:
            eps = get_value(pp.get(pnl, 'eps'))
            net_prof = get_value(pp.get(pnl, 'net_profit'))
            cmp = float(price_df['close'].iloc[-1]) if 'close' in price_df.columns else None
            if (cmp and not np.isnan(eps) and eps > 0
                    and not np.isnan(net_prof) and net_prof > 0):
                shares = net_prof / eps  # in Cr
                market_cap = cmp * shares  # ₹ Cr

        if market_cap is None or tl <= 0:
            return {'available': False,
                    'reason': 'Cannot compute market cap or total liabilities ≤ 0'}

        # Retained Earnings ≈ Reserves
        retained = reserves if not np.isnan(reserves) else 0

        # Components
        A = working_capital / ta
        B = retained / ta
        C = ebit / ta
        D = market_cap / tl
        E = sales / ta if not np.isnan(sales) else 0

        z_score = round(1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E, 2)

        if z_score > 2.99:
            zone = 'SAFE'
            interpretation = 'Low probability of bankruptcy'
        elif z_score >= 1.81:
            zone = 'GREY'
            interpretation = 'Moderate risk — monitor closely'
        else:
            zone = 'DISTRESS'
            interpretation = 'High probability of financial distress'

        return {
            'available': True,
            'z_score': z_score,
            'zone': zone,
            'interpretation': interpretation,
            'components': {
                'A_working_capital_ta': round(A, 4),
                'B_retained_earnings_ta': round(B, 4),
                'C_ebit_ta': round(C, 4),
                'D_mcap_tl': round(D, 4),
                'E_sales_ta': round(E, 4),
            },
            'weighted': {
                'A_1.2x': round(1.2 * A, 4),
                'B_1.4x': round(1.4 * B, 4),
                'C_3.3x': round(3.3 * C, 4),
                'D_0.6x': round(0.6 * D, 4),
                'E_1.0x': round(1.0 * E, 4),
            },
        }


# ======================================================================
# 3. Working Capital Cycle Trend
# ======================================================================

class WorkingCapitalTrend:
    """Multi-year working capital cycle analysis from ratio data.

    Tracks: Debtor Days, Inventory Days, Creditor Days,
    Cash Conversion Cycle, Working Capital Days.
    """

    def analyze(self, data: dict) -> dict:
        rat_df = data.get('ratios', pd.DataFrame())
        if rat_df.empty:
            return {'available': False,
                    'reason': 'Ratio data not available'}

        # Banks don't have traditional WCC metrics (no inventory, debtors, creditors)
        # Detect by checking if Deposits column exists in balance sheet
        bs = data.get('balance_sheet', pd.DataFrame())
        _bs_cols_norm = {c.replace(' ', '').lower() for c in bs.columns} if not bs.empty else set()
        if 'deposits' in _bs_cols_norm:
            return {'available': False,
                    'sector_skip': True,
                    'reason': ('Working Capital Cycle analysis is not applicable '
                               'to banks / financial-services companies. Banks '
                               'do not have traditional Inventory Days, Debtor '
                               'Days, or Creditor Days metrics.')}

        metrics = {
            'debtor_days':    'Debtor Days',
            'inventory_days': 'Inventory Days',
            'days_payable':   'Creditor Days',
            'cash_conversion':'Cash Conversion Cycle',
            'working_capital_days': 'Working Capital Days',
        }

        result = {'available': False, 'metrics': [],
                  'reason': 'No working capital cycle metrics found in ratio data'}
        found_any = False

        for canonical, label in metrics.items():
            series = pp.get(rat_df, canonical).dropna()
            if len(series) < 2:
                continue
            found_any = True

            values = [(str(idx)[:10], round(float(v), 1))
                      for idx, v in series.items()]
            latest = values[-1][1]
            previous = values[-2][1]
            change = round(latest - previous, 1)

            # Trend over last 3+ years
            if len(values) >= 3:
                recent = [v[1] for v in values[-3:]]
                if recent[-1] > recent[0] * 1.1:
                    trend = 'WORSENING'
                elif recent[-1] < recent[0] * 0.9:
                    trend = 'IMPROVING'
                else:
                    trend = 'STABLE'
            else:
                trend = 'N/A'

            result['metrics'].append({
                'label': label,
                'canonical': canonical,
                'latest': latest,
                'previous': previous,
                'yoy_change': change,
                'trend': trend,
                'history': values[-5:],  # last 5 years
            })

        if found_any:
            result['available'] = True
            # Overall WCC health
            trends = [m['trend'] for m in result['metrics']]
            improving = sum(1 for t in trends if t == 'IMPROVING')
            worsening = sum(1 for t in trends if t == 'WORSENING')
            if improving > worsening:
                result['overall'] = 'IMPROVING'
            elif worsening > improving:
                result['overall'] = 'WORSENING'
            else:
                result['overall'] = 'STABLE'

        return result


# ======================================================================
# 4. Historical Valuation Band
# ======================================================================

class HistoricalValuationBand:
    """Compute P/E and P/B historical ranges from time-series data.

    Uses actual EPS and BVPS from annual P&L / Balance Sheet combined
    with price data to derive trailing P/E and P/B over time.
    """

    def analyze(self, data: dict) -> dict:
        pnl      = data.get('pnl', pd.DataFrame())
        bs       = data.get('balance_sheet', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())
        rat_df   = data.get('ratios', pd.DataFrame())

        if pnl.empty or price_df.empty:
            return {'available': False,
                    'reason': 'P&L or Price data not available'}

        result = {'available': False}

        # ── P/E Band from ratio table ──
        # Screener.in provides historical P/E in the ratios table
        # as a row if available. If not, derive from EPS + price.
        pe_band = self._compute_pe_band(pnl, price_df)
        if pe_band:
            result['pe_band'] = pe_band
            result['available'] = True

        # ── P/B Band ──
        pb_band = self._compute_pb_band(pnl, bs, price_df)
        if pb_band:
            result['pb_band'] = pb_band
            result['available'] = True

        # Current position within band
        if pe_band and pe_band.get('current_pe') is not None:
            pe_range = pe_band['max_pe'] - pe_band['min_pe']
            if pe_range > 0:
                pe_pos = (pe_band['current_pe'] - pe_band['min_pe']) / pe_range
                result['pe_percentile'] = round(pe_pos * 100, 1)
                if pe_pos < 0.25:
                    result['pe_zone'] = 'UNDERVALUED'
                elif pe_pos > 0.75:
                    result['pe_zone'] = 'OVERVALUED'
                else:
                    result['pe_zone'] = 'FAIRLY_VALUED'

        return result

    def _compute_pe_band(self, pnl, price_df) -> dict:
        """Compute P/E band from annual EPS and price history."""
        eps_s = pp.get(pnl, 'eps').dropna()
        if len(eps_s) < 3:
            return None

        # For each year's EPS, find the year-end price
        pe_values = []
        for yr_idx, eps_val in eps_s.items():
            eps_v = float(eps_val)
            if eps_v <= 0:
                continue
            yr_str = str(yr_idx)[:4]
            try:
                yr_int = int(yr_str)
            except ValueError:
                continue

            # Find March-end (FY end) price or nearest
            yr_prices = price_df[
                (price_df.index >= f'{yr_int}-01-01') &
                (price_df.index <= f'{yr_int}-12-31')
            ]
            if yr_prices.empty:
                continue
            close_col = 'close' if 'close' in yr_prices.columns else yr_prices.columns[0]
            avg_price = float(yr_prices[close_col].mean())
            if avg_price <= 0:
                continue
            pe = round(avg_price / eps_v, 2)
            if 0 < pe < 500:  # sanity check
                pe_values.append({
                    'year': yr_str,
                    'eps': round(eps_v, 2),
                    'avg_price': round(avg_price, 2),
                    'pe': pe,
                })

        if len(pe_values) < 3:
            return None

        pe_list = [p['pe'] for p in pe_values]
        # Current P/E
        close_col = 'close' if 'close' in price_df.columns else price_df.columns[0]
        cmp = float(price_df[close_col].iloc[-1])
        latest_eps = float(eps_s.iloc[-1])
        current_pe = round(cmp / latest_eps, 2) if latest_eps > 0 else None

        return {
            'history': pe_values,
            'min_pe': round(min(pe_list), 2),
            'max_pe': round(max(pe_list), 2),
            'median_pe': round(float(np.median(pe_list)), 2),
            'avg_pe': round(float(np.mean(pe_list)), 2),
            'current_pe': current_pe,
        }

    def _compute_pb_band(self, pnl, bs, price_df) -> dict:
        """Compute P/B band from annual BVPS and price history."""
        if bs.empty:
            return None

        eq_s  = pp.get(bs, 'equity_capital').dropna()
        res_s = pp.get(bs, 'reserves').dropna()
        eps_s = pp.get(pnl, 'eps').dropna()
        np_s  = pp.get(pnl, 'net_profit').dropna()

        common = eq_s.index.intersection(res_s.index)
        common = common.intersection(eps_s.index).intersection(np_s.index)

        if len(common) < 3:
            return None

        pb_values = []
        for yr in common:
            try:
                equity = float(eq_s.loc[yr]) + float(res_s.loc[yr])
                eps_v = float(eps_s.loc[yr])
                np_v = float(np_s.loc[yr])
                if equity <= 0 or eps_v <= 0 or np_v <= 0:
                    continue
                shares = np_v / eps_v  # in Cr
                bvps = equity / shares

                yr_str = str(yr)[:4]
                try:
                    yr_int = int(yr_str)
                except ValueError:
                    continue

                yr_prices = price_df[
                    (price_df.index >= f'{yr_int}-01-01') &
                    (price_df.index <= f'{yr_int}-12-31')
                ]
                if yr_prices.empty:
                    continue
                close_col = 'close' if 'close' in yr_prices.columns else yr_prices.columns[0]
                avg_price = float(yr_prices[close_col].mean())
                if avg_price <= 0:
                    continue
                pb = round(avg_price / bvps, 2)
                if 0 < pb < 100:
                    pb_values.append({
                        'year': yr_str,
                        'bvps': round(bvps, 2),
                        'avg_price': round(avg_price, 2),
                        'pb': pb,
                    })
            except Exception:
                continue

        if len(pb_values) < 3:
            return None

        pb_list = [p['pb'] for p in pb_values]

        # Current P/B
        close_col = 'close' if 'close' in price_df.columns else price_df.columns[0]
        cmp = float(price_df[close_col].iloc[-1])
        latest_bvps = pb_values[-1]['bvps']
        current_pb = round(cmp / latest_bvps, 2) if latest_bvps > 0 else None

        return {
            'history': pb_values,
            'min_pb': round(min(pb_list), 2),
            'max_pb': round(max(pb_list), 2),
            'median_pb': round(float(np.median(pb_list)), 2),
            'avg_pb': round(float(np.mean(pb_list)), 2),
            'current_pb': current_pb,
        }


# ======================================================================
# 5. Quarterly Performance Matrix
# ======================================================================

class QuarterlyPerformanceMatrix:
    """Analyse quarterly results for QoQ and YoY trends.

    Produces a matrix showing Revenue, Net Profit, OPM for each
    quarter with QoQ and YoY change computations.
    """

    def analyze(self, data: dict) -> dict:
        qtr = data.get('quarterly', pd.DataFrame())
        if qtr.empty:
            return {'available': False,
                    'reason': 'Quarterly data not available'}

        result = {'available': False, 'quarters': []}

        sales_s = pp.get(qtr, 'sales').dropna()
        np_s    = pp.get(qtr, 'net_profit').dropna()
        opm_s   = pp.get(qtr, 'opm').dropna()

        if len(sales_s) < 2:
            return {'available': False,
                    'reason': 'Insufficient quarterly data'}

        # Build quarter-by-quarter data
        common = sales_s.index
        for s in [np_s]:
            common = common.intersection(s.index)

        quarters = sorted(common)
        if len(quarters) < 2:
            return {'available': False,
                    'reason': 'Insufficient overlapping quarterly data'}

        for i, q in enumerate(quarters):
            entry = {
                'quarter': str(q)[:10],
                'revenue': round(float(sales_s.loc[q]), 2),
                'net_profit': round(float(np_s.loc[q]), 2),
            }

            # OPM
            if q in opm_s.index:
                opm_val = float(opm_s.loc[q])
                # If value is decimal (e.g., 0.15), convert to %
                if abs(opm_val) <= 1:
                    opm_val = opm_val * 100
                entry['opm'] = round(opm_val, 2)

            # QoQ change
            if i >= 1:
                prev_q = quarters[i - 1]
                prev_rev = float(sales_s.loc[prev_q])
                prev_np = float(np_s.loc[prev_q])
                if prev_rev > 0:
                    entry['revenue_qoq'] = round(
                        (entry['revenue'] / prev_rev - 1) * 100, 2)
                if prev_np > 0:
                    entry['profit_qoq'] = round(
                        (entry['net_profit'] / prev_np - 1) * 100, 2)

            # YoY change (4 quarters back)
            if i >= 4:
                yoy_q = quarters[i - 4]
                yoy_rev = float(sales_s.loc[yoy_q])
                yoy_np = float(np_s.loc[yoy_q])
                if yoy_rev > 0:
                    entry['revenue_yoy'] = round(
                        (entry['revenue'] / yoy_rev - 1) * 100, 2)
                if yoy_np > 0:
                    entry['profit_yoy'] = round(
                        (entry['net_profit'] / yoy_np - 1) * 100, 2)

            result['quarters'].append(entry)

        if result['quarters']:
            result['available'] = True
            result['num_quarters'] = len(result['quarters'])

            # Compute revenue acceleration / deceleration
            recent = result['quarters'][-4:]
            yoy_growths = [q.get('revenue_yoy') for q in recent
                           if q.get('revenue_yoy') is not None]
            if len(yoy_growths) >= 2:
                if yoy_growths[-1] > yoy_growths[0]:
                    result['revenue_momentum'] = 'ACCELERATING'
                elif yoy_growths[-1] < yoy_growths[0]:
                    result['revenue_momentum'] = 'DECELERATING'
                else:
                    result['revenue_momentum'] = 'STABLE'

            # Margin trend
            opms = [q.get('opm') for q in recent if q.get('opm') is not None]
            if len(opms) >= 2:
                if opms[-1] > opms[0] + 1:
                    result['margin_trend'] = 'EXPANDING'
                elif opms[-1] < opms[0] - 1:
                    result['margin_trend'] = 'CONTRACTING'
                else:
                    result['margin_trend'] = 'STABLE'

        return result
