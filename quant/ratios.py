"""
Financial Ratio Computation
============================
Computes key ratios for the Financial Summary table:
  ROE, ROA, ROCE, PAT Margin, OPM, D/E, Interest Coverage,
  P/E, EPS, Dividend Yield, Revenue / Profit growth, CAGR.
"""
import numpy as np
import pandas as pd
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


class FinancialRatios:

    def calculate(self, data: dict) -> dict:
        pnl      = data.get('pnl', pd.DataFrame())
        bs       = data.get('balance_sheet', pd.DataFrame())
        cf       = data.get('cash_flow', pd.DataFrame())
        rat_df   = data.get('ratios', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        r = {}
        if pnl.empty:
            return r

        sales     = get_value(pp.get(pnl, 'sales'))
        net_prof  = get_value(pp.get(pnl, 'net_profit'))
        op_prof   = get_value(pp.get(pnl, 'operating_profit'))
        eps       = get_value(pp.get(pnl, 'eps'))
        interest  = get_value(pp.get(pnl, 'interest'))
        opm_dec   = get_value(pp.get(pnl, 'opm'))          # decimal (scraper divides by 100)
        div_dec   = get_value(pp.get(pnl, 'dividend_payout'))

        # ── Margin ratios ──
        r['pat_margin'] = self._pct(net_prof, sales)
        r['opm'] = (
            round(opm_dec * 100, 2)
            if not np.isnan(opm_dec)
            else self._pct(op_prof, sales)
        )
        r['eps'] = round(eps, 2) if not np.isnan(eps) else None

        # ── Balance-sheet ratios ──
        if not bs.empty:
            eq_cap   = self._z(get_value(pp.get(bs, 'equity_capital')))
            reserves = self._z(get_value(pp.get(bs, 'reserves')))
            equity   = eq_cap + reserves
            ta       = get_value(pp.get(bs, 'total_assets'))
            borr     = get_value(pp.get(bs, 'borrowings'))

            r['roe'] = self._pct(net_prof, equity)
            r['roa'] = self._pct(net_prof, ta)
            r['debt_to_equity'] = (
                round(self._z(borr) / equity, 2) if equity > 0 else None
            )
            r['interest_coverage'] = (
                round(op_prof / interest, 2)
                if (not np.isnan(interest) and interest > 0
                    and not np.isnan(op_prof))
                else ('∞' if not np.isnan(op_prof) and op_prof > 0 else None)
            )

            # ROCE from screener's own ratio table
            roce_dec = get_value(pp.get(rat_df, 'roce')) if not rat_df.empty else np.nan
            r['roce'] = round(roce_dec * 100, 2) if not np.isnan(roce_dec) else None

        # ── Growth metrics ──
        sales_s = pp.get(pnl, 'sales').dropna()
        np_s    = pp.get(pnl, 'net_profit').dropna()

        if len(sales_s) >= 2:
            r['revenue_growth'] = round(
                (sales_s.iloc[-1] / sales_s.iloc[-2] - 1) * 100, 2
            )
        if len(sales_s) >= 4:
            r['revenue_cagr_3y'] = round(
                ((sales_s.iloc[-1] / sales_s.iloc[-4]) ** (1/3) - 1) * 100, 2
            )
        if len(sales_s) >= 6:
            r['revenue_cagr_5y'] = round(
                ((sales_s.iloc[-1] / sales_s.iloc[-6]) ** (1/5) - 1) * 100, 2
            )
        if len(np_s) >= 2 and np_s.iloc[-2] > 0:
            r['profit_growth'] = round(
                (np_s.iloc[-1] / np_s.iloc[-2] - 1) * 100, 2
            )

        # ── Market-price dependent ──
        # Use TTM EPS for P/E to ensure temporal consistency
        # (Current Price / Trailing Twelve Month EPS)
        ttm_eps = data.get('ttm_eps')
        pe_eps = None  # The EPS actually used for P/E

        if ttm_eps is not None and ttm_eps > 0:
            pe_eps = ttm_eps
            r['ttm_eps'] = round(ttm_eps, 2)
        elif not np.isnan(eps) and eps > 0:
            pe_eps = eps  # Fall back to annual EPS only if TTM unavailable
        r['eps'] = round(eps, 2) if not np.isnan(eps) else None

        if not price_df.empty and pe_eps is not None and pe_eps > 0:
            cmp = (
                float(price_df['close'].iloc[-1])
                if 'close' in price_df.columns
                else float(price_df.iloc[-1, 0])
            )
            r['current_price'] = round(cmp, 2)
            r['pe_ratio']      = round(cmp / pe_eps, 2)
            r['pe_eps_used']   = 'TTM' if ttm_eps is not None and ttm_eps > 0 else 'Annual'

            # Dividend yield
            if not np.isnan(div_dec) and div_dec > 0:
                dps = eps * div_dec     # div_dec is already decimal
                r['dividend_yield'] = round(dps / cmp * 100, 2) if cmp > 0 else None

        # ── SEBI-Mandated Ratios (Reg 33) ──
        # Debtors Turnover = 365 / Debtor Days
        debtor_days = get_value(pp.get(rat_df, 'debtor_days')) if not rat_df.empty else np.nan
        if not np.isnan(debtor_days) and debtor_days > 0:
            r['debtor_days'] = round(debtor_days, 1)
            r['debtors_turnover'] = round(365 / debtor_days, 2)

        # Inventory Turnover = 365 / Inventory Days
        inv_days = get_value(pp.get(rat_df, 'inventory_days')) if not rat_df.empty else np.nan
        if not np.isnan(inv_days) and inv_days > 0:
            r['inventory_days'] = round(inv_days, 1)
            r['inventory_turnover'] = round(365 / inv_days, 2)

        # Cash Conversion Cycle
        ccc = get_value(pp.get(rat_df, 'cash_conversion')) if not rat_df.empty else np.nan
        if not np.isnan(ccc):
            r['cash_conversion_cycle'] = round(ccc, 1)

        # Current Ratio = Current Assets / Current Liabilities
        # Approximate: (Other Assets) / (Other Liabilities) from BS
        if not bs.empty:
            other_assets = self._z(get_value(pp.get(bs, 'other_assets')))
            other_liab = self._z(get_value(pp.get(bs, 'other_liabilities')))
            if other_liab > 0:
                r['current_ratio'] = round(other_assets / other_liab, 2)

        return r

    # helpers
    @staticmethod
    def _pct(num, den):
        """Return percentage rounded to 2 dp, or None."""
        if (num is None or den is None
                or (isinstance(num, float) and np.isnan(num))
                or (isinstance(den, float) and np.isnan(den))
                or den == 0):
            return None
        return round(num / den * 100, 2)

    @staticmethod
    def _z(v):
        return 0.0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
