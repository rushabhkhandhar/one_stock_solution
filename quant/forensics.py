"""
Beneish M-Score — Earnings Manipulation Detection
==================================================

Formula
-------
  M = −4.84
    + 0.920 × DSRI   (Days Sales in Receivables Index)
    + 0.528 × GMI    (Gross Margin Index)
    + 0.404 × AQI    (Asset Quality Index)
    + 0.892 × SGI    (Sales Growth Index)
    + 0.115 × DEPI   (Depreciation Index)
    − 0.172 × SGAI   (SGA Expense Index)
    + 4.679 × TATA   (Total Accruals / Total Assets)
    − 0.327 × LVGI   (Leverage Index)

Interpretation
--------------
  M > −1.78  →  Likely manipulator
  M < −2.22  →  Unlikely manipulator
"""
import numpy as np
import pandas as pd
from config import config
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


class BeneishMScore:

    COEFF = {
        'intercept': -4.84,
        'DSRI':  0.920,
        'GMI':   0.528,
        'AQI':   0.404,
        'SGI':   0.892,
        'DEPI':  0.115,
        'SGAI': -0.172,
        'TATA':  4.679,
        'LVGI': -0.327,
    }

    # ==================================================================
    def calculate(self, data: dict) -> dict:
        pnl    = data.get('pnl', pd.DataFrame())
        bs     = data.get('balance_sheet', pd.DataFrame())
        cf     = data.get('cash_flow', pd.DataFrame())
        ratios = data.get('ratios', pd.DataFrame())

        result = {'available': False, 'reason': ''}

        if pnl.empty or bs.empty or len(pnl) < 2:
            result['reason'] = 'Need ≥ 2 years of P&L + Balance-Sheet data'
            return result

        try:
            # ── Pull current (t) and prior-year (t-1) values ──
            def v(df, name, idx=-1):
                return get_value(pp.get(df, name), idx)

            sales_t,  sales_t1  = v(pnl, 'sales', -1), v(pnl, 'sales', -2)
            exp_t,    exp_t1    = v(pnl, 'expenses', -1), v(pnl, 'expenses', -2)
            np_t                = v(pnl, 'net_profit', -1)
            dep_t,    dep_t1    = v(pnl, 'depreciation', -1), v(pnl, 'depreciation', -2)

            ta_t,  ta_t1  = v(bs, 'total_assets', -1), v(bs, 'total_assets', -2)
            fa_t,  fa_t1  = v(bs, 'fixed_assets', -1), v(bs, 'fixed_assets', -2)
            oa_t,  oa_t1  = v(bs, 'other_assets', -1), v(bs, 'other_assets', -2)
            borr_t, borr_t1 = v(bs, 'borrowings', -1), v(bs, 'borrowings', -2)
            ol_t,  ol_t1  = v(bs, 'other_liabilities', -1), v(bs, 'other_liabilities', -2)
            cwip_t, cwip_t1 = v(bs, 'cwip', -1), v(bs, 'cwip', -2)
            inv_t,  inv_t1  = v(bs, 'investments', -1), v(bs, 'investments', -2)

            cfo_t = v(cf, 'operating_cf', -1)

            dd_t  = v(ratios, 'debtor_days', -1)
            dd_t1 = v(ratios, 'debtor_days', -2)

            # Derived: Receivables ≈ Debtor Days × Sales / 365
            rec_t  = dd_t * sales_t / 365 if self._ok(dd_t, sales_t) else np.nan
            rec_t1 = dd_t1 * sales_t1 / 365 if self._ok(dd_t1, sales_t1) else np.nan

            # Current Assets ≈ Total − Fixed − CWIP − Investments
            ca_t  = ta_t - self._z(fa_t) - self._z(cwip_t) - self._z(inv_t)
            ca_t1 = ta_t1 - self._z(fa_t1) - self._z(cwip_t1) - self._z(inv_t1)

            comp = {}

            # 1. DSRI ─────────────────────────────────────────
            if self._ok(rec_t, rec_t1, sales_t, sales_t1):
                comp['DSRI'] = (rec_t / sales_t) / (rec_t1 / sales_t1)
            else:
                comp['DSRI'] = 1.0

            # 2. GMI ──────────────────────────────────────────
            gm_t  = (sales_t - exp_t) / sales_t   if self._nz(sales_t)  else 0
            gm_t1 = (sales_t1 - exp_t1) / sales_t1 if self._nz(sales_t1) else 0
            comp['GMI'] = gm_t1 / gm_t if self._nz(gm_t) else 1.0

            # 3. AQI ──────────────────────────────────────────
            aq_t  = 1 - (ca_t + self._z(fa_t)) / ta_t   if self._nz(ta_t)  else 0
            aq_t1 = 1 - (ca_t1 + self._z(fa_t1)) / ta_t1 if self._nz(ta_t1) else 0
            comp['AQI'] = aq_t / aq_t1 if self._nz(aq_t1) else 1.0

            # 4. SGI ──────────────────────────────────────────
            comp['SGI'] = sales_t / sales_t1 if self._nz(sales_t1) else 1.0

            # 5. DEPI ─────────────────────────────────────────
            dr_t  = dep_t / (self._z(fa_t) + dep_t)   if self._nz(self._z(fa_t) + dep_t) else 0
            dr_t1 = dep_t1 / (self._z(fa_t1) + dep_t1) if self._nz(self._z(fa_t1) + dep_t1) else 0
            comp['DEPI'] = dr_t1 / dr_t if self._nz(dr_t) else 1.0

            # 6. SGAI ─────────────────────────────────────────
            sr_t  = exp_t / sales_t   if self._nz(sales_t)  else 0
            sr_t1 = exp_t1 / sales_t1 if self._nz(sales_t1) else 0
            comp['SGAI'] = sr_t / sr_t1 if self._nz(sr_t1) else 1.0

            # 7. TATA ─────────────────────────────────────────
            if self._ok(cfo_t, np_t) and self._nz(ta_t):
                comp['TATA'] = (np_t - cfo_t) / ta_t
            else:
                comp['TATA'] = 0.0

            # 8. LVGI ─────────────────────────────────────────
            lev_t  = (self._z(ol_t) + self._z(borr_t)) / ta_t   if self._nz(ta_t)  else 0
            lev_t1 = (self._z(ol_t1) + self._z(borr_t1)) / ta_t1 if self._nz(ta_t1) else 0
            comp['LVGI'] = lev_t / lev_t1 if self._nz(lev_t1) else 1.0

            # ── Final M-Score ────────────────────────────────
            m = self.COEFF['intercept']
            for k, c in self.COEFF.items():
                if k != 'intercept':
                    m += c * comp[k]

            # Interpretation
            th = config.thresholds
            if m > th.mscore_manipulation:
                interp = "⚠️  LIKELY MANIPULATOR — High probability of earnings manipulation"
                risk   = "HIGH"
            elif m > th.mscore_safe:
                interp = "⚡ GREY ZONE — Inconclusive; warrants deeper investigation"
                risk   = "MEDIUM"
            else:
                interp = "✅ UNLIKELY MANIPULATOR — Low probability of earnings manipulation"
                risk   = "LOW"

            result.update({
                'available':      True,
                'm_score':        round(m, 4),
                'interpretation': interp,
                'risk_level':     risk,
                'components':     {k: round(v, 4) for k, v in comp.items()},
                'thresholds': {
                    'manipulation_likely':   th.mscore_manipulation,
                    'manipulation_unlikely': th.mscore_safe,
                },
            })

        except Exception as e:
            result['reason'] = f'M-Score calculation error: {e}'

        return result

    # ── tiny helpers ──────────────────────────────────────────────────
    @staticmethod
    def _z(v):
        """NaN → 0."""
        return 0.0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)

    @staticmethod
    def _ok(*vals):
        """True when ALL values are finite and non-zero."""
        return all(
            v is not None and not (isinstance(v, float) and np.isnan(v)) and v != 0
            for v in vals
        )

    @staticmethod
    def _nz(v):
        """True when v is finite and non-zero."""
        if v is None:
            return False
        if isinstance(v, float) and np.isnan(v):
            return False
        return v != 0
