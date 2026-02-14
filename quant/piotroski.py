"""
Piotroski F-Score — Financial-Strength Scoring (0 – 9)
======================================================

Profitability (4 pts)
  F1  ROA > 0
  F2  Operating Cash Flow > 0
  F3  ΔROA > 0  (year-on-year improvement)
  F4  CFO > Net Income  (earnings quality / accruals)

Leverage & Liquidity (3 pts)
  F5  Δ(Borrowings / Total Assets) ≤ 0
  F6  Δ Current Ratio > 0
  F7  No new equity dilution

Operating Efficiency (2 pts)
  F8  Δ Gross Margin > 0
  F9  Δ Asset Turnover > 0

Rating
  8–9  →  STRONG        5–7  →  MODERATE        0–4  →  WEAK
"""
import numpy as np
import pandas as pd
from config import config
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


class PiotroskiFScore:

    def calculate(self, data: dict) -> dict:
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())
        cf  = data.get('cash_flow', pd.DataFrame())

        result = {'available': False, 'reason': ''}

        if pnl.empty or bs.empty or len(pnl) < 2:
            result['reason'] = 'Need ≥ 2 years of data'
            return result

        try:
            def v(df, name, idx=-1):
                return get_value(pp.get(df, name), idx)

            # Current (t) and prior-year (t-1)
            np_t  = v(pnl, 'net_profit', -1);  np_t1  = v(pnl, 'net_profit', -2)
            sal_t = v(pnl, 'sales', -1);       sal_t1 = v(pnl, 'sales', -2)
            exp_t = v(pnl, 'expenses', -1);    exp_t1 = v(pnl, 'expenses', -2)

            ta_t  = v(bs, 'total_assets', -1); ta_t1  = v(bs, 'total_assets', -2)
            br_t  = v(bs, 'borrowings', -1);   br_t1  = v(bs, 'borrowings', -2)
            ec_t  = v(bs, 'equity_capital', -1);ec_t1  = v(bs, 'equity_capital', -2)
            ol_t  = v(bs, 'other_liabilities', -1); ol_t1 = v(bs, 'other_liabilities', -2)
            oa_t  = v(bs, 'other_assets', -1);  oa_t1  = v(bs, 'other_assets', -2)

            cfo_t = v(cf, 'operating_cf', -1)

            criteria = {}
            score = 0

            # ── PROFITABILITY ────────────────────────────────
            roa_t  = np_t / ta_t   if self._nz(ta_t)  else 0
            roa_t1 = np_t1 / ta_t1 if self._nz(ta_t1) else 0

            f1 = int(roa_t > 0)
            criteria['F1_ROA_positive'] = {'pass': bool(f1), 'value': round(roa_t, 4)}
            score += f1

            f2 = int(not np.isnan(cfo_t) and cfo_t > 0)
            criteria['F2_CFO_positive'] = {'pass': bool(f2), 'value': round(self._z(cfo_t), 2)}
            score += f2

            f3 = int(roa_t > roa_t1)
            criteria['F3_ROA_improving'] = {'pass': bool(f3), 'value': round(roa_t - roa_t1, 4)}
            score += f3

            f4 = int(not np.isnan(cfo_t) and cfo_t > np_t)
            criteria['F4_Accrual_quality'] = {
                'pass': bool(f4),
                'cfo': round(self._z(cfo_t), 2),
                'net_income': round(self._z(np_t), 2),
            }
            score += f4

            # ── LEVERAGE / LIQUIDITY ─────────────────────────
            dr_t  = self._z(br_t) / ta_t   if self._nz(ta_t)  else 0
            dr_t1 = self._z(br_t1) / ta_t1 if self._nz(ta_t1) else 0
            f5 = int(dr_t <= dr_t1)
            criteria['F5_Debt_decreasing'] = {'pass': bool(f5), 'value': round(dr_t - dr_t1, 4)}
            score += f5

            cr_t  = self._z(oa_t) / self._z(ol_t)   if self._nz(self._z(ol_t))  else 0
            cr_t1 = self._z(oa_t1) / self._z(ol_t1) if self._nz(self._z(ol_t1)) else 0
            f6 = int(cr_t > cr_t1)
            criteria['F6_CurrentRatio_improving'] = {'pass': bool(f6), 'value': round(cr_t - cr_t1, 4)}
            score += f6

            f7 = int(self._z(ec_t) <= self._z(ec_t1))
            criteria['F7_No_dilution'] = {
                'pass': bool(f7),
                'equity_t': round(self._z(ec_t), 2),
                'equity_t1': round(self._z(ec_t1), 2),
            }
            score += f7

            # ── OPERATING EFFICIENCY ─────────────────────────
            gm_t  = (sal_t - exp_t) / sal_t   if self._nz(sal_t)  else 0
            gm_t1 = (sal_t1 - exp_t1) / sal_t1 if self._nz(sal_t1) else 0
            f8 = int(gm_t > gm_t1)
            criteria['F8_GrossMargin_improving'] = {'pass': bool(f8), 'value': round(gm_t - gm_t1, 4)}
            score += f8

            at_t  = sal_t / ta_t   if self._nz(ta_t)  else 0
            at_t1 = sal_t1 / ta_t1 if self._nz(ta_t1) else 0
            f9 = int(at_t > at_t1)
            criteria['F9_AssetTurnover_improving'] = {'pass': bool(f9), 'value': round(at_t - at_t1, 4)}
            score += f9

            # ── Interpretation ───────────────────────────────
            th = config.thresholds
            if score >= th.fscore_strong:
                interp   = "💪 STRONG — Excellent financial health"
                strength = "STRONG"
            elif score >= th.fscore_moderate:
                interp   = "👍 MODERATE — Decent financial position"
                strength = "MODERATE"
            else:
                interp   = "⚠️  WEAK — Poor financial health, high risk"
                strength = "WEAK"

            result.update({
                'available':      True,
                'f_score':        score,
                'max_score':      9,
                'interpretation': interp,
                'strength':       strength,
                'criteria':       criteria,
            })

        except Exception as e:
            result['reason'] = f'F-Score error: {e}'

        return result

    # helpers
    @staticmethod
    def _z(v):
        return 0.0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)

    @staticmethod
    def _nz(v):
        if v is None:
            return False
        if isinstance(v, float) and np.isnan(v):
            return False
        return v != 0
