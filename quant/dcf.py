"""
Discounted Cash Flow (DCF) Valuation Model
===========================================

Formulas
--------
  FCF  = EBIAT + D&A − CAPEX − ΔNWC
       ≈ Operating Cash Flow − CAPEX           (simplified)

  Cost of Equity (CAPM)  =  Rf + β × (Rm − Rf)

  WACC  =  (E/(E+D)) × Ke  +  (D/(E+D)) × Kd × (1 − t)

  Terminal Value  =  FCF_n × (1+g) / (WACC − g)

  Intrinsic Value / Share  =  (Σ PV(FCF) + PV(TV) − Net Debt) / Shares
"""
import numpy as np
import pandas as pd
from config import config
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


class DCFModel:

    def __init__(self, cfg=None):
        self.cfg = cfg or config
        self.m = self.cfg.market

    # ==================================================================
    # Public API
    # ==================================================================
    def calculate(self, data: dict) -> dict:
        """Run the full DCF valuation and return a result dict."""
        pnl      = data.get('pnl', pd.DataFrame())
        bs       = data.get('balance_sheet', pd.DataFrame())
        cf       = data.get('cash_flow', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        result = {'available': False, 'reason': ''}

        if cf.empty or pnl.empty or bs.empty:
            result['reason'] = 'Insufficient financial data for DCF'
            return result

        try:
            # ── 1. Historical Free Cash Flow ────────────────
            operating_cf = pp.get(cf, 'operating_cf')
            capex        = self._find_capex(cf)
            fcf          = (operating_cf + capex).dropna()   # capex is -ve

            if len(fcf) < 3:
                result['reason'] = 'Not enough FCF history (need ≥3 years)'
                return result

            # ── 2. Growth-rate estimation ───────────────────
            fcf_growth     = self._estimate_growth(fcf)
            revenue_growth = self._estimate_growth(pp.get(pnl, 'sales').dropna())
            # Use the more conservative estimate, cap at 20 %
            growth_rate    = min(max(fcf_growth, 0.02), 0.20)
            terminal_g     = self.m.terminal_growth_rate

            # ── 3. WACC ────────────────────────────────────
            wacc = self._calculate_wacc(data)
            if wacc <= terminal_g:
                wacc = terminal_g + 0.02        # safety margin

            # ── 4. Latest / base FCF ───────────────────────
            latest_fcf = get_value(fcf)
            if np.isnan(latest_fcf) or latest_fcf <= 0:
                latest_fcf = fcf.tail(3).mean()
                if np.isnan(latest_fcf) or latest_fcf <= 0:
                    result['reason'] = 'Negative / zero FCF — DCF not applicable'
                    result['latest_fcf'] = (
                        float(latest_fcf) if not np.isnan(latest_fcf) else 0
                    )
                    return result

            # ── 5. Project FCF with linearly decaying growth ─
            projected_fcf = []
            pv_fcf        = []
            n = self.m.projection_years
            for yr in range(1, n + 1):
                yr_growth = growth_rate - (growth_rate - terminal_g) * (yr / n)
                fcf_proj  = latest_fcf * (1 + yr_growth) ** yr
                pv        = fcf_proj / (1 + wacc) ** yr
                projected_fcf.append(fcf_proj)
                pv_fcf.append(pv)

            # ── 6. Terminal Value ──────────────────────────
            terminal_fcf = projected_fcf[-1] * (1 + terminal_g)
            terminal_val = terminal_fcf / (wacc - terminal_g)
            pv_terminal  = terminal_val / (1 + wacc) ** n

            # ── 7. Enterprise Value → Equity Value ─────────
            enterprise_value = sum(pv_fcf) + pv_terminal
            net_debt         = self._get_net_debt(bs)
            equity_value     = enterprise_value - net_debt

            # ── 8. Per-share intrinsic value ───────────────
            shares_cr = self._get_shares(data, pnl)
            if np.isnan(shares_cr) or shares_cr <= 0:
                result['reason'] = 'Cannot determine shares outstanding'
                return result

            intrinsic_value = equity_value / shares_cr
            current_price   = self._get_current_price(price_df)
            upside = (
                ((intrinsic_value - current_price) / current_price * 100)
                if current_price > 0 else np.nan
            )

            result.update({
                'available':        True,
                'intrinsic_value':  round(intrinsic_value, 2),
                'current_price':    round(current_price, 2),
                'upside_pct':       round(upside, 2) if not np.isnan(upside) else None,
                'enterprise_value': round(enterprise_value, 2),
                'equity_value':     round(equity_value, 2),
                'wacc':             round(wacc * 100, 2),
                'growth_rate':      round(growth_rate * 100, 2),
                'terminal_growth':  round(terminal_g * 100, 2),
                'net_debt':         round(net_debt, 2),
                'latest_fcf':       round(latest_fcf, 2),
                'projected_fcf':    [round(f, 2) for f in projected_fcf],
                'shares_cr':        round(shares_cr, 2),
            })

            # ── 9. WACC Sensitivity Grid ───────────────────
            try:
                sensitivity = self._wacc_sensitivity(
                    latest_fcf, growth_rate, terminal_g, wacc,
                    net_debt, shares_cr, n)
                result['sensitivity'] = sensitivity
            except Exception:
                result['sensitivity'] = {'available': False}

        except Exception as e:
            result['reason'] = f'DCF calculation error: {e}'

        return result

    # ==================================================================
    # Private helpers
    # ==================================================================
    def _find_capex(self, cf: pd.DataFrame) -> pd.Series:
        """Prefer 'Fixed assets purchased' addon; fall back to Investing CF."""
        for col in cf.columns:
            if 'fixedassetspurchased' in col.lower().replace(' ', ''):
                return cf[col]
        return pp.get(cf, 'investing_cf')

    def _estimate_growth(self, series: pd.Series) -> float:
        """CAGR of a positive-only series, clamped to [-10 %, 30 %]."""
        s = series.dropna()
        if len(s) < 2:
            return 0.05
        pos = s[s > 0]
        if len(pos) < 2:
            return 0.05
        n = len(pos) - 1
        cagr = (pos.iloc[-1] / pos.iloc[0]) ** (1 / n) - 1
        return min(max(cagr, -0.10), 0.30)

    def _calculate_wacc(self, data: dict) -> float:
        """Weighted Average Cost of Capital."""
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())

        # Cost of equity (CAPM) — use real beta if available
        beta_info = data.get('beta_info', {})
        if beta_info.get('available') and beta_info.get('beta'):
            beta = beta_info['beta']
        else:
            beta = self.m.default_beta
        ke = self.m.risk_free_rate + beta * self.m.market_risk_premium

        # Cost of debt
        interest   = get_value(pp.get(pnl, 'interest'))
        borrowings = get_value(pp.get(bs, 'borrowings'))
        kd = (
            (interest / borrowings)
            if (not np.isnan(interest) and not np.isnan(borrowings)
                and borrowings > 0)
            else 0.09
        )

        # Weights
        eq_capital = get_value(pp.get(bs, 'equity_capital'))
        reserves   = get_value(pp.get(bs, 'reserves'))
        equity_val = self._s(eq_capital) + self._s(reserves)
        debt_val   = borrowings if not np.isnan(borrowings) else 0
        if equity_val <= 0:
            equity_val = 1          # fallback

        total = equity_val + debt_val
        we = equity_val / total
        wd = debt_val / total

        wacc = we * ke + wd * kd * (1 - self.m.tax_rate)
        return max(wacc, 0.08)      # floor at 8 %

    def _get_net_debt(self, bs: pd.DataFrame) -> float:
        borr = get_value(pp.get(bs, 'borrowings'))
        return self._s(borr)

    def _get_shares(self, data: dict, pnl: pd.DataFrame) -> float:
        """Shares outstanding (in Cr)."""
        shares = data.get('shares_outstanding')
        if isinstance(shares, pd.Series) and not shares.empty:
            val = get_value(shares)
            if not np.isnan(val) and val > 0:
                return val
        # Fallback: Net Profit / EPS
        np_val  = get_value(pp.get(pnl, 'net_profit'))
        eps_val = get_value(pp.get(pnl, 'eps'))
        if not np.isnan(np_val) and not np.isnan(eps_val) and eps_val > 0:
            return np_val / eps_val
        return np.nan

    def _get_current_price(self, price_df: pd.DataFrame) -> float:
        if isinstance(price_df, pd.DataFrame) and not price_df.empty:
            if 'close' in price_df.columns:
                return float(price_df['close'].iloc[-1])
            return float(price_df.iloc[-1, 0])
        return 0.0

    @staticmethod
    def _s(v):
        return 0.0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)

    # ==================================================================
    # WACC Sensitivity Grid
    # ==================================================================
    def _wacc_sensitivity(self, latest_fcf, growth_rate, terminal_g,
                          base_wacc, net_debt, shares_cr, n):
        """
        Generate a meshgrid showing intrinsic value at varying
        WACC and terminal growth rates.

        Returns a dict with 'wacc_range', 'tgr_range', and 'grid' (2D list).
        """
        wacc_range = [round(base_wacc + d, 3)
                      for d in [-0.02, -0.01, 0.0, 0.01, 0.02]]
        tgr_range  = [round(terminal_g + d, 3)
                      for d in [-0.01, -0.005, 0.0, 0.005, 0.01]]
        # Ensure WACC > TGR always
        grid = []
        for wacc in wacc_range:
            row = []
            for tgr in tgr_range:
                if wacc <= tgr + 0.005:
                    row.append(None)   # Invalid: WACC ≤ TGR
                    continue
                projected = []
                for yr in range(1, n + 1):
                    yr_g = growth_rate - (growth_rate - tgr) * (yr / n)
                    projected.append(latest_fcf * (1 + yr_g) ** yr)
                pv_sum = sum(f / (1 + wacc) ** (i + 1)
                             for i, f in enumerate(projected))
                tv = projected[-1] * (1 + tgr) / (wacc - tgr)
                pv_tv = tv / (1 + wacc) ** n
                ev = pv_sum + pv_tv
                eq = ev - net_debt
                iv = round(eq / shares_cr, 2) if shares_cr > 0 else 0
                row.append(iv)
            grid.append(row)
        return {
            'available': True,
            'wacc_range': [round(w * 100, 2) for w in wacc_range],
            'tgr_range':  [round(t * 100, 2) for t in tgr_range],
            'grid': grid,
        }

    # ==================================================================
    # CFO / EBITDA Conversion Check (Cash-Flow Realism)
    # ==================================================================
    @staticmethod
    def cfo_ebitda_check(data: dict) -> dict:
        """
        Flag companies where CFO/EBITDA < 70%.

        A low ratio suggests reported profits are NOT being converted
        to actual cash — a classic red flag.
        """
        cf  = data.get('cash_flow', pd.DataFrame())
        pnl = data.get('pnl', pd.DataFrame())

        cfo = pp.get(cf, 'operating_cf')
        dep = pp.get(pnl, 'depreciation')
        pat = pp.get(pnl, 'net_profit')
        interest = pp.get(pnl, 'interest')
        tax = pp.get(pnl, 'tax')

        if cfo.dropna().empty or pat.dropna().empty:
            return {'available': False, 'reason': 'Insufficient data'}

        # EBITDA ≈ PAT + Depreciation + Interest + Tax
        ebitda = (pat.fillna(0) + dep.fillna(0) +
                  interest.fillna(0) + tax.fillna(0))

        latest_cfo = float(cfo.dropna().iloc[-1]) if not cfo.dropna().empty else 0
        latest_ebitda = float(ebitda.dropna().iloc[-1]) if not ebitda.dropna().empty else 0

        if latest_ebitda <= 0:
            return {'available': False, 'reason': 'Non-positive EBITDA'}

        ratio = latest_cfo / latest_ebitda
        conversion_pct = round(ratio * 100, 1)

        # 3-year trend
        history = []
        for i in range(-min(3, len(cfo)), 0):
            try:
                c = float(cfo.iloc[i])
                e = float(ebitda.iloc[i])
                history.append(round(c / e * 100, 1) if e > 0 else None)
            except (IndexError, ZeroDivisionError):
                history.append(None)

        flag = conversion_pct < 70
        assessment = (
            f"🔴 Poor cash conversion ({conversion_pct}% < 70%) — "
            "profits may not be backed by real cash" if flag
            else f"🟢 Healthy cash conversion ({conversion_pct}%)"
        )
        return {
            'available': True,
            'cfo': round(latest_cfo, 2),
            'ebitda': round(latest_ebitda, 2),
            'conversion_pct': conversion_pct,
            'ratio': conversion_pct,          # alias for report
            'history': history,
            'flag': flag,
            'is_red_flag': flag,               # canonical key for orchestrator/synthesis
            'assessment': assessment,
            'interpretation': assessment,       # alias for report
        }
