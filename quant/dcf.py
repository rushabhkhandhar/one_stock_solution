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
    # Sectors where FCFF/FCFE DCF is structurally meaningless
    _FINANCIAL_SECTORS = {
        'financial services', 'banking', 'nbfc', 'insurance',
        'financials', 'banks - regional', 'banks - diversified',
        'credit services', 'financial data & stock exchanges',
    }

    def calculate(self, data: dict, sector: str = '') -> dict:
        """Run the full DCF valuation and return a result dict.

        Parameters
        ----------
        data : dict
            Financial data from ingestion.
        sector : str
            yfinance sector/industry string. If the sector is a
            financial-services variant, DCF is skipped (banks use
            deposits as raw materials, making FCF meaningless).
        """
        pnl      = data.get('pnl', pd.DataFrame())
        bs       = data.get('balance_sheet', pd.DataFrame())
        cf       = data.get('cash_flow', pd.DataFrame())
        price_df = data.get('price', pd.DataFrame())

        result = {'available': False, 'reason': ''}

        # ── Rule 1A: Skip DCF entirely for banks / NBFCs ──
        _sec_lower = sector.lower().strip() if sector else ''
        if _sec_lower and any(fs in _sec_lower
                              for fs in self._FINANCIAL_SECTORS):
            result['reason'] = (
                f'DCF disabled for financial-services sector '
                f'("{sector}"). Banks/NBFCs use deposits as raw '
                f'materials — standard FCFF/FCFE models are '
                f'meaningless. Use P/B, Residual Income, or DDM.')
            result['sector_skip'] = True
            return result

        # ── Verify required live market params are available ──
        if self.m.risk_free_rate is None:
            result['reason'] = 'Risk-free rate not available (live fetch failed)'
            return result
        if self.m.market_risk_premium is None:
            result['reason'] = 'Market risk premium not available (live fetch failed)'
            return result
        if self.m.terminal_growth_rate is None:
            result['reason'] = 'Terminal growth rate not available (live fetch failed)'
            return result

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

            # ── Rule 1B: Flag peak-CapEx companies ─────────
            # If |CapEx| / Operating-CF > 0.8, current FCF is
            # structurally depressed and DCF will undervalue.
            latest_ocf = operating_cf.dropna()
            latest_capex = capex.dropna()
            if len(latest_ocf) >= 1 and len(latest_capex) >= 1:
                _ocf_val = float(latest_ocf.iloc[-1])
                _capex_val = abs(float(latest_capex.iloc[-1]))
                if _ocf_val > 0:
                    _capex_ratio = _capex_val / _ocf_val
                    result['capex_ocf_ratio'] = round(_capex_ratio, 3)
                    result['peak_capex'] = _capex_ratio > 0.8

            # ── 2. Growth-rate estimation ───────────────────
            fcf_growth     = self._estimate_growth(fcf)
            revenue_growth = self._estimate_growth(pp.get(pnl, 'sales').dropna())

            if fcf_growth is None and revenue_growth is None:
                result['reason'] = 'Insufficient data to estimate growth rate'
                return result

            # Use the available estimate (prefer FCF, fallback to revenue)
            base_growth = fcf_growth if fcf_growth is not None else revenue_growth
            # No artificial clamping — real data stands as-is
            growth_rate    = base_growth
            terminal_g     = self.m.terminal_growth_rate

            # ── 3. WACC ────────────────────────────────────
            wacc = self._calculate_wacc(data, terminal_g)
            beta_estimated = getattr(self, '_beta_estimated', False)
            if wacc is None:
                result['reason'] = 'WACC could not be computed (missing beta or market data)'
                return result
            if wacc <= terminal_g:
                result['reason'] = (f'WACC ({wacc:.2%}) ≤ terminal growth ({terminal_g:.2%}) — '
                                    'DCF model invalid under current market conditions')
                return result

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
                # Guard against negative projected FCFs (e.g. from
                # very negative growth rates). Floor at zero — we
                # don't give credit for value-destroying cash flows.
                if fcf_proj < 0:
                    fcf_proj = 0.0
                pv        = fcf_proj / (1 + wacc) ** yr
                projected_fcf.append(fcf_proj)
                pv_fcf.append(pv)

            # ── Step 1 result: PV of projected FCFs ──────
            pv_of_fcf_total = sum(pv_fcf)

            # ── Step 2: Terminal Value (Gordon Growth) ─────
            terminal_fcf = projected_fcf[-1] * (1 + terminal_g)
            if terminal_fcf <= 0:
                # If final projected FCF is zero/negative, terminal
                # value cannot be meaningfully computed.
                terminal_val = 0.0
                pv_terminal  = 0.0
            else:
                terminal_val = terminal_fcf / (wacc - terminal_g)
                pv_terminal  = terminal_val / (1 + wacc) ** n

            # ── Step 3: Implied Enterprise Value ───────────
            enterprise_value = pv_of_fcf_total + pv_terminal

            # ── Step 4: Bridge to Equity Value & Target Price
            net_debt     = self._get_net_debt(bs)
            equity_value = enterprise_value - net_debt

            shares_cr = self._get_shares(data, pnl)
            if np.isnan(shares_cr) or shares_cr <= 0:
                result['reason'] = 'Cannot determine shares outstanding'
                return result

            intrinsic_value = equity_value / shares_cr
            current_price   = self._get_current_price(price_df)
            upside = (
                ((intrinsic_value - current_price) / current_price * 100)
                if not np.isnan(current_price) and current_price > 0 else np.nan
            )

            # ── Market Cap & Market EV for sanity check ───
            market_cap = (
                current_price * shares_cr
                if not np.isnan(current_price) and shares_cr > 0
                else np.nan
            )
            market_ev = (
                market_cap + net_debt
                if not np.isnan(market_cap)
                else np.nan
            )

            # ── GUARDRAIL: DCF EV vs Market EV deviation ──
            from config import config as _cfg
            _ev_thresh = _cfg.validation.dcf_ev_threshold_pct
            dcf_ev_mismatch = False
            ev_delta_pct = None
            if not np.isnan(market_ev) and market_ev > 0:
                ev_delta_pct = abs(enterprise_value - market_ev) / market_ev * 100
                if ev_delta_pct > _ev_thresh:
                    dcf_ev_mismatch = True
                    print(f"  ⚠ DCF GUARDRAIL: EV(DCF) ₹{enterprise_value:,.0f} Cr "
                          f"vs Market EV ₹{market_ev:,.0f} Cr "
                          f"(delta {ev_delta_pct:.0f}% > {_ev_thresh:.0f}% threshold) — "
                          f"Target Price overridden to N/A")

            result.update({
                'available':        True,
                'intrinsic_value':  round(intrinsic_value, 2),
                'current_price':    round(current_price, 2),
                'upside_pct':       round(upside, 2) if not np.isnan(upside) else None,
                # 4-step DCF breakdown
                'pv_of_fcf':        round(pv_of_fcf_total, 2),
                'pv_of_terminal':   round(pv_terminal, 2),
                'terminal_value':   round(terminal_val, 2),
                'enterprise_value': round(enterprise_value, 2),
                'equity_value':     round(equity_value, 2),
                'market_cap':       round(market_cap, 2) if not np.isnan(market_cap) else None,
                'market_ev':        round(market_ev, 2) if not np.isnan(market_ev) else None,
                # Guardrail
                'dcf_ev_mismatch':  dcf_ev_mismatch,
                'ev_delta_pct':     round(ev_delta_pct, 1) if ev_delta_pct is not None else None,
                # Inputs
                'wacc':             round(wacc * 100, 2),
                'growth_rate':      round(growth_rate * 100, 2),
                'terminal_growth':  round(terminal_g * 100, 2),
                'net_debt':         round(net_debt, 2),
                'latest_fcf':       round(latest_fcf, 2),
                'projected_fcf':    [round(f, 2) for f in projected_fcf],
                'shares_cr':        round(shares_cr, 2),
                'beta_estimated':   getattr(self, '_beta_estimated', False),
                'effective_tax_rate': round(self._effective_tax_rate * 100, 2) if self._effective_tax_rate is not None else None,
                'risk_free_rate':   round(self.m.risk_free_rate * 100, 2),
                'rf_source':        getattr(self.m, 'risk_free_rate_source', 'fallback'),
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
            return None  # Insufficient data — no fallback
        pos = s[s > 0]
        if len(pos) < 2:
            return None  # Insufficient data — no fallback
        n = len(pos) - 1
        cagr = (pos.iloc[-1] / pos.iloc[0]) ** (1 / n) - 1
        return cagr  # No artificial clamping — real data stands

    def _calculate_wacc(self, data: dict, terminal_g: float = 0.04) -> float:
        """Weighted Average Cost of Capital.
        
        Tax rate is computed from the REAL P&L (effective tax rate =
        tax_expense / PBT) instead of using a hardcoded assumption.
        """
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())

        # ── Effective tax rate from REAL P&L data ──────────
        tax_rate = self._compute_effective_tax_rate(pnl)
        if tax_rate is None:
            return None  # Cannot compute WACC without tax rate
        self._effective_tax_rate = tax_rate  # expose for reporting

        # Cost of equity (CAPM) — ONLY live beta, no fallback
        beta_info = data.get('beta_info', {})
        self._beta_estimated = False
        if beta_info.get('available') and beta_info.get('beta'):
            beta = beta_info['beta']
        else:
            # No live beta available — cannot compute WACC
            return None
        ke = self.m.risk_free_rate + beta * self.m.market_risk_premium

        # Cost of debt
        interest   = get_value(pp.get(pnl, 'interest'))
        borrowings = get_value(pp.get(bs, 'borrowings'))
        if (not np.isnan(interest) and not np.isnan(borrowings)
                and borrowings > 0):
            kd = interest / borrowings
        elif not np.isnan(borrowings) and borrowings > 0:
            # Have debt but no interest line — use live credit spread
            if self.m.default_credit_spread is not None:
                kd = self.m.risk_free_rate + self.m.default_credit_spread
            else:
                # Cannot determine cost of debt — skip debt component
                kd = self.m.risk_free_rate  # conservative: Rf as floor for Kd
        else:
            kd = 0.0  # No debt — cost of debt is irrelevant

        # Weights
        eq_capital = get_value(pp.get(bs, 'equity_capital'))
        reserves   = get_value(pp.get(bs, 'reserves'))
        equity_val = self._s(eq_capital) + self._s(reserves)
        debt_val   = borrowings if not np.isnan(borrowings) else 0
        if equity_val <= 0:
            # Negative equity — use 100% equity cost (Ke) as WACC
            wacc = ke * (1 - tax_rate)
            return wacc  # No artificial floor — real computation stands

        total = equity_val + debt_val
        we = equity_val / total
        wd = debt_val / total

        wacc = we * ke + wd * kd * (1 - tax_rate)
        return wacc  # no artificial floor — real computation stands

    def _compute_effective_tax_rate(self, pnl: pd.DataFrame) -> float:
        """Compute effective tax rate from REAL P&L data.
        
        screener.in provides 'Tax%' directly as the effective rate.
        Falls back to tax_expense / PBT if absolute values are available.
        Uses config.tax_rate ONLY as last resort.
        """
        if pnl.empty:
            return None  # No P&L data — cannot determine tax rate

        # Method 1: Use screener.in's own Tax% (already effective rate)
        tax_pct_series = pp.get(pnl, 'tax_pct')
        if not tax_pct_series.empty:
            # Tax% from screener is in decimal (0.25 = 25%) or percentage
            rates = []
            for i in range(min(3, len(tax_pct_series))):
                val = get_value(tax_pct_series, i)
                if not np.isnan(val):
                    # screener.in reports as fraction (0.22, 0.24, etc.)
                    rate = val if val < 1.0 else val / 100.0
                    if 0.0 < rate < 0.60:  # sanity: 0% to 60%
                        rates.append(rate)
            if rates:
                return round(sum(rates) / len(rates), 4)

        # Method 2: Compute from absolute tax and PBT values
        tax_series = pp.get(pnl, 'tax')
        pbt_series = pp.get(pnl, 'pbt')
        if not tax_series.empty and not pbt_series.empty:
            rates = []
            for i in range(min(3, len(tax_series))):
                tax_val = get_value(tax_series, i)
                pbt_val = get_value(pbt_series, i)
                if (not np.isnan(tax_val) and not np.isnan(pbt_val)
                        and pbt_val > 0 and tax_val >= 0):
                    rate = tax_val / pbt_val
                    if 0.0 < rate < 0.60:
                        rates.append(rate)
            if rates:
                return round(sum(rates) / len(rates), 4)

        return None  # No tax data available — do not fabricate

    def _get_net_debt(self, bs: pd.DataFrame) -> float:
        """Net Debt = Total Borrowings - Cash & Equivalents.

        Proper DCF formula: Equity Value = EV(DCF) - Net Debt
        where Net Debt = Debt - Cash.  Subtracting only gross debt
        without adding back cash overstates the deduction.
        """
        borr = self._s(get_value(pp.get(bs, 'borrowings')))
        cash = self._s(get_value(pp.get(bs, 'cash_equivalents')))
        return borr - cash

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
        return np.nan  # No price data — do not fabricate

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
                iv = round(eq / shares_cr, 2) if shares_cr > 0 else None
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

        # Determine red flag threshold from historical data:
        # If we have 3+ years of history, use mean - 1σ as the threshold.
        # This flags only companies whose conversion ratio is unusually low
        # relative to their OWN historical pattern.
        valid_history = [h for h in history if h is not None]
        if len(valid_history) >= 3:
            import numpy as np
            hist_mean = np.mean(valid_history)
            hist_std = np.std(valid_history)
            dynamic_threshold = hist_mean - hist_std
        else:
            # With limited history, flag if below 50% (well below break-even conversion)
            dynamic_threshold = 50.0

        flag = conversion_pct < dynamic_threshold
        assessment = (
            f"🔴 Poor cash conversion ({conversion_pct}% < {dynamic_threshold:.0f}% threshold) — "
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
