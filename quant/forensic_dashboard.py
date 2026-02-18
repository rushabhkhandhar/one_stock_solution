"""
Forensic Dashboard — Earnings Quality & Red Flag Engine
========================================================
Unified forensic analysis combining:
  1. Cash Flow Realism (CFO/EBITDA < 70% flag)
  2. Related Party Transactions (RPT as % of Revenue)
  3. Contingent Liabilities (as % of Net Worth)
  4. Accruals Quality (Total Accruals / Total Assets)
  5. Revenue-Receivables Divergence
  6. Earnings Quality Composite Score

Specifically tuned for Indian market forensic accounting.
"""
import numpy as np
import pandas as pd
from data.preprocessing import DataPreprocessor, get_value

pp = DataPreprocessor()


class ForensicDashboard:
    """
    Comprehensive forensic analysis dashboard.

    Combines multiple earnings-quality signals into a single
    composite score with individual red-flag triggers.
    """

    def analyze(self, data: dict, analysis: dict) -> dict:
        """
        Run the full forensic dashboard.

        Parameters
        ----------
        data     : full data dict with pnl, balance_sheet, cash_flow
        analysis : analysis dict with cfo_ebitda_check, rpt, contingent, etc.

        Returns
        -------
        dict with forensic_score, individual checks, red_flags
        """
        checks = []
        red_flags = []
        total_score = 0
        max_score = 0

        # ── 1. Cash Flow Realism (CFO/EBITDA) ───────────────
        cfo_check = self._check_cfo_ebitda(data, analysis)
        checks.append(cfo_check)
        if cfo_check['pass'] is not None:   # Only count if not SKIP
            max_score += 2
            if cfo_check['pass']:
                total_score += 2
            elif cfo_check.get('is_red_flag'):
                red_flags.append({
                    'severity': 'HIGH',
                    'category': 'Cash Flow Realism',
                    'detail': cfo_check['detail'],
                })

        # ── 2. Accruals Quality ─────────────────────────────
        accruals_check = self._check_accruals_quality(data)
        checks.append(accruals_check)
        if accruals_check['pass'] is not None:
            max_score += 2
            if accruals_check['pass']:
                total_score += 2
            elif accruals_check.get('is_red_flag'):
                red_flags.append({
                    'severity': 'MEDIUM',
                    'category': 'Accruals Quality',
                    'detail': accruals_check['detail'],
                })

        # ── 3. Revenue vs Receivables Divergence ────────────
        rev_recv_check = self._check_revenue_receivables(data)
        checks.append(rev_recv_check)
        if rev_recv_check['pass'] is not None:
            max_score += 2
            if rev_recv_check['pass']:
                total_score += 2
            elif rev_recv_check.get('is_red_flag'):
                red_flags.append({
                    'severity': 'MEDIUM',
                    'category': 'Revenue-Receivables Divergence',
                    'detail': rev_recv_check['detail'],
                })

        # ── 4. Related Party Transactions ───────────────────
        rpt_check = self._check_rpt(analysis)
        checks.append(rpt_check)
        if rpt_check['pass'] is not None:
            max_score += 2
            if rpt_check['pass']:
                total_score += 2
            elif rpt_check.get('is_red_flag'):
                red_flags.append({
                    'severity': rpt_check.get('severity', 'MEDIUM'),
                    'category': 'Related Party Transactions',
                    'detail': rpt_check['detail'],
                })

        # ── 5. Contingent Liabilities ───────────────────────
        cl_check = self._check_contingent(analysis)
        checks.append(cl_check)
        if cl_check['pass'] is not None:
            max_score += 1
            if cl_check['pass']:
                total_score += 1
            elif cl_check.get('is_red_flag'):
                red_flags.append({
                    'severity': cl_check.get('severity', 'MEDIUM'),
                    'category': 'Contingent Liabilities',
                    'detail': cl_check['detail'],
                })

        # ── 6. Operating Cash Flow Trend ────────────────────
        cfo_trend_check = self._check_cfo_trend(data)
        checks.append(cfo_trend_check)
        if cfo_trend_check['pass'] is not None:
            max_score += 1
            if cfo_trend_check['pass']:
                total_score += 1
            elif cfo_trend_check.get('is_red_flag'):
                red_flags.append({
                    'severity': 'MEDIUM',
                    'category': 'Cash Flow Trend',
                    'detail': cfo_trend_check['detail'],
                })

        # ── Composite Score ─────────────────────────────────
        num_assessed = sum(1 for c in checks if c['pass'] is not None)
        num_skipped = len(checks) - num_assessed

        if max_score == 0:
            # All checks skipped — no data available at all
            forensic_score = None
            quality_rating = 'INSUFFICIENT_DATA'
            quality_label = 'Insufficient data — forensic assessment not possible'
        else:
            forensic_score = round(total_score / max_score * 10, 1)
            # Rating tiers from natural quartiles of the 0–10 scale:
            # top quartile (>7.5)=HIGH, next (>5.0)=MODERATE,
            # next (>2.5)=LOW, bottom=VERY_LOW
            if forensic_score >= 7.5:
                quality_rating = 'HIGH'
                quality_label = 'High Earnings Quality — financials appear genuine'
            elif forensic_score >= 5.0:
                quality_rating = 'MODERATE'
                quality_label = 'Moderate Earnings Quality — some concerns noted'
            elif forensic_score >= 2.5:
                quality_rating = 'LOW'
                quality_label = 'Low Earnings Quality — multiple red flags'
            else:
                quality_rating = 'VERY_LOW'
                quality_label = 'Very Low Earnings Quality — potential manipulation'

            if num_skipped > 0:
                quality_label += f' ({num_skipped} check(s) skipped due to missing data)'

        # Derive a status string for each check so that consumers
        # (e.g. the report generator) can look up 'status' directly.
        for chk in checks:
            if chk['pass'] is None:
                chk['status'] = 'SKIP'
            elif chk['pass']:
                chk['status'] = 'PASS'
            elif chk.get('is_red_flag'):
                chk['status'] = 'FAIL'
            else:
                chk['status'] = 'WARN'

        return {
            'available': True,
            'forensic_score': forensic_score,
            'max_score': 10,
            'quality_rating': quality_rating,
            'quality_label': quality_label,
            'num_checks': len(checks),
            'num_assessed': num_assessed,
            'num_skipped': num_skipped,
            'num_passed': sum(1 for c in checks if c['pass'] is True),
            'num_red_flags': len(red_flags),
            'checks': checks,
            'red_flags': red_flags,
            'score_raw': total_score,
            'score_max_raw': max_score,
        }

    # ==================================================================
    # Individual Checks
    # ==================================================================
    def _check_cfo_ebitda(self, data: dict, analysis: dict) -> dict:
        """Check CFO/EBITDA conversion ratio."""
        cfo_check = analysis.get('cfo_ebitda_check', {})
        if cfo_check.get('available'):
            ratio = cfo_check.get('conversion_pct', cfo_check.get('ratio'))
            if ratio is None:
                ratio = 0  # conversion_pct should always be present when available
            is_red = cfo_check.get('is_red_flag', False)
            return {
                'name': 'CFO/EBITDA Conversion',
                'value': f'{ratio}%',
                'threshold': 'data-relative',
                'pass': not is_red,
                'is_red_flag': is_red,
                'detail': (f'CFO/EBITDA at {ratio}% — '
                           + (cfo_check.get('assessment', 'below threshold, '
                              'profits may not be backed by real cash') if is_red
                              else 'healthy cash conversion')),
            }
        return {
            'name': 'CFO/EBITDA Conversion',
            'value': 'N/A',
            'threshold': 'data-relative',
            'pass': None,  # SKIP — can't assess without data
            'is_red_flag': False,
            'detail': 'Insufficient data for CFO/EBITDA check',
        }

    def _check_accruals_quality(self, data: dict) -> dict:
        """
        Check total accruals relative to total assets.
        High accruals = earnings driven by accounting rather than cash.
        Threshold: |Accruals/TA| > 10% is concerning.
        """
        pnl = data.get('pnl', pd.DataFrame())
        cf = data.get('cash_flow', pd.DataFrame())
        bs = data.get('balance_sheet', pd.DataFrame())

        if pnl.empty or cf.empty or bs.empty:
            return {
                'name': 'Accruals Quality',
                'value': 'N/A',
                'threshold': '|Accruals/TA| < 10%',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Insufficient data',
            }

        net_profit = get_value(pp.get(pnl, 'net_profit'))
        cfo = get_value(pp.get(cf, 'operating_cf'))
        total_assets = get_value(pp.get(bs, 'total_assets'))

        if any(np.isnan(v) for v in [net_profit, cfo, total_assets]):
            return {
                'name': 'Accruals Quality',
                'value': 'N/A',
                'threshold': '|Accruals/TA| < 10%',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Missing data for accruals calculation',
            }

        if total_assets <= 0:
            return {
                'name': 'Accruals Quality',
                'value': 'N/A',
                'threshold': '|Accruals/TA| < 10%',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Invalid total assets',
            }

        # Total Accruals = Net Income - CFO
        accruals = net_profit - cfo
        accruals_ratio = abs(accruals / total_assets) * 100

        is_red = accruals_ratio > 10
        return {
            'name': 'Accruals Quality',
            'value': f'{accruals_ratio:.1f}%',
            'threshold': '|Accruals/TA| < 10%',
            'pass': not is_red,
            'is_red_flag': is_red,
            'detail': (f'Accruals/Total Assets = {accruals_ratio:.1f}% — '
                       + ('HIGH accruals suggest earnings may not be '
                          'supported by cash flow' if is_red
                          else 'within acceptable range')),
        }

    def _check_revenue_receivables(self, data: dict) -> dict:
        """
        Check if receivables are growing faster than revenue.
        If debtor days are increasing significantly, it may indicate
        channel stuffing or aggressive revenue recognition.
        """
        pnl = data.get('pnl', pd.DataFrame())
        ratios_df = data.get('ratios', pd.DataFrame())

        if pnl.empty:
            return {
                'name': 'Revenue-Receivables Divergence',
                'value': 'N/A',
                'threshold': 'Debtor days stable/declining',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Insufficient data',
            }

        # Check debtor days trend
        debtor_days = pp.get(ratios_df, 'debtor_days') if not ratios_df.empty else pd.Series()
        if len(debtor_days.dropna()) >= 2:
            latest_dd = float(debtor_days.dropna().iloc[-1])
            prev_dd = float(debtor_days.dropna().iloc[-2])
            increase = latest_dd - prev_dd

            is_red = increase > 15  # >15 day increase is concerning
            return {
                'name': 'Revenue-Receivables Divergence',
                'value': f'{latest_dd:.0f} days (Δ{increase:+.0f})',
                'threshold': 'Δ Debtor Days < +15',
                'pass': not is_red,
                'is_red_flag': is_red,
                'detail': (f'Debtor days changed by {increase:+.0f} days — '
                           + ('significant increase suggests potential channel '
                              'stuffing or collection issues' if is_red
                              else 'stable receivables collection')),
            }

        # Revenue growth vs receivables growth (from P&L)
        sales = pp.get(pnl, 'sales')
        if len(sales.dropna()) >= 2:
            rev_growth = (get_value(sales, -1) / get_value(sales, -2) - 1) * 100
            return {
                'name': 'Revenue-Receivables Divergence',
                'value': f'Rev Growth {rev_growth:+.1f}%',
                'threshold': 'Debtor days stable',
                'pass': True,
                'is_red_flag': False,
                'detail': 'Debtor days data not available for full check',
            }

        return {
            'name': 'Revenue-Receivables Divergence',
            'value': 'N/A',
            'threshold': 'Debtor days stable',
            'pass': None,  # SKIP — can't assess without data
            'is_red_flag': False,
            'detail': 'Insufficient data for divergence check',
        }

    def _check_rpt(self, analysis: dict) -> dict:
        """Check Related Party Transaction exposure."""
        rpt = analysis.get('rpt', {})
        if rpt.get('available'):
            severity = rpt.get('severity', 'UNKNOWN')
            pct = rpt.get('rpt_as_pct_revenue')
            is_red = severity in ('HIGH', 'CRITICAL')
            return {
                'name': 'Related Party Transactions',
                'value': f'{pct}% of revenue' if pct is not None else 'Detected',
                'threshold': 'RPT < 25% of revenue',
                'pass': not is_red,
                'is_red_flag': is_red,
                'severity': severity,
                'detail': rpt.get('flag', 'RPT analyzed'),
            }
        return {
            'name': 'Related Party Transactions',
            'value': 'Not found in AR',
            'threshold': 'RPT < 25% of revenue',
            'pass': None,  # SKIP — can't assess without data
            'is_red_flag': False,
            'detail': 'RPT section not detected in annual report',
        }

    def _check_contingent(self, analysis: dict) -> dict:
        """Check contingent liabilities exposure."""
        cl = analysis.get('contingent', {})
        if cl.get('available'):
            severity = cl.get('severity', 'UNKNOWN')
            pct = cl.get('contingent_as_pct_networth')
            is_red = severity in ('HIGH', 'CRITICAL')
            return {
                'name': 'Contingent Liabilities',
                'value': f'{pct}% of net worth' if pct is not None else 'Detected',
                'threshold': 'CL < 20% of net worth',
                'pass': not is_red,
                'is_red_flag': is_red,
                'severity': severity,
                'detail': cl.get('detail', 'Contingent liabilities analyzed'),
            }
        return {
            'name': 'Contingent Liabilities',
            'value': 'Not found in AR',
            'threshold': 'CL < 20% of net worth',
            'pass': None,  # SKIP — can't assess without data
            'is_red_flag': False,
            'detail': 'Contingent liabilities section not detected',
        }

    def _check_cfo_trend(self, data: dict) -> dict:
        """
        Check if CFO has been consistently positive over 3+ years.
        Persistent negative CFO despite positive PAT is a major red flag.
        """
        cf = data.get('cash_flow', pd.DataFrame())
        pnl = data.get('pnl', pd.DataFrame())

        if cf.empty or pnl.empty:
            return {
                'name': 'Cash Flow Trend',
                'value': 'N/A',
                'threshold': 'CFO positive when PAT positive',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Insufficient data',
            }

        cfo = pp.get(cf, 'operating_cf').dropna()
        pat = pp.get(pnl, 'net_profit').dropna()

        if len(cfo) < 3 or len(pat) < 3:
            return {
                'name': 'Cash Flow Trend',
                'value': 'N/A',
                'threshold': 'CFO positive when PAT positive',
                'pass': None,  # SKIP — can't assess without data
                'is_red_flag': False,
                'detail': 'Less than 3 years of data',
            }

        # Check last 3 years: if PAT > 0 but CFO < 0, flag it
        divergence_years = 0
        for i in range(-3, 0):
            try:
                p = float(pat.iloc[i])
                c = float(cfo.iloc[i])
                if p > 0 and c < 0:
                    divergence_years += 1
            except (IndexError, ValueError):
                pass

        is_red = divergence_years >= 2
        return {
            'name': 'Cash Flow Trend',
            'value': f'{divergence_years}/3 years PAT+/CFO-',
            'threshold': '< 2 divergence years',
            'pass': not is_red,
            'is_red_flag': is_red,
            'detail': (f'{divergence_years} of last 3 years show positive PAT '
                       'but negative CFO — ' +
                       ('persistent disconnect between reported profits and '
                        'actual cash generation' if is_red
                        else 'cash flow supports reported earnings')),
        }
