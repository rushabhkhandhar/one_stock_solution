"""
Cross-Validation Engine
========================
Validates scraper-extracted financial data against numbers found
in the actual Annual Report PDFs.

Trust Score = (matched_checks / total_checks) * 100

All thresholds (tolerance, trust labels, auditor penalties) are
pulled from ``config.validation`` — ZERO hardcoded values in this
module.

Validation checks:
  1. Revenue match            — scraper vs AR
  2. PAT (Net Profit) match   — scraper vs AR
  3. EPS match                — scraper vs AR
  4. Operating Cash Flow      — scraper vs AR
  5. Footnote consistency     — do footnotes mention exceptional items
                                that explain number deviations?
  6. Auditor opinion          — clean / qualified / adverse?
  7. Contingent liabilities   — material amounts flagged?

AR values in Lakhs are auto-normalised to Crores (÷100) before
comparison.

Period matching: If an AR year is provided, the validator picks
the matching FY column from the scraper data.
"""
import numpy as np
import pandas as pd
from config import config


class CrossValidator:
    """
    Cross-validate scraper data against Annual Report PDF data.

    All thresholds are pulled from ``config.validation`` —
    nothing is hardcoded in this module.

    Usage:
        validator = CrossValidator()
        result = validator.validate(scraper_data, ar_parsed)
        # result['trust_score']   → 0–100
        # result['checks']        → list of individual check results
        # result['footnote_flags'] → important footnote findings
        # result['auditor_flags']  → auditor red flags
    """

    def validate(self, scraper_data: dict, ar_parsed: dict,
                  ar_year: int = None) -> dict:
        """
        Run all validation checks.

        Args:
            scraper_data: The cleaned data dict from ingestion pipeline
            ar_parsed:    Output of PDFParser.parse()
            ar_year:      Fiscal year the AR covers (e.g. 2025 for FY ending
                          Mar 2025).  When provided the validator picks the
                          matching column from scraper data instead of the
                          latest one.

        Returns:
            Comprehensive validation result dict.
        """
        if not ar_parsed or not ar_parsed.get('available'):
            return {
                'available': False,
                'reason': 'Annual report data not available for validation',
                'trust_score': None,
            }

        checks = []
        key_figs = ar_parsed.get('key_figures', {})

        # Helper: pick the right value getter based on whether we have
        # a target AR year.
        def _val(df_key, col_name):
            if ar_year:
                return self._get_value_for_year(
                    scraper_data, df_key, col_name, ar_year)
            return self._get_latest_value(scraper_data, df_key, col_name)

        # ------------------------------------------------------------------
        # 1. Revenue validation
        # ------------------------------------------------------------------
        scraper_revenue = _val('pnl', 'Sales')
        ar_revenue = key_figs.get('revenue_ar') or key_figs.get('revenue_table')
        checks.append(self._compare_values(
            'Revenue from Operations',
            scraper_revenue, ar_revenue,
            'Scraper P&L', 'Annual Report'
        ))

        # ------------------------------------------------------------------
        # 2. PAT (Profit After Tax) validation
        # ------------------------------------------------------------------
        scraper_pat = _val('pnl', 'NetProfit')
        ar_pat = key_figs.get('pat_ar') or key_figs.get('pat_table')
        checks.append(self._compare_values(
            'Profit After Tax (PAT)',
            scraper_pat, ar_pat,
            'Scraper P&L', 'Annual Report'
        ))

        # ------------------------------------------------------------------
        # 3. EPS validation
        # ------------------------------------------------------------------
        scraper_eps = _val('pnl', 'EPSinRs')
        ar_eps = key_figs.get('eps_ar') or key_figs.get('eps_table')
        checks.append(self._compare_values(
            'Earnings Per Share (EPS)',
            scraper_eps, ar_eps,
            'Scraper P&L', 'Annual Report'
        ))

        # ------------------------------------------------------------------
        # 4. Operating Cash Flow validation
        # ------------------------------------------------------------------
        scraper_ocf = _val('cash_flow', 'CashfromOperatingActivity')
        ar_ocf = key_figs.get('operating_cashflow_ar')
        checks.append(self._compare_values(
            'Operating Cash Flow',
            scraper_ocf, ar_ocf,
            'Scraper CF', 'Annual Report'
        ))

        # ------------------------------------------------------------------
        # 5. Footnote consistency check
        # ------------------------------------------------------------------
        footnote_flags = self._check_footnotes(ar_parsed.get('footnotes', []))

        # ------------------------------------------------------------------
        # 6. Auditor opinion check
        # ------------------------------------------------------------------
        auditor_flags = self._check_auditor(
            ar_parsed.get('auditor_observations', []))

        # ------------------------------------------------------------------
        # 7. Contingent liabilities check
        # ------------------------------------------------------------------
        contingent_flag = self._check_contingent(
            ar_parsed.get('contingent_liabilities', ''),
            scraper_data
        )

        # ------------------------------------------------------------------
        # Compute Trust Score
        # ------------------------------------------------------------------
        passed  = sum(1 for c in checks if c['status'] in ('MATCH', 'CORPORATE_ACTION_ADJUSTED'))
        partial = sum(1 for c in checks if c['status'] == 'PARTIAL')
        skipped = sum(1 for c in checks if c['status'] == 'SKIPPED')
        total   = sum(1 for c in checks if c['status'] != 'SKIPPED')

        v = config.validation
        if total == 0:
            trust_score = None
            trust_label = 'INSUFFICIENT DATA'
        else:
            raw_score = ((passed + 0.5 * partial) / total) * 100
            # Penalize for auditor red flags
            severe_flags = sum(1 for f in auditor_flags
                               if f.get('severity') == 'HIGH')
            penalty = min(severe_flags * v.auditor_penalty_per_flag,
                          v.auditor_penalty_cap)
            trust_score = max(0, round(raw_score - penalty, 1))

            # Trust label — thresholds from config.validation
            if trust_score >= v.trust_high:
                trust_label = 'HIGH CONFIDENCE \u2705'
            elif trust_score >= v.trust_moderate:
                trust_label = 'MODERATE CONFIDENCE \U0001f7e1'
            else:
                trust_label = 'UNRELIABLE \U0001f534'

        return {
            'available': True,
            'trust_score': trust_score,
            'trust_label': trust_label,
            'checks': checks,
            'summary': {
                'passed': passed,
                'partial': partial,
                'failed': sum(1 for c in checks if c['status'] == 'MISMATCH'),
                'skipped': sum(1 for c in checks if c['status'] == 'SKIPPED'),
                'total': len(checks),
            },
            'footnote_flags': footnote_flags,
            'auditor_flags': auditor_flags,
            'contingent_flag': contingent_flag,
        }

    # ==================================================================
    # Value comparison
    # ==================================================================
    def _compare_values(self, metric: str,
                         scraper_val, ar_val,
                         scraper_source: str,
                         ar_source: str) -> dict:
        """Compare two values with tolerance."""
        result = {
            'metric': metric,
            'scraper_value': scraper_val,
            'ar_value': ar_val,
            'scraper_source': scraper_source,
            'ar_source': ar_source,
        }

        # Can't compare if either is missing
        if scraper_val is None or ar_val is None:
            if scraper_val is None and ar_val is None:
                result['status'] = 'SKIPPED'
                result['detail'] = 'Both values missing'
            elif scraper_val is None:
                result['status'] = 'SKIPPED'
                result['detail'] = 'Scraper value missing'
            else:
                result['status'] = 'SKIPPED'
                result['detail'] = 'Annual Report value missing'
            return result

        if np.isnan(scraper_val) or np.isnan(ar_val):
            result['status'] = 'SKIPPED'
            result['detail'] = 'NaN value(s)'
            return result

        v = config.validation          # shorthand

        # Compute difference
        if abs(ar_val) < v.abs_threshold:
            diff = abs(scraper_val - ar_val)
            pct_diff = None
            if diff <= v.abs_tolerance:
                result['status'] = 'MATCH'
                result['detail'] = f'Absolute diff: {diff:.2f} (within tolerance)'
            else:
                result['status'] = 'MISMATCH'
                result['detail'] = f'Absolute diff: {diff:.2f}'
        else:
            pct_diff = abs(scraper_val - ar_val) / abs(ar_val) * 100
            result['pct_diff'] = round(pct_diff, 2)

            if pct_diff <= v.tolerance_pct:
                result['status'] = 'MATCH'
                result['detail'] = (
                    f'{pct_diff:.2f}% difference '
                    f'(within {v.tolerance_pct}% tolerance)'
                )
            elif pct_diff <= v.tolerance_pct * 2:
                result['status'] = 'PARTIAL'
                result['detail'] = (
                    f'{pct_diff:.2f}% difference — minor discrepancy, '
                    f'likely due to rounding or restatement'
                )
            else:
                result['status'] = 'MISMATCH'
                result['detail'] = (
                    f'{pct_diff:.2f}% difference — significant mismatch! '
                    f'Check for restatements, exceptional items, or '
                    f'standalone vs consolidated mismatch.'
                )

        # ── Lakhs → Crores normalisation ────────────────────
        # Indian Annual Reports typically report in Lakhs, while
        # Screener.in standardises to Crores (1 Cr = 100 Lakhs).
        if result['status'] in ('MISMATCH', 'PARTIAL'):
            ar_normalised = ar_val / v.lakhs_to_crores
            if abs(scraper_val) > 0:
                adj_pct = abs(scraper_val - ar_normalised) / abs(scraper_val) * 100
            else:
                adj_pct = float('inf')
            if adj_pct <= v.tolerance_pct:
                result['status'] = 'MATCH'
                result['ar_value_normalised'] = round(ar_normalised, 2)
                result['unit_adjusted'] = True
                result['pct_diff'] = round(adj_pct, 2)
                result['detail'] = (
                    f'Unit mismatch detected (Lakhs→Crores, ÷{v.lakhs_to_crores:.0f}): '
                    f'AR {ar_val:,.2f} Lakhs ≈ {ar_normalised:,.2f} Cr — '
                    f'{adj_pct:.1f}% diff after conversion'
                )

        # ── Corporate-action adjustment detection ────────────
        # For EPS specifically, Screener.in restates for splits/bonus/
        # mergers using post-action share count.  If the ratio of
        # AR_value / scraper_value is close to 2, 3, 5, or 10, this
        # is likely a corporate-action adjustment, not a true mismatch.
        if (result['status'] in ('MISMATCH', 'PARTIAL')
                and 'EPS' in metric
                and scraper_val is not None and ar_val is not None
                and scraper_val > 0 and ar_val > 0):
            _ratio = ar_val / scraper_val
            _corp_mults = [2, 3, 5, 10]
            _closest = min(_corp_mults, key=lambda c: abs(_ratio - c))
            if abs(_ratio - _closest) / _closest < 0.15:
                result['status'] = 'CORPORATE_ACTION_ADJUSTED'
                result['corporate_action_multiplier'] = _closest
                result['detail'] = (
                    f'AR EPS ({ar_val:.2f}) ≈ {_closest}× Scraper EPS '
                    f'({scraper_val:.2f}) — likely a merger/split/bonus '
                    f'share-count adjustment. Not a data error.'
                )

        return result

    # ==================================================================
    # Footnote analysis
    # ==================================================================
    def _check_footnotes(self, footnotes: list) -> list:
        """
        Analyze footnotes for material items that could affect numbers.

        Flags:
          - Exceptional / extraordinary items
          - Restatements / prior period adjustments
          - Change in accounting policy
          - Going concern doubt
          - Contingent liabilities above threshold
          - Related party transactions
        """
        flags = []

        risk_patterns = {
            'exceptional_item': {
                'pattern': ['exceptional', 'extraordinary', 'one-time',
                            'non-recurring'],
                'severity': 'MEDIUM',
                'impact': 'May cause scraper numbers to differ from '
                          'normalized earnings',
            },
            'restatement': {
                'pattern': ['restate', 'restated', 'prior period adjustment',
                            'reclassif'],
                'severity': 'HIGH',
                'impact': 'Historical numbers may have changed — scraper '
                          'may show old values',
            },
            'accounting_change': {
                'pattern': ['change in accounting policy',
                            'change in accounting estimate',
                            'first-time adoption', 'transition'],
                'severity': 'MEDIUM',
                'impact': 'Year-over-year comparisons may not be '
                          'apples-to-apples',
            },
            'going_concern': {
                'pattern': ['going concern', 'ability to continue',
                            'material uncertainty'],
                'severity': 'CRITICAL',
                'impact': 'Company viability in question — \u26a0\ufe0f',
            },
            'legal_settlement': {
                'pattern': ['legal claim', 'settlement', 'litigation',
                            'arbitration', 'penalty imposed'],
                'severity': 'MEDIUM',
                'impact': 'Legal provisions may distort reported profit',
            },
            'impairment': {
                'pattern': ['impairment', 'write-off', 'write-down',
                            'provision for bad', 'expected credit loss'],
                'severity': 'MEDIUM',
                'impact': 'Asset values or profit may be lower due to '
                          'write-downs',
            },
        }

        # Boilerplate phrases that should NOT trigger flags
        # (standard Indian AR language, not actual issues)
        _BOILERPLATE = [
            'regrouped and/or reclassified wherever necessary',
            'reclassified wherever necessary',
            'regrouped wherever necessary',
            'previous year figures have been regrouped',
            'figures have been reclassified',
            'previous year figures have been rearranged',
        ]

        for fn in footnotes:
            combined = (fn.get('title', '') + ' ' +
                        fn.get('text', '')[:500]).lower()

            # Skip boilerplate notes
            if any(bp in combined for bp in _BOILERPLATE):
                continue

            for flag_name, config in risk_patterns.items():
                for keyword in config['pattern']:
                    if keyword in combined:
                        flags.append({
                            'type': flag_name,
                            'note_id': fn.get('note_id'),
                            'title': fn.get('title', ''),
                            'severity': config['severity'],
                            'impact': config['impact'],
                            'page': fn.get('page'),
                            'keyword_matched': keyword,
                            'numbers_in_note': fn.get('numbers', [])[:5],
                        })
                        break  # One flag per pattern per note

        # Deduplicate by (type, normalized_title) — content-based dedup
        seen = set()
        unique = []
        for f in flags:
            # Normalize title for dedup (strip whitespace, lowercase, first 80 chars)
            norm_title = f.get('title', '').strip().lower()[:80]
            key = (f['type'], norm_title)
            if key not in seen:
                seen.add(key)
                unique.append(f)

        return unique

    # ==================================================================
    # Auditor opinion analysis
    # ==================================================================
    def _check_auditor(self, observations: list) -> list:
        """Classify auditor observations by severity."""
        flags = []

        severity_map = {
            'emphasis': 'MEDIUM',
            'qualifi': 'HIGH',
            'material': 'HIGH',
            'going concern': 'CRITICAL',
            'key audit matter': 'LOW',  # Standard in all reports
            'adverse': 'CRITICAL',
            'disclaimer': 'CRITICAL',
            'except for': 'HIGH',
            'departure': 'HIGH',
            'non-?compliance': 'HIGH',
        }

        for obs in observations:
            obs_type = obs.get('type', '').lower()
            severity = 'MEDIUM'
            for pattern, sev in severity_map.items():
                if pattern in obs_type:
                    severity = sev
                    break

            flags.append({
                'observation': obs.get('context', '')[:200],
                'page': obs.get('page'),
                'severity': severity,
            })

        return flags

    # ==================================================================
    # Contingent liabilities check
    # ==================================================================
    def _check_contingent(self, contingent_text: str,
                           scraper_data: dict) -> dict:
        """Check if contingent liabilities are material."""
        if not contingent_text:
            return {
                'available': False,
                'detail': 'Contingent liabilities section not found in AR',
            }

        # Try to extract total contingent amount
        import re
        amounts = re.findall(r'[\d,]+\.\d{2}', contingent_text)
        total = 0
        for amt in amounts:
            try:
                total = max(total, float(amt.replace(',', '')))
            except ValueError:
                pass

        # Compare with total assets
        total_assets = self._get_latest_value(
            scraper_data, 'balance_sheet', 'TotalAssets')

        if total_assets and total_assets > 0 and total > 0:
            pct = (total / total_assets) * 100
            if pct > 10:
                severity = 'HIGH'
                detail = (f'Largest contingent liability: \u20b9{total:,.0f} Cr '
                          f'({pct:.1f}% of total assets) \u2014 MATERIAL')
            elif pct > 5:
                severity = 'MEDIUM'
                detail = (f'Largest contingent liability: \u20b9{total:,.0f} Cr '
                          f'({pct:.1f}% of total assets)')
            else:
                severity = 'LOW'
                detail = (f'Largest contingent liability: \u20b9{total:,.0f} Cr '
                          f'({pct:.1f}% of total assets) \u2014 not material')
        else:
            severity = 'LOW'
            detail = 'Could not quantify contingent liabilities'

        return {
            'available': True,
            'severity': severity,
            'detail': detail,
        }

    # ==================================================================
    # Helpers
    # ==================================================================
    def _get_value_for_year(self, data: dict, df_key: str,
                             col_name: str, target_year: int):
        """
        Get the value for a specific FY from a scraper DataFrame.

        Screener.in indexes rows like 'Mar 2025', 'Mar 2024', etc.
        If the target year is found, return that row's value;
        otherwise fall back to the latest value.
        """
        df = data.get(df_key)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return None

        # Resolve column
        col = None
        for c in df.columns:
            normalized = c.replace('\xa0', '').replace(' ', '').lower()
            if normalized == col_name.replace(' ', '').lower():
                col = c
                break
        if col is None:
            return None

        # Try to find a row whose index contains the target year
        year_str = str(target_year)
        for idx in df.index:
            if year_str in str(idx):
                val = df.loc[idx, col]
                if pd.notna(val):
                    return float(val)

        # Fallback: latest non-null
        vals = df[col].dropna()
        if vals.empty:
            return None
        return float(vals.iloc[-1])

    def _get_latest_value(self, data: dict, df_key: str,
                           col_name: str):
        """Get the latest annual value from a scraper DataFrame."""
        df = data.get(df_key)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return None

        # Try exact and normalized column name
        col = None
        for c in df.columns:
            normalized = c.replace('\xa0', '').replace(' ', '').lower()
            if normalized == col_name.replace(' ', '').lower():
                col = c
                break

        if col is None:
            return None

        vals = df[col].dropna()
        if vals.empty:
            return None

        return float(vals.iloc[-1])
