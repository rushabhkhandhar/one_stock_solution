"""
Forensic Extras — RPT, Contingent Liabilities, Auditor Red Flags
=================================================================
Structured extraction from Annual Report parsed data.

Master Plan Items:
  2a — Related Party Transactions (RPT as % of revenue)
  2b — Contingent Liabilities (as % of net worth)
  2c — Auditor Qualifications / Emphasis of Matter
"""
import re
import numpy as np


class ForensicExtras:
    """Extract structured forensic intelligence from parsed AR data."""

    # ------------------------------------------------------------------
    # 2a — Related Party Transactions
    # ------------------------------------------------------------------
    def extract_rpt(self, ar_parsed: dict, data: dict) -> dict:
        """
        Structured RPT extraction from annual report.

        Flags:
          - RPT > 10% of revenue → MEDIUM
          - RPT > 25% of revenue → HIGH
          - RPT > 50% of revenue → CRITICAL
        """
        rpt_text = ar_parsed.get('related_party_summary', '')
        if not rpt_text:
            return {'available': False, 'reason': 'No RPT section found in AR'}

        # Extract monetary amounts from RPT text
        amounts = self._extract_amounts(rpt_text)
        total_rpt = sum(amounts) if amounts else 0

        # Get revenue for comparison
        pnl = data.get('pnl')
        revenue = np.nan
        if pnl is not None and not pnl.empty:
            from data.preprocessing import DataPreprocessor, get_value
            pp = DataPreprocessor()
            revenue = get_value(pp.get(pnl, 'sales'))

        rpt_pct = None
        if total_rpt > 0 and not np.isnan(revenue) and revenue > 0:
            rpt_pct = round(total_rpt / revenue * 100, 2)

        # Classify severity
        if rpt_pct is not None:
            if rpt_pct > 50:
                severity = 'CRITICAL'
                flag = f'🔴 RPT at {rpt_pct}% of revenue — excessive related party exposure'
            elif rpt_pct > 25:
                severity = 'HIGH'
                flag = f'🟠 RPT at {rpt_pct}% of revenue — significant related party dependence'
            elif rpt_pct > 10:
                severity = 'MEDIUM'
                flag = f'🟡 RPT at {rpt_pct}% of revenue — monitor closely'
            else:
                severity = 'LOW'
                flag = f'🟢 RPT at {rpt_pct}% of revenue — within normal range'
        else:
            severity = 'UNKNOWN'
            flag = 'RPT amounts could not be quantified from AR text'

        # Extract RPT categories
        categories = self._parse_rpt_categories(rpt_text)

        return {
            'available': True,
            'total_rpt_amount': total_rpt,
            'rpt_as_pct_revenue': rpt_pct,
            'severity': severity,
            'flag': flag,
            'num_amounts_found': len(amounts),
            'categories': categories,
            'raw_text_preview': rpt_text[:500] if len(rpt_text) > 500 else rpt_text,
        }

    # ------------------------------------------------------------------
    # 2b — Contingent Liabilities Analysis
    # ------------------------------------------------------------------
    def analyze_contingent(self, ar_parsed: dict, data: dict) -> dict:
        """
        Analyze contingent liabilities from AR.

        Flag if contingent > 20% of net worth.
        """
        cl_text = ar_parsed.get('contingent_liabilities', '')
        if not cl_text:
            return {'available': False, 'reason': 'No contingent liabilities section'}

        amounts = self._extract_amounts(cl_text)
        total_cl = sum(amounts) if amounts else 0

        # Net worth = Equity Capital + Reserves
        bs = data.get('balance_sheet')
        net_worth = np.nan
        if bs is not None and not bs.empty:
            from data.preprocessing import DataPreprocessor, get_value
            pp = DataPreprocessor()
            eq = get_value(pp.get(bs, 'equity_capital'))
            res = get_value(pp.get(bs, 'reserves'))
            if not np.isnan(eq) and not np.isnan(res):
                net_worth = eq + res

        cl_pct = None
        if total_cl > 0 and not np.isnan(net_worth) and net_worth > 0:
            cl_pct = round(total_cl / net_worth * 100, 2)

        # ── Sanity bound: if extracted CL > 150% of net worth,
        #    it's almost certainly a text-extraction artefact
        #    (e.g. picking up unrelated numbers from AR text).
        #    Flag as unreliable rather than reporting a hallucinated figure.
        if cl_pct is not None and cl_pct > 150:
            return {
                'available': True,
                'total_contingent': total_cl,
                'contingent_as_pct_networth': None,  # suppress the hallucinated %
                'severity': 'DATA_QUALITY',
                'data_quality_issue': True,
                'detail': (f'Extracted contingent amount (₹{total_cl:,.0f} Cr, '
                           f'{cl_pct:.0f}% of net worth) exceeds plausibility '
                           'bound — likely a text-extraction error. '
                           'Could not quantify contingent liabilities reliably.'),
            }

        if cl_pct is not None:
            if cl_pct > 50:
                severity = 'CRITICAL'
            elif cl_pct > 20:
                severity = 'HIGH'
            elif cl_pct > 5:
                severity = 'MEDIUM'
            else:
                severity = 'LOW'
        else:
            severity = 'UNKNOWN'

        return {
            'available': True,
            'total_contingent': total_cl,
            'contingent_as_pct_networth': cl_pct,
            'severity': severity,
            'detail': (f'Contingent liabilities ₹{total_cl:,.0f} Cr '
                       f'({cl_pct}% of net worth)'
                       if cl_pct is not None
                       else f'Contingent liabilities: {len(amounts)} items found'),
        }

    # ------------------------------------------------------------------
    # 2c — Auditor Red Flags Summary
    # ------------------------------------------------------------------
    def summarize_auditor_flags(self, ar_parsed: dict) -> dict:
        """
        Summarize auditor observations into severity-tagged list.
        """
        observations = ar_parsed.get('auditor_observations', [])
        if not observations:
            return {'available': False, 'reason': 'No auditor observations'}

        HIGH_KEYWORDS = [
            'qualification', 'qualified', 'adverse', 'disclaimer',
            'going concern', 'material misstatement', 'material weakness',
            'non-compliance', 'departure',
        ]
        MEDIUM_KEYWORDS = [
            'emphasis of matter', 'key audit matter', 'except for',
            'material uncertainty',
        ]

        flags = []
        for obs in observations:
            context = obs.get('context', '').lower()
            obs_type = obs.get('type', '')

            if any(kw in context for kw in HIGH_KEYWORDS):
                severity = 'HIGH'
            elif any(kw in context for kw in MEDIUM_KEYWORDS):
                severity = 'MEDIUM'
            else:
                severity = 'LOW'

            flags.append({
                'severity': severity,
                'observation': obs.get('context', '')[:300],
                'type': obs_type,
                'page': obs.get('page'),
            })

        # Sort by severity
        sev_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        flags.sort(key=lambda x: sev_order.get(x['severity'], 3))

        has_critical = any(f['severity'] == 'HIGH' for f in flags)

        return {
            'available': True,
            'flags': flags,
            'total_observations': len(flags),
            'has_critical_flags': has_critical,
            'summary': (
                '🔴 CRITICAL auditor flags detected — review immediately'
                if has_critical
                else f'✔ {len(flags)} auditor observations, none critical'
            ),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_amounts(text: str) -> list:
        """Extract monetary amounts (in Crores) from text."""
        # Pattern: numbers with optional commas, optional decimal
        # May be preceded by ₹ or Rs or followed by "crore" / "lakhs"
        amounts = []

        # Pattern for crore amounts
        crore_pat = re.compile(
            r'(?:₹|Rs\.?\s*)?'
            r'([\d,]+(?:\.\d+)?)\s*'
            r'(?:crore|cr\.?|crores)',
            re.IGNORECASE
        )
        for m in crore_pat.finditer(text):
            try:
                val = float(m.group(1).replace(',', ''))
                if val > 0:
                    amounts.append(val)
            except ValueError:
                pass

        # Pattern for lakh amounts (convert to crore)
        lakh_pat = re.compile(
            r'(?:₹|Rs\.?\s*)?'
            r'([\d,]+(?:\.\d+)?)\s*'
            r'(?:lakh|lac|lakhs)',
            re.IGNORECASE
        )
        for m in lakh_pat.finditer(text):
            try:
                val = float(m.group(1).replace(',', '')) / 100
                if val > 0:
                    amounts.append(val)
            except ValueError:
                pass

        # If no unit-annotated amounts, try plain large numbers
        if not amounts:
            plain_pat = re.compile(r'([\d,]+(?:\.\d+)?)')
            for m in plain_pat.finditer(text):
                try:
                    val = float(m.group(1).replace(',', ''))
                    if val > 100:  # Only large numbers likely in Cr
                        amounts.append(val)
                except ValueError:
                    pass

        return amounts

    @staticmethod
    def _parse_rpt_categories(text: str) -> list:
        """Parse RPT text into categories."""
        categories = []
        rpt_keywords = [
            'subsidiary', 'associate', 'joint venture', 'key management',
            'director', 'promoter', 'holding company', 'fellow subsidiary',
            'enterprise', 'trust', 'relative',
        ]
        for kw in rpt_keywords:
            if kw in text.lower():
                categories.append(kw.title())
        return categories
