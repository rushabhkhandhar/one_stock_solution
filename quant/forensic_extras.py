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
    def extract_rpt(self, ar_parsed: dict, data: dict,
                     sotp_available: bool = False,
                     num_segments: int = 0) -> dict:
        """
        Structured RPT extraction from annual report.

        Flags:
          - RPT > 10% of revenue → MEDIUM
          - RPT > 25% of revenue → HIGH
          - RPT > 50% of revenue → CRITICAL

        Rule 3: If the company has SOTP/multi-segment structure (i.e.
        holding company or conglomerate), high RPT is partially expected
        due to inter-subsidiary transfers. Severity is contextualized.
        """
        rpt_text = ar_parsed.get('related_party_summary', '')
        if not rpt_text:
            return {'available': False, 'reason': 'No RPT section found in AR'}

        # Scope the text to avoid AGM authorization limits that
        # dwarf actual RPT figures (e.g. "up to ₹9,80,136 crore")
        rpt_text_scoped = self._scope_rpt_text(rpt_text)

        # Extract monetary amounts from RPT text
        amounts = self._extract_amounts(rpt_text_scoped)
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

        # Detect holding-company / conglomerate structure
        _is_holding = sotp_available or num_segments >= 3

        # Classify severity
        if rpt_pct is not None:
            if rpt_pct > 50:
                if _is_holding:
                    severity = 'HIGH_HOLDING'
                    flag = (f'🟠 RPT at {rpt_pct}% of revenue — '
                            f'elevated, but company operates as a '
                            f'multi-entity holding structure '
                            f'({num_segments} segments). Inter-subsidiary '
                            f'transfers are partially structural; '
                            f'verify arm\'s-length pricing.')
                else:
                    severity = 'CRITICAL'
                    flag = f'🔴 RPT at {rpt_pct}% of revenue — excessive related party exposure'
            elif rpt_pct > 25:
                if _is_holding:
                    severity = 'MEDIUM_HOLDING'
                    flag = (f'🟡 RPT at {rpt_pct}% of revenue — '
                            f'within normal range for holding/'
                            f'conglomerate structures with '
                            f'{num_segments} segments.')
                else:
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
            'is_holding_structure': _is_holding,
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

        # ── Smart scoping: if the extracted text contains a full
        # Balance Sheet (common — PDF parser pulls the entire page
        # that mentions "contingent liabilities"), narrow down to
        # only the CL line and its neighbouring numbers.
        cl_text_scoped = self._scope_contingent_text(cl_text)

        amounts = self._extract_amounts(cl_text_scoped)
        total_cl = sum(amounts) if amounts else 0

        # Net worth = Equity Capital + Reserves
        bs = data.get('balance_sheet')
        net_worth = np.nan
        total_assets = np.nan
        if bs is not None and not bs.empty:
            from data.preprocessing import DataPreprocessor, get_value
            pp = DataPreprocessor()
            eq = get_value(pp.get(bs, 'equity_capital'))
            res = get_value(pp.get(bs, 'reserves'))
            if not np.isnan(eq) and not np.isnan(res):
                net_worth = eq + res
            ta = get_value(pp.get(bs, 'total_assets'))
            if not np.isnan(ta):
                total_assets = ta

        cl_pct = None
        if total_cl > 0 and not np.isnan(net_worth) and net_worth > 0:
            cl_pct = round(total_cl / net_worth * 100, 2)

        # ── Sanity bounding box (Rule 2) ──────────────────
        # Multiple independent checks — if ANY fires, flag as
        # data-quality issue rather than reporting hallucinated figure.
        _is_implausible = False
        _reason_parts = []

        # Check 1: CL > 150% of net worth
        if cl_pct is not None and cl_pct > 150:
            _is_implausible = True
            _reason_parts.append(
                f'{cl_pct:.0f}% of net worth (> 150% bound)')

        # Check 2: CL > 200% of total assets
        if (total_cl > 0 and not np.isnan(total_assets)
                and total_assets > 0):
            _cl_pct_ta = total_cl / total_assets * 100
            if _cl_pct_ta > 200:
                _is_implausible = True
                _reason_parts.append(
                    f'{_cl_pct_ta:.0f}% of total assets (> 200% bound)')

        # Check 3: CL > 500% of net worth (catastrophic hallucination)
        if cl_pct is not None and cl_pct > 500:
            _is_implausible = True
            _reason_parts.append(
                f'{cl_pct:.0f}% of net worth (> 500% catastrophic bound)')

        if _is_implausible:
            return {
                'available': True,
                'total_contingent': total_cl,
                'contingent_as_pct_networth': None,
                'severity': 'DATA_QUALITY',
                'data_quality_issue': True,
                'detail': (f'Extracted contingent amount (₹{total_cl:,.0f} Cr, '
                           f'{"; ".join(_reason_parts)}) exceeds plausibility '
                           'bound — likely a text-extraction error. '
                           'Manual override required.'),
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
    def _scope_contingent_text(raw_text: str) -> str:
        """Narrow full-page text to just the contingent liabilities region.

        The PDF parser often pulls the entire Balance Sheet page because
        it contains a 'Contingent liabilities' line item.  This method
        finds that line and returns only the surrounding context (a few
        lines before and after) so that _extract_amounts doesn't pick up
        the Balance Sheet totals.

        If the text already looks focused (short, or has a dedicated CL
        heading), it is returned unchanged.
        """
        # If the text is short enough, it's probably already scoped
        if len(raw_text) < 1500:
            return raw_text

        lines = raw_text.split('\n')
        cl_line_idx = None
        for i, line in enumerate(lines):
            if re.search(r'contingent\s+liabilit', line, re.I):
                cl_line_idx = i
                # Prefer a later occurrence (the actual CL note/schedule)
                # over an early TOC reference, but take the first one
                # that appears AFTER a balance-sheet "Total" line.
                break

        if cl_line_idx is None:
            return raw_text

        # Check if this is just a line item on the Balance Sheet page
        # (i.e., the page also has "CAPITAL AND LIABILITIES", "ASSETS",
        # "Total" lines).  In that case, extract just the CL line and
        # a few surrounding lines for context/numbers.
        has_bs_markers = any(
            re.search(r'capital\s+and\s+liabilit|\bASSETS\b|'
                      r'standalone\s+balance\s+sheet|\bTotal\b',
                      l, re.I)
            for l in lines[:cl_line_idx]
        )

        if has_bs_markers:
            # It's a Balance Sheet page — extract ONLY the CL line
            # and a few lines after it (schedule detail / numbers).
            # Do NOT include lines before the CL line (those contain
            # the Balance Sheet Total which is much larger).
            start = cl_line_idx
            end = min(len(lines), cl_line_idx + 8)
            # Also prepend the unit header (first ~5 lines) so
            # _extract_amounts can still detect the reporting unit
            header_lines = lines[:min(8, cl_line_idx)]
            scoped = '\n'.join(header_lines + ['---'] + lines[start:end])
            return scoped

        # Not a Balance Sheet page — return full text
        return raw_text

    @staticmethod
    def _scope_rpt_text(raw_text: str) -> str:
        """Remove AGM-notice authorization blocks from RPT text.

        AGM notices typically contain lines like:
          "RESOLVED THAT ... approval ... to enter into related
           party transactions ... not exceeding ₹9,80,136 crore"
        These are *proposed caps*, not actual RPT amounts.  We strip
        them so that _extract_amounts only sees real figures.
        """
        # Split on page separators (our pdf_parser joins pages with ---)
        pages = re.split(r'\n---\n', raw_text)
        if len(pages) <= 1:
            # Single page — try to strip AGM resolution blocks inline
            # Remove "RESOLVED THAT ... ." blocks
            cleaned = re.sub(
                r'"?RESOLVED\s+(?:THAT|FURTHER).*?(?:\.|")',
                '', raw_text, flags=re.I | re.S)
            return cleaned if len(cleaned) > 200 else raw_text

        # Multiple pages — keep only those that look like Notes, not AGM
        agm_re = re.compile(
            r'ordinary\s+resolution|special\s+resolution'
            r'|approval\s+of\s+.*member'
            r'|resolved\s+that\s+pursuant', re.I)
        notes_pages = []
        other_pages = []
        for pg in pages:
            if agm_re.search(pg):
                continue          # drop AGM pages entirely
            notes_pages.append(pg)

        result = '\n---\n'.join(notes_pages) if notes_pages else raw_text
        return result if len(result) > 200 else raw_text

    @staticmethod
    def _extract_amounts(text: str) -> list:
        """Extract monetary amounts (in Crores) from text.

        Strategy:
        1. Detect the page-level reporting unit from headers
           (e.g. "in ₹ Thousands", "₹ in Lakhs").
        2. Match unit-annotated amounts (crore, lakh) first.
        3. Fallback: use only the LARGEST plain number (not sum of all)
           and apply the detected unit conversion.
        """
        amounts = []

        # ── Detect reporting unit from page header ────────────
        _unit_divisor = 1.0          # default: assume Crores
        _unit_label = 'crores'
        _header_region = text[:800].lower()   # unit info is near top
        # Patterns handle varied Indian AR formats:
        #   "(₹ in Thousands)", "(Amounts in ₹ Thousands)",
        #   "(₹ in '000s)", "(in ₹ Lakhs)", "(Rs. in Crores)"
        #   "(` in '000)" — backtick used for ₹ in some bank ARs
        #   "(in '000)" — no currency symbol at all
        # ₹ variants: ₹, Rs, Rupee, ` (backtick/grave accent)
        _RUPEE = r"(?:₹|`|rs\.?|rupee)"
        # Thousands patterns: thousand, '000, 000s, '000s
        # Note: PDF extractors often produce smart-quotes \u2018/\u2019
        _APOS  = r"[\u2018\u2019'`]"
        _THOU  = r"(?:thousand|" + _APOS + r"000|" + _APOS + r"000s|000s?)"
        _LAKH  = r"(?:lakh|lac|lakhs)"
        _CRORE = r"(?:crore|cr[.\s]|crores)"

        # Check for thousands — with or without currency symbol
        if (re.search(_RUPEE + r'\s*(?:in\s*)?' + _THOU, _header_region)
                or re.search(r'(?:in|amount)\s*' + _RUPEE + r'\s*' + _THOU,
                             _header_region)
                or re.search(r'\(\s*(?:in\s*)?' + _THOU + r'\s*\)',
                             _header_region)
                or re.search(r'\(\s*' + _RUPEE + r'\s*(?:in\s*)?' + _THOU,
                             _header_region)):
            _unit_divisor = 1e5      # thousands → crores
            _unit_label = 'thousands'
        elif (re.search(_RUPEE + r'\s*(?:in\s*)?' + _LAKH, _header_region)
                or re.search(r'(?:in|amount)\s*' + _RUPEE + r'\s*' + _LAKH,
                             _header_region)):
            _unit_divisor = 100      # lakhs → crores
            _unit_label = 'lakhs'
        elif (re.search(_RUPEE + r'\s*(?:in\s*)?' + _CRORE, _header_region)
                or re.search(r'(?:in|amount)\s*' + _RUPEE + r'\s*' + _CRORE,
                             _header_region)):
            _unit_divisor = 1.0
            _unit_label = 'crores'

        # ── 1. Unit-annotated amounts (crore) ─────────────────
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

        # ── 2. Unit-annotated amounts (lakh → crore) ──────────
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

        # ── 3. Fallback: largest plain number only ────────────
        # Use the single largest number (not sum) and convert
        # using the detected reporting unit.
        if not amounts:
            plain_pat = re.compile(r'([\d,]+(?:\.\d+)?)')
            candidates = []
            for m in plain_pat.finditer(text):
                try:
                    val = float(m.group(1).replace(',', ''))
                    if val > 100:  # filter trivial numbers
                        candidates.append(val)
                except ValueError:
                    pass
            if candidates:
                # Take the largest single amount and convert to Cr
                max_val = max(candidates) / _unit_divisor
                if max_val > 0:
                    amounts.append(max_val)

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
