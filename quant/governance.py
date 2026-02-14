"""
Governance Dashboard — Board Composition & Quality
====================================================
Master Plan Items:
  4a — Board composition (Independent Directors %, meeting attendance)
  4b — Promoter remuneration vs profit
  4c — Promoter pledging (handled in orchestrator shareholding)

Extracts governance data from:
  - Annual Report PDF (corporate governance section)
  - Screener.in page (basic info)
"""
import re


class GovernanceDashboard:
    """Extract and analyze corporate governance metrics."""

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def analyze(self, ar_parsed: dict, data: dict) -> dict:
        """
        Extract governance metrics from parsed annual report.

        Returns:
            {
                available, board_composition, independent_pct,
                board_meetings, attendance_pct,
                promoter_remuneration, remuneration_as_pct_profit,
                governance_score, flags
            }
        """
        result = {'available': False}

        # Try to extract from AR sections
        sections = ar_parsed.get('sections', {})
        footnotes = ar_parsed.get('footnotes', [])

        # Extract corporate governance text from the full AR
        gov_text = self._find_governance_text(ar_parsed)

        if not gov_text and not footnotes:
            result['reason'] = 'No corporate governance section found in AR'
            return result

        result['available'] = True
        flags = []

        # ── Board Composition ──
        board = self._extract_board_composition(gov_text, footnotes)
        result['board_composition'] = board
        if board.get('independent_pct') is not None:
            result['independent_pct'] = board['independent_pct']
            if board['independent_pct'] < 33:
                flags.append({
                    'severity': 'HIGH',
                    'flag': f"Independent directors only {board['independent_pct']}% "
                            f"(SEBI requires min 33% for promoter-chaired boards)",
                })
            elif board['independent_pct'] < 50:
                flags.append({
                    'severity': 'MEDIUM',
                    'flag': f"Independent directors at {board['independent_pct']}% — "
                            f"meets minimum but not best practice (50%+)",
                })

        # ── Board Meetings ──
        meetings = self._extract_meetings(gov_text)
        result['board_meetings'] = meetings
        if meetings.get('count') is not None and meetings['count'] < 4:
            flags.append({
                'severity': 'HIGH',
                'flag': f"Only {meetings['count']} board meetings "
                        f"(SEBI mandates minimum 4 per year)",
            })

        # ── Promoter Remuneration ──
        remuneration = self._extract_remuneration(gov_text, footnotes, data)
        result['promoter_remuneration'] = remuneration
        if remuneration.get('as_pct_profit') is not None:
            if remuneration['as_pct_profit'] > 10:
                flags.append({
                    'severity': 'HIGH',
                    'flag': f"Promoter/KMP remuneration is {remuneration['as_pct_profit']}% "
                            f"of PAT — excessive",
                })
            elif remuneration['as_pct_profit'] > 5:
                flags.append({
                    'severity': 'MEDIUM',
                    'flag': f"Promoter/KMP remuneration is {remuneration['as_pct_profit']}% "
                            f"of PAT — on higher side",
                })

        # ── Governance Score (0-10) ──
        score = 10
        for f in flags:
            if f['severity'] == 'HIGH':
                score -= 3
            elif f['severity'] == 'MEDIUM':
                score -= 1
        result['governance_score'] = max(0, score)
        result['flags'] = flags

        return result

    # ------------------------------------------------------------------
    # Board composition extraction
    # ------------------------------------------------------------------
    def _extract_board_composition(self, gov_text: str,
                                    footnotes: list) -> dict:
        """Extract board size and independent director percentage."""
        board = {'total_directors': None, 'independent_pct': None}

        text = gov_text.lower()

        # Try to find total directors
        dir_pat = re.compile(
            r'(?:board\s+(?:of\s+)?directors?\s+(?:comprises?|consists?)\s+'
            r'(?:of\s+)?(\d+))|'
            r'(?:(\d+)\s+directors?\s+(?:on|in)\s+the\s+board)',
            re.IGNORECASE
        )
        m = dir_pat.search(gov_text)
        if m:
            board['total_directors'] = int(m.group(1) or m.group(2))

        # Try to find independent directors count/percentage
        ind_pat = re.compile(
            r'(\d+)\s+(?:of\s+(?:the\s+)?(?:\d+\s+)?)?'
            r'(?:are\s+)?independent\s+(?:non-executive\s+)?directors?',
            re.IGNORECASE
        )
        m = ind_pat.search(gov_text)
        if m and board['total_directors']:
            ind_count = int(m.group(1))
            board['independent_count'] = ind_count
            board['independent_pct'] = round(
                ind_count / board['total_directors'] * 100, 1)

        # Fallback: look for percentage directly
        if board['independent_pct'] is None:
            pct_pat = re.compile(
                r'(\d+(?:\.\d+)?)\s*%\s*(?:of\s+)?'
                r'(?:the\s+)?(?:board\s+)?(?:are\s+)?independent',
                re.IGNORECASE
            )
            m = pct_pat.search(gov_text)
            if m:
                board['independent_pct'] = float(m.group(1))

        # Check footnotes for director info
        for fn in footnotes:
            fn_text = (fn.get('title', '') + ' ' + fn.get('text', '')).lower()
            if 'director' in fn_text and 'independent' in fn_text:
                m = ind_pat.search(fn.get('text', ''))
                if m and board['independent_pct'] is None:
                    # Can't compute percentage without total
                    board['independent_count'] = int(m.group(1))

        return board

    # ------------------------------------------------------------------
    # Board meetings extraction
    # ------------------------------------------------------------------
    def _extract_meetings(self, gov_text: str) -> dict:
        """Extract number of board meetings held."""
        meetings = {'count': None, 'attendance_pct': None}

        # Pattern: "X board meetings were held" / "held X meetings"
        meet_pat = re.compile(
            r'(\d+)\s+(?:board\s+)?meetings?\s+'
            r'(?:were\s+)?(?:held|conducted)',
            re.IGNORECASE
        )
        m = meet_pat.search(gov_text)
        if m:
            meetings['count'] = int(m.group(1))

        # Attendance percentage
        att_pat = re.compile(
            r'(?:average\s+)?attendance.*?(\d+(?:\.\d+)?)\s*%',
            re.IGNORECASE
        )
        m = att_pat.search(gov_text)
        if m:
            meetings['attendance_pct'] = float(m.group(1))

        return meetings

    # ------------------------------------------------------------------
    # Promoter remuneration
    # ------------------------------------------------------------------
    def _extract_remuneration(self, gov_text: str,
                               footnotes: list, data: dict) -> dict:
        """Extract promoter/KMP remuneration and compare to PAT."""
        import numpy as np
        from data.preprocessing import DataPreprocessor, get_value

        remuneration = {'total_cr': None, 'as_pct_profit': None}

        # Search for remuneration amounts in governance text
        rem_pat = re.compile(
            r'(?:remuneration|compensation|salary)\s*'
            r'(?:.*?)\s*([\d,]+(?:\.\d+)?)\s*'
            r'(?:crore|cr|lakh)',
            re.IGNORECASE
        )
        amounts = []
        for m in rem_pat.finditer(gov_text):
            try:
                val = float(m.group(1).replace(',', ''))
                amounts.append(val)
            except ValueError:
                pass

        # Also check relevant footnotes
        for fn in footnotes:
            fn_text = fn.get('title', '') + ' ' + fn.get('text', '')
            if any(kw in fn_text.lower()
                   for kw in ['remuneration', 'key management',
                              'managerial', 'director']):
                for m in rem_pat.finditer(fn_text):
                    try:
                        val = float(m.group(1).replace(',', ''))
                        amounts.append(val)
                    except ValueError:
                        pass

        if amounts:
            total_rem = max(amounts)  # Largest is likely total KMP remuneration
            remuneration['total_cr'] = round(total_rem, 2)

            # Compare to PAT
            pnl = data.get('pnl')
            if pnl is not None and not pnl.empty:
                pp = DataPreprocessor()
                pat = get_value(pp.get(pnl, 'net_profit'))
                if not np.isnan(pat) and pat > 0:
                    remuneration['as_pct_profit'] = round(
                        total_rem / pat * 100, 2)

        return remuneration

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _find_governance_text(ar_parsed: dict) -> str:
        """Find corporate governance related text from parsed AR."""
        # Check if there's a dedicated governance section in pages
        sections = ar_parsed.get('sections', {})

        # Use auditor report pages as proxy (governance report is nearby)
        # Also check footnotes for governance-related content
        footnotes = ar_parsed.get('footnotes', [])
        gov_texts = []

        for fn in footnotes:
            fn_text = (fn.get('title', '') + ' ' + fn.get('text', '')).lower()
            if any(kw in fn_text for kw in [
                'director', 'board', 'governance', 'remuneration',
                'meeting', 'independent', 'committee',
            ]):
                gov_texts.append(fn.get('text', ''))

        # Also include related_party_summary and contingent text
        # as they sometimes contain governance info
        rpt = ar_parsed.get('related_party_summary', '')
        if 'director' in rpt.lower() or 'key management' in rpt.lower():
            gov_texts.append(rpt)

        return '\n\n'.join(gov_texts)
