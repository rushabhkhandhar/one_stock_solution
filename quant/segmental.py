"""
Segmental Performance Extraction
==================================
Master Plan Item 1d:
  Extract segment-wise revenue, EBIT, and growth from annual report.

Uses the pdf_parser's segment_reporting page classification to find
the right pages, then extracts tables and text.
"""
import re
import numpy as np
import pandas as pd


class SegmentalAnalysis:
    """Extract and analyze business segment performance from AR."""

    def extract(self, ar_parsed: dict) -> dict:
        """
        Extract segmental data from parsed annual report.

        Returns:
            {
                available, segments: [
                    {name, revenue, ebit, ebit_margin, revenue_pct}
                ],
                total_revenue, dominant_segment, concentration_risk
            }
        """
        sections = ar_parsed.get('sections', {})
        segment_pages = sections.get('segment_reporting', [])

        if not segment_pages:
            return {'available': False,
                    'reason': 'No segment reporting section found in AR'}

        # Try to extract from tables first (more reliable)
        tables = ar_parsed.get('tables', {})
        segments = []

        # Check if segment tables were extracted
        for key, table_list in tables.items():
            if 'segment' in key.lower():
                for df in table_list:
                    segments = self._parse_segment_table(df)
                    if segments:
                        break
            if segments:
                break

        # If no segment tables, try text extraction from footnotes
        if not segments:
            footnotes = ar_parsed.get('footnotes', [])
            segments = self._extract_from_footnotes(footnotes)

        if not segments:
            return {
                'available': True,
                'segments': [],
                'reason': 'Segment section found but could not parse details',
                'segment_pages': segment_pages,
            }

        # Calculate totals and percentages
        total_rev = sum(s.get('revenue', 0) for s in segments if s.get('revenue'))
        if total_rev > 0:
            for s in segments:
                if s.get('revenue'):
                    s['revenue_pct'] = round(s['revenue'] / total_rev * 100, 1)

        # Identify dominant segment and concentration risk
        dominant = max(segments, key=lambda x: x.get('revenue', 0)) if segments else None
        concentration = None
        if dominant and dominant.get('revenue_pct'):
            pct = dominant['revenue_pct']
            if pct > 80:
                concentration = 'HIGH'
            elif pct > 60:
                concentration = 'MEDIUM'
            else:
                concentration = 'LOW'

        return {
            'available': True,
            'segments': segments,
            'total_revenue': total_rev,
            'dominant_segment': dominant.get('name') if dominant else None,
            'dominant_pct': dominant.get('revenue_pct') if dominant else None,
            'concentration_risk': concentration,
            'num_segments': len(segments),
        }

    # ------------------------------------------------------------------
    # Table parsing
    # ------------------------------------------------------------------
    def _parse_segment_table(self, df: pd.DataFrame) -> list:
        """Parse a segment reporting DataFrame."""
        if df.empty or len(df) < 2:
            return []

        segments = []
        # Look for revenue and EBIT/profit rows
        for _, row in df.iterrows():
            name_col = row.iloc[0] if len(row) > 0 else ''
            if not isinstance(name_col, str):
                name_col = str(name_col)

            name_lower = name_col.lower().strip()

            # Skip header/total rows
            if any(kw in name_lower for kw in [
                'total', 'inter-segment', 'elimination', 'unallocated',
                'reconciliation',
            ]):
                continue

            # Try to get numeric values
            values = []
            for cell in row.iloc[1:]:
                try:
                    val = float(str(cell).replace(',', '').replace('(', '-').replace(')', ''))
                    values.append(val)
                except (ValueError, TypeError):
                    pass

            if name_lower and values and not any(
                kw in name_lower for kw in ['segment', 'reporting', 'note', 'year']
            ):
                seg = {'name': name_col.strip()}
                if values:
                    seg['revenue'] = abs(values[0])
                if len(values) > 1:
                    seg['ebit'] = values[1]
                    if seg.get('revenue') and seg['revenue'] > 0:
                        seg['ebit_margin'] = round(
                            values[1] / seg['revenue'] * 100, 1)
                segments.append(seg)

        return segments

    # ------------------------------------------------------------------
    # Footnote-based extraction
    # ------------------------------------------------------------------
    def _extract_from_footnotes(self, footnotes: list) -> list:
        """Extract segment info from footnotes about segment reporting."""
        segments = []

        for fn in footnotes:
            title = fn.get('title', '').lower()
            text = fn.get('text', '')

            if 'segment' not in title:
                continue

            # Look for segment names followed by numbers
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if not line or len(line) < 5:
                    continue

                # Pattern: "Segment Name  1,234.56  567.89"
                m = re.match(
                    r'^([A-Za-z][A-Za-z\s&/\-]+?)\s+'
                    r'([\d,]+(?:\.\d+)?)\s*'
                    r'(?:([\d,]+(?:\.\d+)?))?',
                    line
                )
                if m:
                    name = m.group(1).strip()
                    # Skip if name looks like a header
                    if any(kw in name.lower() for kw in [
                        'total', 'elimination', 'unallocated',
                        'particular', 'segment',
                    ]):
                        continue

                    try:
                        rev = float(m.group(2).replace(',', ''))
                    except ValueError:
                        continue

                    seg = {'name': name, 'revenue': rev}
                    if m.group(3):
                        try:
                            ebit = float(m.group(3).replace(',', ''))
                            seg['ebit'] = ebit
                            if rev > 0:
                                seg['ebit_margin'] = round(ebit / rev * 100, 1)
                        except ValueError:
                            pass
                    segments.append(seg)

        return segments
