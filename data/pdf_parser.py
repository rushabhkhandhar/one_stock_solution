"""
Annual Report PDF Parser  (Free — No API Keys Required)
========================================================
Uses PyMuPDF (fitz) for fast text extraction and page classification,
pdfplumber for structured table extraction.

Extracts:
  1. Financial Statements (P&L, Balance Sheet, Cash Flow)
  2. Footnotes / Notes to Financial Statements
  3. Key numerical figures for cross-validation
  4. Auditor observations
  5. Contingent liabilities, related party transactions

Architecture:
  • Phase A — Page Classification (PyMuPDF regex scan)
  • Phase B — Table Extraction   (pdfplumber on classified pages)
  • Phase C — Footnote Extraction (text blocks from Notes pages)
  • Phase D — Number Extraction   (regex-based value finder)
"""
import re
import os
from typing import Optional
import fitz           # PyMuPDF
import pdfplumber
import pandas as pd
import numpy as np


# ======================================================================
# Section patterns — used to classify pages
# ======================================================================
SECTION_PATTERNS = {
    'standalone_pnl': re.compile(
        r'standalone\s+statement\s+of\s+profit\s+and\s+loss', re.I),
    'standalone_bs': re.compile(
        r'standalone\s+balance\s+sheet', re.I),
    'standalone_cf': re.compile(
        r'standalone\s+statement\s+of\s+cash\s+flow', re.I),
    'consolidated_pnl': re.compile(
        r'consolidated\s+statement\s+of\s+profit\s+and\s+loss', re.I),
    'consolidated_bs': re.compile(
        r'consolidated\s+balance\s+sheet', re.I),
    'consolidated_cf': re.compile(
        r'consolidated\s+statement\s+of\s+cash\s+flow', re.I),
    'notes_to_financial': re.compile(
        r'notes\s+(forming\s+part\s+of|to\s+the)\s+(consolidated\s+)?'
        r'(standalone\s+)?financial\s+statements?', re.I),
    'auditor_report': re.compile(
        r"independent\s+auditor'?s?\s+report", re.I),
    'significant_accounting': re.compile(
        r'significant\s+accounting\s+polic', re.I),
    'contingent_liabilities': re.compile(
        r'contingent\s+liabilit', re.I),
    'related_party': re.compile(
        r'related\s+party\s+(transaction|disclosure)', re.I),
    'segment_reporting': re.compile(
        r'segment\s+(report|information)', re.I),
    'revenue_recognition': re.compile(
        r'revenue\s+from\s+(operations|contracts)', re.I),
    'earnings_per_share': re.compile(
        r'earnings\s+per\s+(equity\s+)?share', re.I),
    'financial_overview': re.compile(
        r'financial\s+performance\s+overview|ten.year\s+financial', re.I),
}

# Footnote-specific note patterns
NOTE_TITLE_PATTERN = re.compile(
    r'^(?:Note|NOTE)\s*[:\-]?\s*(\d+[a-z]?)\s*[:\-\u2013\u2014]\s*(.+)', re.M)
NOTE_NUMBER_PATTERN = re.compile(
    r'^\s*(\d{1,3})\.\s+(.+)', re.M)

# Number extraction: matches "1,23,456.78" or "123,456.78" or "123.45"
CRORE_NUMBER = re.compile(r'[\d,]+\.\d{2}')
INDIAN_NUMBER = re.compile(r'([\d,]+(?:\.\d+)?)')


class PDFParser:
    """
    Extract structured data from Annual Report PDFs.

    Usage:
        parser = PDFParser()
        result = parser.parse('output/reports/TCS_AR_2025.pdf')
        # result['sections']     -> classified page ranges
        # result['tables']       -> extracted financial tables
        # result['footnotes']    -> list of footnote dicts
        # result['key_figures']  -> extracted key financial figures
        # result['auditor_obs']  -> auditor observations
    """

    def __init__(self):
        pass

    # ==================================================================
    # Master parse
    # ==================================================================
    def parse(self, pdf_path: str, consolidated: bool = True) -> dict:
        """
        Full pipeline: classify -> extract tables -> extract footnotes -> numbers.

        Returns:
            {
                'path': str,
                'total_pages': int,
                'sections': {section_name: [page_numbers]},
                'tables': {section_name: [DataFrame, ...]},
                'footnotes': [{'note_id': ..., 'title': ..., 'text': ..., 'page': ...}],
                'key_figures': {metric_name: value},
                'auditor_observations': [str],
                'contingent_liabilities': str,
                'related_party_summary': str,
                'available': True
            }
        """
        if not os.path.exists(pdf_path):
            return {'available': False, 'reason': f'File not found: {pdf_path}'}

        print(f"    \U0001f4c4 Parsing {os.path.basename(pdf_path)} ...")

        # Phase A -- Page Classification
        sections = self._classify_pages(pdf_path)

        # Determine prefix based on consolidated flag
        prefix = 'consolidated' if consolidated else 'standalone'

        # Phase B -- Table Extraction from key financial statement pages
        tables = self._extract_tables(pdf_path, sections, prefix)

        # Phase C -- Footnote Extraction
        footnotes = self._extract_footnotes(pdf_path, sections)

        # Phase D -- Key Figures from financial overview / P&L pages
        key_figures = self._extract_key_figures(pdf_path, sections, prefix)

        # Phase E -- Auditor observations
        auditor_obs = self._extract_auditor_observations(pdf_path, sections)

        # Phase F -- Special sections
        contingent = self._extract_section_text(
            pdf_path, sections.get('contingent_liabilities', []), max_pages=2)
        # RPT: filter out AGM notice pages (which contain authorization
        # limits, not actual RPT figures) and prefer Notes pages.
        rpt_pages = self._filter_rpt_pages(
            pdf_path, sections.get('related_party', []))
        related = self._extract_section_text(
            pdf_path, rpt_pages, max_pages=2)

        doc = fitz.open(pdf_path)
        total_pages = doc.page_count
        doc.close()

        result = {
            'available': True,
            'path': pdf_path,
            'total_pages': total_pages,
            'sections': {k: v for k, v in sections.items() if v},
            'tables': tables,
            'footnotes': footnotes,
            'key_figures': key_figures,
            'auditor_observations': auditor_obs,
            'contingent_liabilities': contingent,
            'related_party_summary': related,
        }

        n_fn = len(footnotes)
        n_figs = len(key_figures)
        n_tables = sum(len(v) for v in tables.values())
        print(f"    \u2714 Extracted: {n_tables} tables, {n_fn} footnotes, "
              f"{n_figs} key figures")

        return result

    # ==================================================================
    # Phase A -- Page Classification
    # ==================================================================
    def _classify_pages(self, pdf_path: str) -> dict:
        """Scan every page and classify by section patterns."""
        doc = fitz.open(pdf_path)
        sections = {name: [] for name in SECTION_PATTERNS}

        for i in range(doc.page_count):
            text = doc[i].get_text()
            for name, pattern in SECTION_PATTERNS.items():
                if pattern.search(text):
                    sections[name].append(i + 1)  # 1-indexed

        doc.close()
        return sections

    # ==================================================================
    # Phase B -- Table Extraction
    # ==================================================================
    def _extract_tables(self, pdf_path: str, sections: dict,
                        prefix: str) -> dict:
        """Extract financial tables from classified pages using pdfplumber."""
        tables = {}

        # Target sections and their page keys
        targets = {
            f'{prefix}_pnl': f'{prefix}_pnl',
            f'{prefix}_bs': f'{prefix}_bs',
            f'{prefix}_cf': f'{prefix}_cf',
            'financial_overview': 'financial_overview',
        }

        # --- Exclude opposite-statement pages (same logic as key figures) ---
        opposite = 'standalone' if prefix == 'consolidated' else 'consolidated'
        opposite_pages = set(
            sections.get(f'{opposite}_pnl', []) +
            sections.get(f'{opposite}_cf', []) +
            sections.get(f'{opposite}_bs', [])
        )
        prefix_pages = set(
            sections.get(f'{prefix}_pnl', []) +
            sections.get(f'{prefix}_cf', []) +
            sections.get(f'{prefix}_bs', [])
        )
        ambiguous_pages = prefix_pages & opposite_pages
        exclude_pages = opposite_pages | ambiguous_pages

        pdf = pdfplumber.open(pdf_path)

        for label, section_key in targets.items():
            pages = sections.get(section_key, [])
            pages = [p for p in pages if p not in exclude_pages]
            if not pages:
                continue

            extracted = []
            # Try the first 3 pages of each section
            for page_num in pages[:3]:
                if page_num > len(pdf.pages):
                    continue
                page = pdf.pages[page_num - 1]
                page_tables = page.extract_tables()
                for tbl in page_tables:
                    if tbl and len(tbl) >= 3:  # At least header + 2 data rows
                        df = self._table_to_dataframe(tbl)
                        if not df.empty:
                            extracted.append(df)
            if extracted:
                tables[label] = extracted

        pdf.close()
        return tables

    def _table_to_dataframe(self, raw_table: list) -> pd.DataFrame:
        """Convert pdfplumber raw table to a cleaned DataFrame."""
        if not raw_table or len(raw_table) < 2:
            return pd.DataFrame()

        # Clean cells
        cleaned = []
        for row in raw_table:
            cleaned_row = []
            for cell in row:
                if cell is None:
                    cleaned_row.append('')
                else:
                    cell = str(cell).replace('\n', ' ').strip()
                    cleaned_row.append(cell)
            cleaned.append(cleaned_row)

        # Try to use first non-empty row as header
        header_idx = 0
        for i, row in enumerate(cleaned):
            if any(cell.strip() for cell in row):
                header_idx = i
                break

        headers = cleaned[header_idx]
        data = cleaned[header_idx + 1:]

        if not data:
            return pd.DataFrame()

        # Make headers unique
        seen = {}
        unique_headers = []
        for h in headers:
            h = h.strip() or f'col_{len(unique_headers)}'
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            unique_headers.append(h)

        try:
            df = pd.DataFrame(data, columns=unique_headers)
            df = df.replace('', np.nan).dropna(how='all').reset_index(drop=True)
            return df
        except Exception:
            return pd.DataFrame()

    # ==================================================================
    # Phase C -- Footnote Extraction
    # ==================================================================
    def _extract_footnotes(self, pdf_path: str, sections: dict) -> list:
        """
        Extract footnotes/notes from the "Notes to Financial Statements".

        Identifies individual notes by numbered headings and captures
        the text content for each.
        """
        note_pages = sections.get('notes_to_financial', [])
        if not note_pages:
            return []

        doc = fitz.open(pdf_path)
        footnotes = []
        current_note = None

        # Process notes pages (can span 100+ pages)
        # Focus on the first 80 pages of notes to keep it manageable
        pages_to_scan = note_pages[:80]

        for page_num in pages_to_scan:
            if page_num > doc.page_count:
                continue
            text = doc[page_num - 1].get_text()
            lines = text.split('\n')

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Check for note title patterns:
                # "Note 1: Significant accounting policies"
                # "1. Revenue from operations"
                # "Note 28 - Contingent Liabilities"
                m = NOTE_TITLE_PATTERN.match(line)
                if not m:
                    m = NOTE_NUMBER_PATTERN.match(line)

                if m:
                    if current_note:
                        footnotes.append(current_note)

                    note_id = m.group(1)
                    title = m.group(2).strip() if len(m.groups()) > 1 else ''
                    current_note = {
                        'note_id': note_id,
                        'title': title,
                        'text': '',
                        'page': page_num,
                        'numbers': [],
                    }
                elif current_note:
                    current_note['text'] += line + '\n'
                    nums = CRORE_NUMBER.findall(line)
                    for n in nums:
                        try:
                            val = float(n.replace(',', ''))
                            current_note['numbers'].append(val)
                        except ValueError:
                            pass

        if current_note:
            footnotes.append(current_note)

        doc.close()

        # Tag important footnotes
        important_keywords = [
            'contingent', 'related party', 'segment', 'revenue',
            'depreciation', 'intangible', 'employee', 'tax',
            'borrowing', 'lease', 'provisions', 'earnings per share',
            'dividend', 'exceptional', 'going concern', 'impairment',
            'fair value', 'financial instrument', 'subsequent event',
        ]
        for fn in footnotes:
            fn['is_important'] = any(
                kw in (fn['title'] + fn['text'][:200]).lower()
                for kw in important_keywords
            )

        return footnotes

    # ==================================================================
    # Phase D -- Key Figure Extraction
    # ==================================================================
    def _extract_key_figures(self, pdf_path: str, sections: dict,
                             prefix: str) -> dict:
        """
        Extract key financial figures from the P&L overview page.
        These are used for cross-validation against scraper data.
        """
        doc = fitz.open(pdf_path)
        figures = {}

        target_pages = (
            sections.get('financial_overview', []) +
            sections.get(f'{prefix}_pnl', []) +
            sections.get(f'{prefix}_cf', []) +
            sections.get(f'{prefix}_bs', [])
        )

        # --- Exclude pages belonging to the *opposite* statement type ---
        # E.g. when prefix='consolidated', drop pages also in standalone
        opposite = 'standalone' if prefix == 'consolidated' else 'consolidated'
        opposite_pages = set(
            sections.get(f'{opposite}_pnl', []) +
            sections.get(f'{opposite}_cf', []) +
            sections.get(f'{opposite}_bs', [])
        )
        # Pages appearing in BOTH consolidated AND standalone are usually
        # Table-of-Contents / index pages — exclude them too.
        prefix_pages = set(
            sections.get(f'{prefix}_pnl', []) +
            sections.get(f'{prefix}_cf', []) +
            sections.get(f'{prefix}_bs', [])
        )
        ambiguous_pages = prefix_pages & opposite_pages  # TOC-like pages
        exclude = opposite_pages | ambiguous_pages
        target_pages = [p for p in target_pages if p not in exclude]

        rev_pat = re.compile(
            r'revenue\s+from\s+operations?\s*'
            r'[\s\S]{0,80}?([\d,]*\d{3,}(?:\.\d+)?)', re.I)
        pat_pat = re.compile(
            r'profit\s+(?:after\s+tax|for\s+the\s+(?:year|period))'
            r'\s*[\s\S]{0,80}?([\d,]*\d{3,}(?:\.\d+)?)', re.I)
        eps_pat = re.compile(
            r'(?:basic\s+)?earnings\s+per\s+(?:equity\s+)?share'
            r'\s*[\s\S]{0,80}?([\d,]+\.\d+)', re.I)
        total_assets_pat = re.compile(
            r'total\s+assets?\s*[\s\S]{0,50}?([\d,]+(?:\.\d+)?)', re.I)
        borrowings_pat = re.compile(
            r'(?:total\s+)?borrowings?\s*[\s\S]{0,50}?([\d,]+(?:\.\d+)?)', re.I)
        dividend_pat = re.compile(
            r'dividend\s+(?:per\s+share|declared)\s*[\s\S]{0,50}?'
            r'([\d,]+(?:\.\d+)?)', re.I)
        op_cf_pat = re.compile(
            r'net\s+cash\s+(?:flows?\s+)?(?:generated\s+)?from\s+operating'
            r'\s+activities\s*\n?\s*([\d,]+(?:\.\d+)?)', re.I)

        named_patterns = [
            ('revenue_ar', rev_pat),
            ('pat_ar', pat_pat),
            ('eps_ar', eps_pat),
            ('total_assets_ar', total_assets_pat),
            ('borrowings_ar', borrowings_pat),
            ('dividend_per_share_ar', dividend_pat),
            ('operating_cashflow_ar', op_cf_pat),
        ]

        # Deduplicate and maintain order
        seen = set()
        unique_pages = []
        for p in target_pages:
            if p not in seen:
                seen.add(p)
                unique_pages.append(p)

        for page_num in unique_pages:
            if page_num > doc.page_count:
                continue
            text = doc[page_num - 1].get_text()

            for key, pat in named_patterns:
                if key in figures:
                    continue
                m = pat.search(text)
                if m:
                    try:
                        val = float(m.group(1).replace(',', ''))
                        if val > 0:
                            figures[key] = val
                    except (ValueError, IndexError):
                        pass

        # Also try extracting from tables (more reliable)
        self._extract_figures_from_tables(pdf_path, sections, prefix, figures)

        doc.close()
        return figures

    def _extract_figures_from_tables(self, pdf_path: str, sections: dict,
                                      prefix: str, figures: dict):
        """Extract key figures from pdfplumber tables -- more reliable."""
        overview_pages = sections.get('financial_overview', [])
        if not overview_pages:
            return

        pdf = pdfplumber.open(pdf_path)
        for page_num in overview_pages[:3]:
            if page_num > len(pdf.pages):
                continue
            page = pdf.pages[page_num - 1]
            tables = page.extract_tables()
            for tbl in tables:
                if not tbl or len(tbl) < 3:
                    continue
                for row in tbl:
                    if not row:
                        continue
                    row_text = ' '.join(str(c or '') for c in row).lower()
                    nums = []
                    for cell in row[1:]:
                        if cell:
                            m = INDIAN_NUMBER.search(str(cell).replace(' ', ''))
                            if m:
                                try:
                                    val = float(m.group(1).replace(',', ''))
                                    nums.append(val)
                                except ValueError:
                                    pass
                    if not nums:
                        continue

                    latest = nums[0]

                    if 'revenue' in row_text and 'revenue_table' not in figures:
                        figures['revenue_table'] = latest
                    elif ('profit after tax' in row_text or
                          'profit attributable' in row_text):
                        if 'pat_table' not in figures:
                            figures['pat_table'] = latest
                    elif 'earnings per share' in row_text:
                        if 'eps_table' not in figures:
                            figures['eps_table'] = latest
        pdf.close()

    # ==================================================================
    # Phase E -- Auditor Observations
    # ==================================================================
    def _extract_auditor_observations(self, pdf_path: str,
                                       sections: dict) -> list:
        """Extract key observations from the auditor's report."""
        auditor_pages = sections.get('auditor_report', [])
        if not auditor_pages:
            return []

        doc = fitz.open(pdf_path)
        observations = []

        alert_patterns = [
            re.compile(r'emphasis\s+of\s+matter', re.I),
            re.compile(r'qualifi(ed|cation)', re.I),
            re.compile(r'material\s+(misstatement|weakness|uncertainty)', re.I),
            re.compile(r'going\s+concern', re.I),
            re.compile(r'key\s+audit\s+matter', re.I),
            re.compile(r'adverse\s+opinion', re.I),
            re.compile(r'disclaimer\s+of\s+opinion', re.I),
            re.compile(r'except\s+for', re.I),
            re.compile(r'departure\s+from', re.I),
            re.compile(r'non-?compliance', re.I),
        ]

        for page_num in auditor_pages[:10]:
            if page_num > doc.page_count:
                continue
            text = doc[page_num - 1].get_text()
            for pat in alert_patterns:
                matches = pat.finditer(text)
                for m in matches:
                    start = max(0, m.start() - 150)
                    end = min(len(text), m.end() + 300)
                    context = text[start:end].replace('\n', ' ').strip()
                    observations.append({
                        'type': pat.pattern.replace('\\s+', ' '),
                        'context': context,
                        'page': page_num,
                    })

        doc.close()

        # Deduplicate
        seen = set()
        unique = []
        for obs in observations:
            key = obs['context'][:80]
            if key not in seen:
                seen.add(key)
                unique.append(obs)

        return unique

    # ==================================================================
    # Utility -- Extract raw text from specific pages
    # ==================================================================
    def _filter_rpt_pages(self, pdf_path: str, pages: list) -> list:
        """Filter RPT pages: prefer Notes to FS; exclude AGM notices.

        AGM notice pages contain huge authorization limits ("up to
        ₹X crore") that dwarf actual RPT amounts and cause
        extraction hallucinations.  Pages from Notes to Financial
        Statements contain the real RPT disclosure.
        """
        if not pages:
            return pages

        import fitz as _fitz
        doc = _fitz.open(pdf_path)
        notes_pages = []
        other_pages = []

        AGM_RE = re.compile(
            r'ordinary\s+resolution|special\s+resolution'
            r'|notice\s+.*annual\s+general'
            r'|approval\s+of\s+.*member'
            r'|resolved\s+that\s+pursuant',
            re.I)
        NOTES_RE = re.compile(
            r'notes\s+to\s+.*financial|note\s+\d+'
            r'|standalone\s+financial\s+statement'
            r'|consolidated\s+financial\s+statement',
            re.I)

        for pg in pages:
            if pg > doc.page_count:
                continue
            text = doc[pg - 1].get_text()
            is_agm = bool(AGM_RE.search(text))
            is_notes = bool(NOTES_RE.search(text))

            if is_agm and not is_notes:
                continue          # skip pure AGM notice pages
            if is_notes:
                notes_pages.append(pg)
            else:
                other_pages.append(pg)

        doc.close()

        # Prefer notes pages; fall back to non-AGM pages
        result = notes_pages if notes_pages else other_pages
        return result if result else pages  # last resort: original list

    def _extract_section_text(self, pdf_path: str, pages: list,
                               max_pages: int = 3) -> str:
        """Extract raw text from a list of page numbers."""
        if not pages:
            return ''

        doc = fitz.open(pdf_path)
        texts = []
        for page_num in pages[:max_pages]:
            if page_num > doc.page_count:
                continue
            texts.append(doc[page_num - 1].get_text())
        doc.close()
        return '\n\n---\n\n'.join(texts)

    # ==================================================================
    # Quick summary for display
    # ==================================================================
    def summarize_footnotes(self, footnotes: list,
                             top_n: int = 15) -> list:
        """Return the most important footnotes for the report."""
        important = [fn for fn in footnotes if fn.get('is_important')]
        others = [fn for fn in footnotes if not fn.get('is_important')]

        selected = important[:top_n]
        remaining = top_n - len(selected)
        if remaining > 0:
            selected.extend(others[:remaining])

        for fn in selected:
            if len(fn['text']) > 500:
                fn['text_preview'] = fn['text'][:500] + ' ...'
            else:
                fn['text_preview'] = fn['text']

        return selected
