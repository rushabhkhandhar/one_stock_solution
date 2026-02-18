"""
Layout-Aware Table Parser — High-Fidelity PDF Extraction
==========================================================
Standard text extraction often "flattens" tables, losing the
relationships between column headers and cell values.

This module provides:
  1. Header-aware table extraction (preserves column→value mapping)
  2. Facts-based output (each row embedded with column headers)
  3. Structured JSON/dict for precise RAG retrieval
  4. Specialized extractors for:
     - BRSR (Business Responsibility & Sustainability Report)
     - Segmental Revenue tables
     - Notes to Accounts (contingent liabilities, RPT)
     - Financial overview / ten-year highlight tables

Uses pdfplumber in "accurate mode" + custom header detection.
"""
import re
from typing import Optional
import pandas as pd
import numpy as np

try:
    import pdfplumber
    _PDFPLUMBER = True
except ImportError:
    _PDFPLUMBER = False

try:
    import fitz  # PyMuPDF
    _FITZ = True
except ImportError:
    _FITZ = False


class LayoutAwareParser:
    """
    Extract tables with full header-value relationships preserved.

    Usage:
        parser = LayoutAwareParser()
        result = parser.extract_tables(pdf_path, table_type='segmental')
    """

    def __init__(self):
        self._available = _PDFPLUMBER or _FITZ

    @property
    def available(self) -> bool:
        return self._available

    def extract_tables(self, pdf_path: str,
                       table_type: str = 'all',
                       pages: list = None,
                       max_pages: int = 80) -> dict:
        """
        Extract tables from a PDF with layout awareness.

        Parameters
        ----------
        pdf_path   : path to PDF file
        table_type : 'all', 'segmental', 'brsr', 'contingent', 'rpt',
                     'financial_overview'
        pages      : specific page numbers to extract from (0-indexed)
        max_pages  : maximum pages to scan (to keep runtime reasonable)

        Returns
        -------
        dict with extracted_tables (list of structured table dicts)
        """
        if not self._available:
            return {'available': False,
                    'reason': 'pdfplumber not installed'}

        if not pdf_path:
            return {'available': False, 'reason': 'No PDF path provided'}

        try:
            tables = []

            # Use fitz (PyMuPDF) for fast page text scanning, then only
            # send relevant pages to pdfplumber for table extraction.
            relevant_pages = []
            if _FITZ:
                import fitz as _fitz
                doc = _fitz.open(pdf_path)
                total_pages = len(doc)
                if pages:
                    scan_range = pages
                elif table_type == 'all':
                    start = max(0, total_pages // 2 - 10)
                    end = min(total_pages, start + max_pages)
                    scan_range = range(start, end)
                else:
                    scan_range = range(min(total_pages, max_pages))

                for page_num in scan_range:
                    if page_num >= total_pages:
                        continue
                    page_text = doc[page_num].get_text() or ''
                    if table_type != 'all':
                        if self._is_relevant_page(page_text, table_type):
                            relevant_pages.append((page_num, page_text))
                    else:
                        # For 'all', only send pages that look like tables
                        if ('|' in page_text or
                            any(kw in page_text.lower() for kw in [
                                'revenue', 'profit', 'balance', 'cash flow',
                                'segment', 'contingent', 'related party',
                            ])):
                            relevant_pages.append((page_num, page_text))
                doc.close()
            else:
                # No fitz, directly use pdfplumber (slower)
                relevant_pages = None  # Signal to scan all

            if relevant_pages is not None and not relevant_pages:
                return {
                    'available': True,
                    'extracted_tables': [],
                    'num_tables': 0,
                    'reason': f'No {table_type} tables found in scanned pages',
                }

            with pdfplumber.open(pdf_path) as pdf:
                if relevant_pages is not None:
                    target_page_nums = [p[0] for p in relevant_pages]
                elif pages:
                    target_page_nums = pages
                else:
                    total = len(pdf.pages)
                    start = max(0, total // 2 - 10)
                    end = min(total, start + max_pages)
                    target_page_nums = list(range(start, end))

                for page_num in target_page_nums[:30]:  # Hard cap at 30 pages
                    if page_num >= len(pdf.pages):
                        continue

                    page = pdf.pages[page_num]

                    # Extract tables with settings for better accuracy
                    page_tables = page.extract_tables({
                        'vertical_strategy': 'text',
                        'horizontal_strategy': 'text',
                        'snap_tolerance': 5,
                        'join_tolerance': 5,
                        'min_words_vertical': 2,
                        'min_words_horizontal': 1,
                    })

                    # Get page_text from fitz scan if available
                    page_text = ''
                    if relevant_pages is not None:
                        for pn, pt in relevant_pages:
                            if pn == page_num:
                                page_text = pt
                                break
                    if not page_text:
                        try:
                            page_text = page.extract_text() or ''
                        except Exception:
                            page_text = ''

                    for raw_table in page_tables:
                        if not raw_table or len(raw_table) < 2:
                            continue

                        structured = self._structure_table(
                            raw_table, page_num, page_text, table_type)
                        if structured:
                            tables.append(structured)

            if not tables:
                return {
                    'available': True,
                    'extracted_tables': [],
                    'num_tables': 0,
                    'reason': f'No {table_type} tables found',
                }

            return {
                'available': True,
                'extracted_tables': tables,
                'num_tables': len(tables),
            }

        except Exception as e:
            return {'available': False, 'reason': str(e)}

    def extract_facts(self, pdf_path: str,
                      table_type: str = 'all') -> dict:
        """
        Extract tables as flat facts (each row embedded with headers).

        Returns a list of facts like:
          {"metric": "Revenue from Operations", "FY2024": "1,23,456",
           "FY2023": "1,10,234", "source": "page 45", "table_type": "pnl"}
        """
        result = self.extract_tables(pdf_path, table_type)
        if not result.get('available') or not result.get('extracted_tables'):
            return result

        facts = []
        for table in result['extracted_tables']:
            headers = table.get('headers', [])
            rows = table.get('rows', [])

            for row in rows:
                fact = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        fact[header] = row[i]
                fact['source_page'] = table.get('page_number', -1)
                fact['table_type'] = table.get('detected_type', 'unknown')
                fact['context'] = table.get('context', '')
                facts.append(fact)

        return {
            'available': True,
            'facts': facts,
            'num_facts': len(facts),
            'num_tables': result['num_tables'],
        }

    # ==================================================================
    # Table Structuring
    # ==================================================================
    def _structure_table(self, raw_table: list, page_num: int,
                          page_text: str, table_type: str) -> dict:
        """Convert raw table to structured format with headers."""
        if not raw_table or len(raw_table) < 2:
            return None

        # Clean cells
        cleaned = []
        for row in raw_table:
            cleaned_row = []
            for cell in row:
                if cell is None:
                    cleaned_row.append('')
                else:
                    # Clean whitespace and normalize
                    cleaned_row.append(
                        re.sub(r'\s+', ' ', str(cell).strip()))
            cleaned_row = [c for c in cleaned_row if c]  # Remove empty
            if cleaned_row:
                cleaned.append(cleaned_row)

        if len(cleaned) < 2:
            return None

        # Detect headers (first row with mostly non-numeric content)
        headers = self._detect_headers(cleaned)
        if not headers:
            return None

        # Separate data rows
        data_rows = []
        header_idx = cleaned.index(headers) if headers in cleaned else 0
        for row in cleaned[header_idx + 1:]:
            # Normalize row to match header length
            while len(row) < len(headers):
                row.append('')
            data_rows.append(row[:len(headers)])

        if not data_rows:
            return None

        # Detect table type from context
        detected_type = self._detect_table_type(headers, page_text)

        # Extract surrounding context for RAG
        context = self._extract_context(page_text, raw_table)

        return {
            'headers': headers,
            'rows': data_rows,
            'num_rows': len(data_rows),
            'num_cols': len(headers),
            'page_number': page_num,
            'detected_type': detected_type,
            'context': context[:300],
            # Markdown representation for RAG
            'markdown': self._to_markdown(headers, data_rows),
        }

    def _detect_headers(self, cleaned_rows: list) -> list:
        """Detect the header row in a table."""
        for row in cleaned_rows[:3]:
            # Header row: has text content, not all numbers
            text_cells = sum(1 for c in row
                             if c and not re.match(r'^[\d,.\-()%₹]+$', c))
            if text_cells >= len(row) * 0.4 and len(row) >= 2:
                return row
        # Fallback: first row
        return cleaned_rows[0] if cleaned_rows else None

    def _detect_table_type(self, headers: list, page_text: str) -> str:
        """Detect table type from headers and surrounding text."""
        headers_lower = ' '.join(h.lower() for h in headers)
        text_lower = page_text.lower()

        if any(kw in text_lower for kw in [
            'segment', 'business segment', 'operating segment'
        ]):
            return 'segmental'
        if any(kw in headers_lower for kw in [
            'energy', 'ghg', 'emission', 'water', 'waste', 'brsr'
        ]):
            return 'brsr'
        if 'contingent' in text_lower:
            return 'contingent'
        if 'related party' in text_lower:
            return 'rpt'
        if any(kw in headers_lower for kw in [
            'revenue', 'profit', 'ebitda', 'net income', 'eps'
        ]):
            return 'financial'
        return 'general'

    def _is_relevant_page(self, page_text: str, table_type: str) -> bool:
        """Check if a page is relevant for the requested table type."""
        text_lower = page_text.lower()
        RELEVANCE = {
            'segmental': ['segment', 'business unit', 'division'],
            'brsr': ['brsr', 'business responsibility', 'sustainability',
                     'energy intensity', 'ghg', 'emission'],
            'contingent': ['contingent liabilit', 'disputed', 'pending litigation'],
            'rpt': ['related party', 'transaction with'],
            'financial_overview': ['financial overview', 'ten year',
                                   'financial highlights'],
        }
        keywords = RELEVANCE.get(table_type, [])
        return any(kw in text_lower for kw in keywords)

    def _extract_context(self, page_text: str, raw_table: list) -> str:
        """Extract surrounding context for a table."""
        # Get first cell text to find table location
        first_cell = ''
        for row in raw_table:
            for cell in row:
                if cell:
                    first_cell = str(cell)[:50]
                    break
            if first_cell:
                break

        # Find table location in page text and get surrounding context
        idx = page_text.find(first_cell)
        if idx > 0:
            start = max(0, idx - 200)
            return page_text[start:idx].strip()
        return page_text[:200]

    def _to_markdown(self, headers: list, rows: list) -> str:
        """Convert table to Markdown format for RAG."""
        lines = []
        lines.append('| ' + ' | '.join(headers) + ' |')
        lines.append('|' + '|'.join('---' for _ in headers) + '|')
        for row in rows[:20]:  # Cap rows
            lines.append('| ' + ' | '.join(str(c) for c in row) + ' |')
        return '\n'.join(lines)

    # ==================================================================
    # Specialized Extractors
    # ==================================================================
    def extract_segmental(self, pdf_path: str) -> dict:
        """
        Specialized extractor for segmental revenue tables.
        Returns structured segment data with EBIT and margins.
        """
        result = self.extract_tables(pdf_path, table_type='segmental')
        if not result.get('available') or not result.get('extracted_tables'):
            return result

        segments = []
        for table in result['extracted_tables']:
            headers = table.get('headers', [])
            rows = table.get('rows', [])

            for row in rows:
                if not row:
                    continue
                seg_name = row[0] if row else ''
                if not seg_name or seg_name.lower() in [
                    'total', 'elimination', 'unallocated'
                ]:
                    continue

                seg = {'name': seg_name}
                # Try to extract numeric values
                for i, header in enumerate(headers[1:], 1):
                    if i < len(row):
                        try:
                            val = float(
                                str(row[i]).replace(',', '').replace(
                                    '(', '-').replace(')', ''))
                            header_lower = header.lower()
                            if 'revenue' in header_lower or 'sales' in header_lower:
                                seg['revenue'] = abs(val)
                            elif 'ebit' in header_lower or 'profit' in header_lower:
                                seg['ebit'] = val
                        except (ValueError, TypeError):
                            pass
                segments.append(seg)

        return {
            'available': True,
            'segments': segments,
            'num_segments': len(segments),
        }

    def extract_brsr_metrics(self, pdf_path: str) -> dict:
        """
        Specialized extractor for BRSR (Business Responsibility
        & Sustainability Report) tables.
        """
        facts_result = self.extract_facts(pdf_path, table_type='brsr')
        if not facts_result.get('available'):
            return facts_result

        brsr_metrics = {}
        for fact in facts_result.get('facts', []):
            # Look for key BRSR metrics
            for key, val in fact.items():
                key_lower = key.lower()
                if any(kw in key_lower for kw in [
                    'energy intensity', 'ghg', 'emission', 'water',
                    'waste', 'ltifr', 'fatality', 'renewable',
                    'csr spend', 'women', 'safety',
                ]):
                    try:
                        numeric_val = float(
                            re.sub(r'[^\d.\-]', '', str(val)))
                        brsr_metrics[key] = numeric_val
                    except (ValueError, TypeError):
                        brsr_metrics[key] = val

        return {
            'available': True,
            'metrics': brsr_metrics,
            'num_metrics': len(brsr_metrics),
            'source': 'BRSR tables',
        }
