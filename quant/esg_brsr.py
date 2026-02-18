"""
ESG / BRSR Intelligence
========================
Master Plan Items:
  5a — BRSR (Business Responsibility & Sustainability Report) extraction
  5b — Carbon targets, energy intensity, gender diversity

SEBI mandates BRSR for top-1000 listed companies.
Extracts from annual report PDF.
"""
import re
import os
import fitz  # PyMuPDF


class ESGAnalyzer:
    """Extract ESG/BRSR metrics from annual report PDFs."""

    # BRSR section detection patterns
    BRSR_PATTERNS = [
        re.compile(r'business\s+responsibility\s+(?:and\s+)?sustainability',
                   re.IGNORECASE),
        re.compile(r'BRSR', re.IGNORECASE),
        re.compile(r'ESG\s+(?:report|disclosur)', re.IGNORECASE),
        re.compile(r'sustainability\s+report', re.IGNORECASE),
    ]

    # Metric extraction patterns
    METRIC_PATTERNS = {
        'energy_intensity': re.compile(
            r'energy\s+intensity\s*[\s:]+\s*([\d,.]+)\s*'
            r'(?:GJ|MWh|kWh|toe)',
            re.IGNORECASE
        ),
        'ghg_scope1': re.compile(
            r'scope\s*1\s*(?:emissions?)?\s*[\s:]+\s*([\d,.]+)\s*'
            r'(?:tCO2|MT|tonnes?)',
            re.IGNORECASE
        ),
        'ghg_scope2': re.compile(
            r'scope\s*2\s*(?:emissions?)?\s*[\s:]+\s*([\d,.]+)\s*'
            r'(?:tCO2|MT|tonnes?)',
            re.IGNORECASE
        ),
        'water_consumption': re.compile(
            r'(?:total\s+)?water\s+(?:consumption|withdrawal)\s*[\s:]+\s*'
            r'([\d,.]+)\s*(?:KL|kilolitre|ML|megalitre)',
            re.IGNORECASE
        ),
        'waste_generated': re.compile(
            r'(?:total\s+)?waste\s+generated\s*[\s:]+\s*([\d,.]+)\s*'
            r'(?:MT|tonnes?|kg)',
            re.IGNORECASE
        ),
        'women_employees_pct': re.compile(
            r'(?:women|female)\s+(?:employees?|workforce)\s*[\s:]+\s*'
            r'([\d,.]+)\s*%',
            re.IGNORECASE
        ),
        'women_board_pct': re.compile(
            r'(?:women|female)\s+(?:on\s+)?(?:board|directors?)\s*[\s:]+\s*'
            r'([\d,.]+)\s*%',
            re.IGNORECASE
        ),
        'safety_incidents': re.compile(
            r'(?:LTIFR|lost\s+time\s+injury)\s*[\s:]+\s*([\d,.]+)',
            re.IGNORECASE
        ),
        'renewable_energy_pct': re.compile(
            r'(?:renewable|clean)\s+energy\s*[\s:]+\s*([\d,.]+)\s*%',
            re.IGNORECASE
        ),
        'csr_spend': re.compile(
            r'CSR\s+(?:spend|expenditure)\s*[\s:]+\s*'
            r'(?:₹|Rs\.?\s*)?([\d,.]+)\s*(?:crore|cr|lakh)',
            re.IGNORECASE
        ),
    }

    # Carbon target patterns
    CARBON_TARGET_PATTERNS = [
        re.compile(
            r'(?:net\s+zero|carbon\s+neutral|carbon\s+negative)\s*'
            r'(?:by\s+)?(\d{4})',
            re.IGNORECASE
        ),
        re.compile(
            r'reduce\s+(?:carbon|emissions?|GHG)\s+(?:by\s+)?'
            r'(\d+(?:\.\d+)?)\s*%\s*(?:by\s+)?(\d{4})?',
            re.IGNORECASE
        ),
        re.compile(
            r'science.based\s+target|SBTi',
            re.IGNORECASE
        ),
    ]

    # Rule 6: Green-transition keyword patterns — detect forward-
    # looking green CapEx even when current ESG score is low.
    GREEN_TRANSITION_PATTERNS = [
        re.compile(r'green\s+hydrogen|green\s+H[₂2]', re.IGNORECASE),
        re.compile(r'electric\s+vehicle|EV\s+(?:fleet|transition|rollout)',
                   re.IGNORECASE),
        re.compile(r'circular\s+economy', re.IGNORECASE),
        re.compile(r'green\s+bond', re.IGNORECASE),
        re.compile(r'renewable\s+(?:energy\s+)?transition', re.IGNORECASE),
        re.compile(r'solar\s+(?:power|plant|capacity|rooftop)', re.IGNORECASE),
        re.compile(r'battery\s+(?:storage|energy|gigafactory)', re.IGNORECASE),
        re.compile(r'wind\s+(?:energy|farm|turbine)', re.IGNORECASE),
        re.compile(r'sustainability\s+linked\s+(?:loan|bond)', re.IGNORECASE),
        re.compile(r'zero\s+(?:waste|discharge|liquid)', re.IGNORECASE),
        re.compile(r'biodiversity|afforestation|carbon\s+capture',
                   re.IGNORECASE),
    ]

    def analyze(self, ar_parsed: dict, pdf_path: str = None) -> dict:
        """
        Extract ESG/BRSR metrics from annual report.

        Parameters:
            ar_parsed: parsed annual report dict
            pdf_path: optional path to the PDF for deeper scanning

        Returns:
            {available, metrics, carbon_targets, esg_score, brsr_found}
        """
        result = {
            'available': False,
            'brsr_found': False,
            'metrics': {},
            'carbon_targets': [],
            'principles': [],
        }

        # ── Scan for BRSR section in the PDF ──
        brsr_text = ''
        if pdf_path and os.path.exists(pdf_path):
            brsr_text = self._extract_brsr_text(pdf_path)

        if brsr_text:
            result['brsr_found'] = True
            result['available'] = True
        else:
            # Fall back to footnotes and AR text
            all_text = self._collect_ar_text(ar_parsed)
            if any(pat.search(all_text) for pat in self.BRSR_PATTERNS):
                brsr_text = all_text
                result['brsr_found'] = True
                result['available'] = True

        if not brsr_text:
            result['reason'] = 'No BRSR/ESG section found in annual report'
            return result

        # ── Extract metrics ──
        metrics = {}
        for metric_name, pattern in self.METRIC_PATTERNS.items():
            m = pattern.search(brsr_text)
            if m:
                try:
                    val = float(m.group(1).replace(',', ''))
                    metrics[metric_name] = val
                except (ValueError, IndexError):
                    pass

        result['metrics'] = metrics

        # ── Extract carbon targets ──
        targets = []
        for pat in self.CARBON_TARGET_PATTERNS:
            for m in pat.finditer(brsr_text):
                start = max(0, m.start() - 50)
                end = min(len(brsr_text), m.end() + 100)
                context = brsr_text[start:end].replace('\n', ' ').strip()
                targets.append(context)
        result['carbon_targets'] = targets[:5]

        # ── Extract BRSR principles ──
        principles = self._extract_principles(brsr_text)
        result['principles'] = principles

        # ── ESG Score (data-driven: scored from actual disclosures) ──
        score = 0
        # +1 for each metric disclosed (transparency — up to 5)
        score += min(len(metrics), 5)
        # +2 for having carbon targets
        if targets:
            score += 2
        # +1 for renewable energy > median disclosure (any >0% shows commitment)
        if metrics.get('renewable_energy_pct') is not None and metrics['renewable_energy_pct'] > 0:
            score += 1
        # +1 for women on board > 0% (any representation)
        if metrics.get('women_board_pct') is not None and metrics['women_board_pct'] > 0:
            score += 1
        # +1 for BRSR disclosure itself
        if result['brsr_found']:
            score += 1

        result['esg_score'] = min(score, 10)

        # ── Rule 6: Green-transition modifier ──
        # Scan for forward-looking green CapEx keywords even when
        # the current absolute ESG score is low (bottom-decile).
        green_hits = []
        for pat in self.GREEN_TRANSITION_PATTERNS:
            for m in pat.finditer(brsr_text):
                start = max(0, m.start() - 40)
                end = min(len(brsr_text), m.end() + 80)
                snippet = brsr_text[start:end].replace('\n', ' ').strip()
                green_hits.append(snippet)
        # De-duplicate very similar hits
        _seen = set()
        _unique_hits = []
        for h in green_hits:
            _key = h[:50].lower()
            if _key not in _seen:
                _seen.add(_key)
                _unique_hits.append(h)
        result['green_transition_keywords'] = _unique_hits[:8]

        # Transition Phase: ESG ≤ 4 but green CapEx evidence present
        _esg = result['esg_score']
        if _esg <= 4 and len(_unique_hits) >= 1:
            result['transition_phase'] = True
            result['transition_reason'] = (
                f'ESG score {_esg}/10 is bottom-decile but '
                f'{len(_unique_hits)} green-transition keyword(s) '
                f'detected in BRSR/AR text, suggesting active '
                f'decarbonisation CapEx. Score reflects legacy '
                f'footprint, not forward intent.')
            # Modest score uplift: +1 for demonstrable transition
            result['esg_score'] = min(_esg + 1, 10)
        else:
            result['transition_phase'] = False

        return result

    # ------------------------------------------------------------------
    # PDF BRSR text extraction
    # ------------------------------------------------------------------
    def _extract_brsr_text(self, pdf_path: str) -> str:
        """Scan PDF for BRSR section and extract text."""
        try:
            doc = fitz.open(pdf_path)
        except Exception:
            return ''

        brsr_pages = []
        for i in range(doc.page_count):
            text = doc[i].get_text()
            if any(pat.search(text) for pat in self.BRSR_PATTERNS):
                brsr_pages.append(i)

        if not brsr_pages:
            doc.close()
            return ''

        # Extract text from BRSR pages + a few subsequent pages
        texts = []
        start = min(brsr_pages)
        end = min(start + 30, doc.page_count)  # BRSR can span 20+ pages
        for i in range(start, end):
            texts.append(doc[i].get_text())

        doc.close()
        return '\n\n'.join(texts)

    # ------------------------------------------------------------------
    # Collect text from AR parsed data
    # ------------------------------------------------------------------
    @staticmethod
    def _collect_ar_text(ar_parsed: dict) -> str:
        """Collect all available text from parsed AR."""
        texts = []
        for fn in ar_parsed.get('footnotes', []):
            texts.append(fn.get('text', ''))
        for key in ['related_party_summary', 'contingent_liabilities']:
            texts.append(ar_parsed.get(key, ''))
        return '\n\n'.join(texts)

    # ------------------------------------------------------------------
    # BRSR Principles extraction
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_principles(text: str) -> list:
        """
        Extract BRSR's 9 principles assessment.

        BRSR is structured around 9 principles from
        National Guidelines on Responsible Business Conduct (NGRBC).
        """
        principles = []
        # Pattern: "Principle 1:" or "P1:" followed by description
        p_pat = re.compile(
            r'(?:principle|P)\s*(\d)\s*[:\-–]\s*(.{20,200}?)(?:\n|\.)',
            re.IGNORECASE
        )
        for m in p_pat.finditer(text):
            num = int(m.group(1))
            desc = m.group(2).strip()
            if 1 <= num <= 9:
                principles.append({
                    'number': num,
                    'description': desc,
                })

        return principles
