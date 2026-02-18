"""
Text Intelligence Engine
=========================
Unified module to analyze ALL text data from:
  • Concall transcripts
  • Annual report parsed sections
  • Corporate announcements / press releases

Produces:
  1. Key Insights summary (company plans, status, risks, opportunities)
  2. Topic extraction (capex, guidance, M&A, debt, dividends, etc.)
  3. Forward-looking statement detection
  4. Risk/opportunity classification

Uses keyword matching + FinBERT sentiment for each topic.
No LLM — pure NLP heuristics.
"""
import re
from collections import defaultdict


# ─── Topic definitions with keyword patterns ─────────────────────────

TOPIC_PATTERNS = {
    'Revenue & Growth': [
        r'revenue\s+growth', r'top[\s-]?line', r'sales\s+growth',
        r'order\s+book', r'order\s+inflow', r'revenue\s+guidance',
        r'yoy\s+growth', r'growth\s+trajectory', r'market\s+share',
    ],
    'Profitability': [
        r'margin\s+expansion', r'margin\s+improvement', r'ebitda\s+margin',
        r'operating\s+margin', r'cost\s+reduction', r'cost\s+optimization',
        r'profitability', r'net\s+profit', r'bottom[\s-]?line',
    ],
    'Capex & Expansion': [
        r'capex', r'capital\s+expenditure', r'expansion\s+plan',
        r'new\s+plant', r'capacity\s+(addition|expansion|utilization)',
        r'greenfield', r'brownfield', r'commissioning',
    ],
    'Debt & Leverage': [
        r'debt\s+reduction', r'deleveraging', r'debt[\s-]?free',
        r'net\s+debt', r'leverage\s+ratio', r'interest\s+cost',
        r'borrowing', r'credit\s+rating', r'refinanc',
    ],
    'Dividend & Shareholder Returns': [
        r'dividend', r'buyback', r'shareholder\s+return',
        r'payout\s+ratio', r'special\s+dividend',
    ],
    'M&A / Joint Ventures': [
        r'acquisition', r'merger', r'joint\s+venture', r'strategic\s+partnership',
        r'divestment', r'subsidiary', r'stake\s+sale', r'buyout',
    ],
    'Regulatory & Compliance': [
        r'sebi', r'regulatory', r'compliance', r'government\s+policy',
        r'tax\s+(benefit|incentive)', r'gst', r'customs\s+duty', r'tariff',
        r'anti[\s-]?dumping', r'litigation', r'penalty', r'fine\s+imposed',
    ],
    'ESG & Sustainability': [
        r'esg', r'sustainability', r'carbon\s+(neutral|emission|footprint)',
        r'renewable\s+energy', r'green\s+hydrogen', r'net[\s-]?zero',
        r'brsr', r'csr\s+spend', r'water\s+conservation',
    ],
    'Technology & Innovation': [
        r'digital\s+transformation', r'ai\b', r'artificial\s+intelligence',
        r'automation', r'r\s*&\s*d', r'patent', r'innovation',
        r'new\s+product', r'technology\s+platform',
    ],
    'Management Outlook': [
        r'guidance', r'outlook', r'going\s+forward', r'next\s+(quarter|year)',
        r'fy\s*\d{2,4}', r'full[\s-]?year', r'medium[\s-]?term', r'long[\s-]?term',
        r'confident', r'optimistic', r'cautious', r'challenging',
    ],
    'Risk Factors': [
        r'risk', r'headwind', r'challenge', r'uncertain', r'slowdown',
        r'volatility', r'geopolitical', r'recession', r'inflation',
        r'supply\s+chain\s+disruption', r'attrition', r'competition',
    ],
}

FORWARD_LOOKING_PATTERNS = [
    r'going\s+forward', r'expect\s+to', r'plan\s+to', r'anticipat',
    r'likely\s+to', r'targeting', r'guidance', r'roadmap',
    r'next\s+(quarter|year|fy)', r'fy\s*\d{2,4}', r'medium[\s-]?term',
    r'outlook', r'pipeline', r'upcoming', r'intend\s+to',
    r'we\s+believe', r'we\s+see', r'positioned\s+to',
]


class TextIntelligenceEngine:
    """
    Analyze all text sources and produce structured intelligence.
    """

    def __init__(self):
        self._sentiment = None

    def _get_sentiment(self):
        """Lazy-load FinBERT so import doesn't fail without torch."""
        if self._sentiment is None:
            try:
                from qualitative.sentiment import SentimentAnalyzer
                self._sentiment = SentimentAnalyzer()
            except Exception:
                self._sentiment = None
        return self._sentiment

    def analyze(self, concall_texts: list = None,
                ar_parsed: dict = None,
                announcements: list = None) -> dict:
        """
        Unified text analysis.

        Parameters
        ----------
        concall_texts : list[str] — raw concall transcript texts
        ar_parsed     : dict from PDFParser — has 'sections' dict
        announcements : list[str] — corporate announcement texts

        Returns
        -------
        dict with:
            available, insights (list), topic_analysis (dict),
            forward_looking (list), company_status, plans, risks,
            opportunities, overall_tone
        """
        all_texts = []

        # Import transcript cleaner
        from qualitative.rag_engine import RAGEngine
        _clean = RAGEngine.clean_transcript_noise

        # Collect all text sources
        if concall_texts:
            for t in concall_texts:
                if isinstance(t, str) and len(t) > 50:
                    all_texts.append(('concall', _clean(t)))

        if ar_parsed and isinstance(ar_parsed, dict):
            # Footnotes — each has 'text' field with actual content
            for fn in ar_parsed.get('footnotes', []):
                fn_text = fn.get('text', '')
                if isinstance(fn_text, str) and len(fn_text) > 50:
                    all_texts.append(('annual_report', fn_text))

            # Contingent liabilities — raw text string
            contingent = ar_parsed.get('contingent_liabilities', '')
            if isinstance(contingent, str) and len(contingent) > 50:
                all_texts.append(('annual_report', contingent))

            # Related party summary — raw text string
            rpt_text = ar_parsed.get('related_party_summary', '')
            if isinstance(rpt_text, str) and len(rpt_text) > 50:
                all_texts.append(('annual_report', rpt_text))

            # Auditor observations — list of strings
            for obs in ar_parsed.get('auditor_observations', []):
                obs_text = obs if isinstance(obs, str) else obs.get('context', '') if isinstance(obs, dict) else ''
                if isinstance(obs_text, str) and len(obs_text) > 30:
                    all_texts.append(('annual_report', obs_text))

            # Also raw text if nothing extracted above
            if not any(src == 'annual_report' for src, _ in all_texts):
                raw = ar_parsed.get('raw_text', '')
                if raw and len(raw) > 100:
                    for chunk in self._chunk_text(raw, 3000):
                        all_texts.append(('annual_report', chunk))

        if announcements:
            for ann in announcements:
                text = ann if isinstance(ann, str) else ann.get('text', '')
                if len(text) > 30:
                    all_texts.append(('announcement', text))

        if not all_texts:
            return {'available': False, 'reason': 'No text data available'}

        # Run analysis
        combined_text = "\n\n".join(t[1] for t in all_texts)

        # 1. Topic analysis
        topic_analysis = self._extract_topics(combined_text, all_texts)

        # 2. Forward-looking statements
        forward_looking = self._extract_forward_looking(combined_text)

        # 3. Key insights by category
        company_status = self._extract_category_insights(
            topic_analysis, ['Revenue & Growth', 'Profitability',
                             'Debt & Leverage'])
        plans = self._extract_category_insights(
            topic_analysis, ['Capex & Expansion', 'M&A / Joint Ventures',
                             'Technology & Innovation'])
        risks = self._extract_category_insights(
            topic_analysis, ['Risk Factors', 'Regulatory & Compliance'])
        opportunities = self._extract_category_insights(
            topic_analysis, ['Management Outlook', 'ESG & Sustainability',
                             'Revenue & Growth'])

        # 4. Sentiment per topic (if FinBERT available)
        sa = self._get_sentiment()
        if sa and sa.available:
            for topic, info in topic_analysis.items():
                snippets = info.get('key_sentences', [])
                if snippets:
                    # Score the top 3 snippets
                    top = snippets[:3]
                    scores = []
                    for s in top:
                        res = sa.analyze(s)
                        if res.get('available'):
                            scores.append(res.get('score', 0))
                    if scores:
                        avg_score = sum(scores) / len(scores)
                        info['sentiment_score'] = round(avg_score, 4)
                        info['sentiment_tone'] = (
                            'POSITIVE' if avg_score > 0.15 else
                            ('NEGATIVE' if avg_score < -0.15 else 'NEUTRAL'))

        # 5. Overall tone from topic sentiments
        topic_sentiments = [v.get('sentiment_score', 0)
                            for v in topic_analysis.values()
                            if 'sentiment_score' in v]
        overall_tone = 'NEUTRAL'
        overall_score = 0
        if topic_sentiments:
            overall_score = round(sum(topic_sentiments) / len(topic_sentiments), 4)
            if overall_score > 0.15:
                overall_tone = 'POSITIVE'
            elif overall_score < -0.15:
                overall_tone = 'NEGATIVE'

        # 6. Generate summary insights
        insights = self._generate_insights(topic_analysis, forward_looking)

        return {
            'available': True,
            'num_sources': len(all_texts),
            'source_breakdown': {
                'concall': sum(1 for s, _ in all_texts if s == 'concall'),
                'annual_report': sum(1 for s, _ in all_texts if s == 'annual_report'),
                'announcement': sum(1 for s, _ in all_texts if s == 'announcement'),
            },
            'insights': insights,
            'topic_analysis': topic_analysis,
            'forward_looking': forward_looking[:10],
            'company_status': company_status,
            'plans': plans,
            'risks': risks,
            'opportunities': opportunities,
            'overall_tone': overall_tone,
            'overall_score': overall_score,
        }

    # ==================================================================
    # Topic Extraction
    # ==================================================================
    def _extract_topics(self, combined_text: str,
                        all_texts: list) -> dict:
        """Extract mentions and key sentences for each topic."""
        topic_analysis = {}
        sentences = self._split_sentences(combined_text)

        for topic, patterns in TOPIC_PATTERNS.items():
            matching_sentences = []
            mention_count = 0
            for sent in sentences:
                sent_lower = sent.lower()
                for pat in patterns:
                    if re.search(pat, sent_lower):
                        mention_count += 1
                        if len(sent) > 30 and sent not in matching_sentences:
                            matching_sentences.append(sent.strip())
                        break

            if mention_count > 0:
                # Deduplicate and pick best sentences
                key_sentences = self._pick_best_sentences(
                    matching_sentences, max_n=5)
                topic_analysis[topic] = {
                    'mention_count': mention_count,
                    'key_sentences': key_sentences,
                    'coverage': 'HIGH' if mention_count > 10 else
                                ('MEDIUM' if mention_count > 3 else 'LOW'),
                }

        return topic_analysis

    # ==================================================================
    # Forward-Looking Statements
    # ==================================================================
    def _extract_forward_looking(self, text: str) -> list:
        """Find forward-looking statements."""
        sentences = self._split_sentences(text)
        forward = []
        for sent in sentences:
            sent_lower = sent.lower()
            for pat in FORWARD_LOOKING_PATTERNS:
                if re.search(pat, sent_lower):
                    cleaned = sent.strip()
                    if 30 < len(cleaned) < 500 and cleaned not in forward:
                        forward.append(cleaned)
                    break
        # Sort by length (longer = more informative)
        forward.sort(key=len, reverse=True)
        return forward[:15]

    # ==================================================================
    # Category Insights
    # ==================================================================
    def _extract_category_insights(self, topic_analysis: dict,
                                   categories: list) -> list:
        """Pull top insights from specific topic categories."""
        insights = []
        for cat in categories:
            info = topic_analysis.get(cat, {})
            for sent in info.get('key_sentences', [])[:2]:
                insights.append(sent)
        return insights[:8]

    # ==================================================================
    # Generate Summary Insights
    # ==================================================================
    def _generate_insights(self, topic_analysis: dict,
                           forward_looking: list) -> list:
        """Produce bullet-point insights from all analysis."""
        insights = []

        # Coverage summary
        active_topics = sorted(
            [(t, v['mention_count']) for t, v in topic_analysis.items()],
            key=lambda x: -x[1])

        if active_topics:
            top_topics = [t for t, _ in active_topics[:3]]
            insights.append(
                f"Key discussion topics: {', '.join(top_topics)}")

        # Per-topic insights (pick the best sentence from each high-coverage topic)
        for topic, info in topic_analysis.items():
            if info.get('coverage') in ('HIGH', 'MEDIUM'):
                sents = info.get('key_sentences', [])
                tone = info.get('sentiment_tone', '')
                tone_tag = f" [{tone}]" if tone else ""
                if sents:
                    snippet = self._smart_truncate(sents[0], 500)
                    insights.append(f"**{topic}**{tone_tag}: {snippet}")

        # Forward-looking highlights
        if forward_looking:
            snippet = self._smart_truncate(forward_looking[0], 500)
            insights.append(
                f"**Forward Guidance**: {snippet}")

        return insights

    # ==================================================================
    # Utility
    # ==================================================================
    def _split_sentences(self, text: str) -> list:
        """Split text into sentences, respecting abbreviations."""
        # Clean up common noise
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n{2,}', '. ', text)
        # Protect common abbreviations & decimals from splitting
        _PLACEHOLDER = '\u200B'  # zero-width space as safe placeholder
        text = re.sub(
            r'(\b(?:Mr|Mrs|Ms|Dr|Sr|Jr|Prof|Inc|Ltd|Co|Corp|vs|etc|Rs|approx))\. ',
            lambda m: m.group(1) + _PLACEHOLDER + ' ', text)
        # Protect i.e. and e.g.
        text = text.replace('i.e. ', 'i.e' + _PLACEHOLDER + ' ')
        text = text.replace('e.g. ', 'e.g' + _PLACEHOLDER + ' ')
        # Protect decimals like 0.9 million
        text = re.sub(r'(\d)\. (\d)', lambda m: m.group(1) + _PLACEHOLDER + ' ' + m.group(2), text)
        # Split on sentence-ending punctuation
        sentences = re.split(r'(?<=[.!?])\s+', text)
        # Restore protected dots
        sentences = [s.replace(_PLACEHOLDER, '.') for s in sentences]
        return [s.strip() for s in sentences if len(s.strip()) > 20]

    def _chunk_text(self, text: str, chunk_size: int = 3000) -> list:
        """Split large text into overlapping chunks."""
        words = text.split()
        chunks = []
        for i in range(0, len(words), chunk_size - 200):
            chunk = ' '.join(words[i:i + chunk_size])
            if len(chunk) > 50:
                chunks.append(chunk)
        return chunks

    def _pick_best_sentences(self, sentences: list,
                             max_n: int = 5) -> list:
        """Pick the most informative sentences (by length and uniqueness)."""
        if not sentences:
            return []
        # Remove very short and very long
        filtered = [s for s in sentences if 40 < len(s) < 500]
        if not filtered:
            filtered = sentences[:max_n]
        # Sort by length (medium-length = most informative)
        filtered.sort(key=lambda s: -min(len(s), 300))
        return filtered[:max_n]

    @staticmethod
    def _smart_truncate(text: str, max_chars: int = 300) -> str:
        """Return only *complete* sentences that fit within *max_chars*.

        If no full sentence fits, take the first sentence (even if it
        exceeds the budget slightly) and append an ellipsis.
        """
        import re as _re
        # Clean transcript noise
        try:
            from qualitative.rag_engine import RAGEngine
            text = RAGEngine.clean_transcript_noise(text)
        except Exception:
            pass
        text = text.strip()
        if len(text) <= max_chars:
            return text

        sentences = _re.split(r'(?<=[.!?])\s+', text)
        result = ''
        for sent in sentences:
            candidate = (result + ' ' + sent).strip() if result else sent
            if len(candidate) <= max_chars:
                result = candidate
            else:
                break
        if result:
            return result

        # No complete sentence fits — trim the first sentence
        first = sentences[0] if sentences else text
        if len(first) <= max_chars:
            return first
        window = first[:max_chars]
        for delim in ['. ', '; ', ', ']:
            idx = window.rfind(delim)
            if idx > max_chars * 0.35:
                return window[:idx + 1].strip()
        idx = window.rfind(' ')
        if idx > 0:
            return window[:idx].rstrip('.,;:!?') + ' \u2026'
        return window
