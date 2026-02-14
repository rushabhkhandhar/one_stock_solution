"""
RAG Research Agent — Qualitative Analysis
==========================================
Uses RAGEngine + SentimentAnalyzer + ManagementTracker to produce
a qualitative intelligence summary from concall transcripts and
corporate announcements.
"""
from qualitative.rag_engine import RAGEngine
from qualitative.sentiment import SentimentAnalyzer
from qualitative.management_tracker import ManagementTracker


class RAGAgent:

    def __init__(self):
        self._rag = RAGEngine()
        self._sentiment = SentimentAnalyzer()
        self._mgmt = ManagementTracker()

    @property
    def available(self) -> bool:
        return True

    def run(self, data: dict) -> dict:
        """
        Produce a qualitative intelligence bundle.

        Expects data dict with optional keys:
            concall_texts  : list[str] — full concall transcripts
            announcements  : list[dict] — corporate announcements

        Returns:
            {sentiment, management_delta, key_themes, available}
        """
        result = {'available': True}

        concall_texts = data.get('concall_texts', [])
        announcements = data.get('announcements', [])

        # ----------------------------------------------------------
        # 1. Sentiment on latest concall
        # ----------------------------------------------------------
        if concall_texts:
            latest = concall_texts[0]  # first = most recent (latest-first order)
            result['sentiment'] = self._sentiment.analyze_transcript(latest)
        else:
            # Fall back to announcement text
            ann_text = ' '.join(
                a.get('subject', '') for a in announcements[:20]
            )
            if ann_text.strip():
                result['sentiment'] = self._sentiment.analyze(ann_text)
            else:
                result['sentiment'] = {'available': False,
                                       'reason': 'No text for sentiment'}

        # ----------------------------------------------------------
        # 2. Management guidance delta
        # ----------------------------------------------------------
        if len(concall_texts) >= 2:
            result['management_delta'] = self._mgmt.compare_guidance(
                current_transcript=concall_texts[0],   # latest
                prior_transcripts=concall_texts[1:],    # older ones
            )
        else:
            result['management_delta'] = {
                'available': False,
                'reason': 'Need ≥2 concall transcripts for delta',
            }

        # ----------------------------------------------------------
        # 3. Key themes via RAG search
        # ----------------------------------------------------------
        if concall_texts:
            self._rag.clear()
            for txt in concall_texts:
                self._rag.index_document(txt, metadata={})

            themes = {}
            for query in [
                'revenue growth drivers and outlook',
                'margin improvement or pressure',
                'competitive moat and market share',
                'capital allocation and capex plans',
                'risk factors and challenges',
            ]:
                hits = self._rag.search(query, top_k=2)
                if hits:
                    themes[query] = [h['text'][:300] for h in hits]
            result['key_themes'] = themes
        else:
            result['key_themes'] = {}

        return result
