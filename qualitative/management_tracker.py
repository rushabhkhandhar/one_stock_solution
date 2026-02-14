"""
Management Guidance Tracker
============================
Incremental-Chunking strategy:
  1. Chunk current-quarter concall transcript by topic.
  2. Index prior-quarter transcripts with the RAG engine.
  3. For each current chunk, retrieve most similar prior chunks.
  4. Surface changes in management tone / guidance using sentiment delta.

Dependencies: RAGEngine, SentimentAnalyzer
"""
import re
from qualitative.rag_engine import RAGEngine
from qualitative.sentiment import SentimentAnalyzer


# Keywords that signal forward-looking guidance
_GUIDANCE_KEYWORDS = [
    'guidance', 'outlook', 'expect', 'target', 'project',
    'forecast', 'growth', 'margin', 'revenue', 'pipeline',
    'order book', 'capex', 'investment', 'headcount', 'hiring',
    'attrition', 'deal', 'traction', 'momentum', 'demand',
]


class ManagementTracker:
    """Compare management guidance across quarters."""

    def __init__(self):
        self._rag = RAGEngine()
        self._sentiment = SentimentAnalyzer()

    @property
    def available(self) -> bool:
        return True   # RAG + sentiment are in-memory; always available

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def compare_guidance(self, current_transcript: str,
                         prior_transcripts: list) -> dict:
        """
        Compare current quarter guidance against prior quarters.

        Parameters:
            current_transcript  : full text of the latest concall
            prior_transcripts   : list of prior quarter transcripts (str)

        Returns:
            {
                available, comparisons: [
                    {topic, current_snippet, prior_snippet,
                     current_sentiment, prior_sentiment, delta}
                ],
                summary
            }
        """
        if not current_transcript:
            return {'available': False, 'reason': 'No current transcript'}

        if not prior_transcripts:
            return {
                'available': False,
                'reason': 'No prior transcripts for comparison',
            }

        # 1. Index all prior transcripts
        self._rag.clear()
        for idx, prior in enumerate(prior_transcripts):
            self._rag.index_document(
                prior,
                metadata={'quarter_index': idx},
                chunk_size=400,
                overlap=80,
            )

        # 2. Extract guidance-relevant chunks from current transcript
        guidance_chunks = self._extract_guidance_chunks(current_transcript)
        if not guidance_chunks:
            return {
                'available': True,
                'comparisons': [],
                'summary': 'No forward-looking guidance detected in current transcript.',
            }

        # 3. For each guidance chunk, find closest prior chunk
        comparisons = []
        for chunk in guidance_chunks[:10]:   # cap at 10 topics
            hits = self._rag.search(chunk['text'], top_k=1)
            if not hits:
                continue

            prior_text = hits[0]['text']

            # Sentiment scoring
            cur_sent = self._sentiment.analyze(chunk['text'])
            pri_sent = self._sentiment.analyze(prior_text)

            cur_score = cur_sent.get('score', 0.0) if cur_sent.get('available') else 0.0
            pri_score = pri_sent.get('score', 0.0) if pri_sent.get('available') else 0.0
            delta = SentimentAnalyzer.compute_delta(cur_score, pri_score)

            comparisons.append({
                'topic': chunk.get('topic', 'General'),
                'current_snippet': _truncate(chunk['text'], 300),
                'prior_snippet':   _truncate(prior_text, 300),
                'current_sentiment': cur_score,
                'prior_sentiment':   pri_score,
                'delta': delta,
            })

        # 4. Build summary
        deltas = [c['delta']['delta'] for c in comparisons]
        avg_delta = round(sum(deltas) / len(deltas), 4) if deltas else 0

        if avg_delta < -0.10:
            summary = '🔴 Management tone has deteriorated significantly versus prior quarters.'
        elif avg_delta < -0.03:
            summary = '🟡 Slight caution detected in management guidance compared to prior periods.'
        elif avg_delta > 0.10:
            summary = '🟢 Management confidence has improved markedly quarter-on-quarter.'
        elif avg_delta > 0.03:
            summary = '🟢 Marginally more optimistic guidance versus prior quarters.'
        else:
            summary = '⚪ Management tone broadly consistent with prior quarters.'

        return {
            'available': True,
            'comparisons': comparisons,
            'avg_delta': avg_delta,
            'summary': summary,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_guidance_chunks(text: str, max_chunks: int = 15) -> list:
        """Extract paragraphs that contain forward-looking language."""
        paragraphs = re.split(r'\n\s*\n', text)
        guidance = []

        for para in paragraphs:
            lower = para.lower()
            # Count how many guidance keywords hit
            hits = sum(1 for kw in _GUIDANCE_KEYWORDS if kw in lower)
            if hits >= 2 and len(para.split()) > 20:
                # Guess topic from keyword
                topic = 'General Guidance'
                for kw in ['margin', 'revenue', 'growth', 'pipeline',
                           'attrition', 'hiring', 'capex', 'deal',
                           'order book', 'demand']:
                    if kw in lower:
                        topic = kw.title()
                        break
                guidance.append({'text': para.strip(), 'topic': topic})

            if len(guidance) >= max_chunks:
                break

        return guidance


def _truncate(text: str, max_len: int = 300) -> str:
    """Truncate text to max_len chars."""
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(' ', 1)[0] + '…'
