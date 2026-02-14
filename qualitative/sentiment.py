"""
Sentiment Analysis — FinBERT
================================
Uses FinBERT (yiyanghkust/finbert-tone) to score financial text.

Features:
  • Single / batch text analysis
  • Sentiment Delta between quarters (inflection detection)
  • Section-level scoring for concall transcripts
"""
import re


class SentimentAnalyzer:
    """Compute sentiment scores for financial text using FinBERT."""

    # Max tokens per chunk for FinBERT (512 sub-word ≈ 350 words to be safe)
    MAX_CHUNK_WORDS = 350

    def __init__(self):
        self._model = None
        self._available = False
        try:
            from transformers import pipeline
            self._model = pipeline(
                "text-classification",
                model="yiyanghkust/finbert-tone",
                top_k=None,        # return all class probabilities
                truncation=True,   # auto-truncate to 512 tokens
                max_length=512,
            )
            self._available = True
        except Exception:
            pass

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Single text
    # ------------------------------------------------------------------
    def analyze(self, text: str) -> dict:
        """
        Analyze a single text passage.

        Returns:
            {label, confidence, positive, negative, neutral, score}
            where score = P(positive) − P(negative)  ∈ [−1, 1]
        """
        if not self._available:
            return {
                'available': False,
                'reason': 'FinBERT not installed. '
                          'Run: pip install transformers torch',
            }

        text = text[:2000]   # safety truncation
        results = self._model(text)

        # results is a list of lists: [[{label, score}, ...]]
        probs = {r['label'].lower(): r['score'] for r in results[0]}
        pos = probs.get('positive', 0)
        neg = probs.get('negative', 0)
        neu = probs.get('neutral', 0)
        score = pos - neg

        top_label = max(probs, key=probs.get)

        return {
            'available': True,
            'label': top_label,
            'confidence': round(max(pos, neg, neu), 4),
            'positive': round(pos, 4),
            'negative': round(neg, 4),
            'neutral':  round(neu, 4),
            'score': round(score, 4),    # net sentiment
        }

    # ------------------------------------------------------------------
    # Batch analysis
    # ------------------------------------------------------------------
    def batch_analyze(self, texts: list) -> list:
        """Analyze multiple text passages."""
        return [self.analyze(t) for t in texts]

    # ------------------------------------------------------------------
    # Transcript-level analysis (section scoring)
    # ------------------------------------------------------------------
    def analyze_transcript(self, transcript: str) -> dict:
        """
        Score a full concall transcript.

        Splits into management remarks vs Q&A,
        computes per-section and overall sentiment.
        """
        if not self._available:
            return {'available': False}

        # Split into Management Remarks vs Analyst Q&A
        mgmt_text, qa_text = self._split_mgmt_qa(transcript)

        # Score overall
        chunks = self._smart_chunk(transcript)
        if not chunks:
            return {'available': False, 'reason': 'Transcript too short'}

        scores = []
        for chunk in chunks:
            result = self.analyze(chunk)
            if result.get('available'):
                scores.append(result['score'])

        if not scores:
            return {'available': False, 'reason': 'Could not score any chunks'}

        import numpy as np
        overall_score = round(float(np.mean(scores)), 4)
        score_std = round(float(np.std(scores)), 4)

        # Classify overall tone
        if overall_score > 0.2:
            tone = 'BULLISH'
        elif overall_score > 0.05:
            tone = 'MILDLY POSITIVE'
        elif overall_score > -0.05:
            tone = 'NEUTRAL'
        elif overall_score > -0.2:
            tone = 'CAUTIOUS'
        else:
            tone = 'BEARISH'

        result = {
            'available': True,
            'overall_score': overall_score,
            'tone': tone,
            'score_std': score_std,
            'num_chunks': len(scores),
            'chunk_scores': scores,
        }

        # Score Management Remarks section separately
        if mgmt_text and len(mgmt_text) > 200:
            mgmt_chunks = self._smart_chunk(mgmt_text)
            mgmt_scores = []
            for ch in mgmt_chunks:
                r = self.analyze(ch)
                if r.get('available'):
                    mgmt_scores.append(r['score'])
            if mgmt_scores:
                result['mgmt_score'] = round(float(np.mean(mgmt_scores)), 4)
                result['mgmt_tone'] = self._classify_tone(
                    float(np.mean(mgmt_scores)))

        # Score Analyst Q&A section separately (3x more predictive)
        if qa_text and len(qa_text) > 200:
            qa_chunks = self._smart_chunk(qa_text)
            qa_scores = []
            for ch in qa_chunks:
                r = self.analyze(ch)
                if r.get('available'):
                    qa_scores.append(r['score'])
            if qa_scores:
                result['qa_score'] = round(float(np.mean(qa_scores)), 4)
                result['qa_tone'] = self._classify_tone(
                    float(np.mean(qa_scores)))
                result['qa_num_chunks'] = len(qa_scores)

        return result

    # ------------------------------------------------------------------
    # Split Management Remarks from Analyst Q&A
    # ------------------------------------------------------------------
    @staticmethod
    def _split_mgmt_qa(transcript: str) -> tuple:
        """
        Split concall transcript into (management_remarks, qa_section).

        Concall structure typically:
          1. Operator/Moderator intro
          2. Management prepared remarks
          3. Q&A session (marked by "question-and-answer" or "Q&A" header)
        """
        if not transcript:
            return ('', '')

        # Patterns that mark start of Q&A
        qa_markers = [
            r'question[\s\-]*and[\s\-]*answer\s*session',
            r'Q\s*&\s*A\s*session',
            r'we\s+(?:will|shall)\s+now\s+(?:begin|open|start)\s+'
            r'(?:the\s+)?(?:Q\s*&\s*A|question)',
            r'operator.*?open.*?(?:floor|line).*?question',
            r'first\s+question\s+(?:is\s+)?(?:from|comes)',
            r'(?:ladies\s+and\s+)?gentlemen.*?question',
        ]

        combined = '|'.join(f'(?:{p})' for p in qa_markers)
        match = re.search(combined, transcript, re.IGNORECASE)

        if match:
            mgmt_text = transcript[:match.start()].strip()
            qa_text = transcript[match.start():].strip()
            return (mgmt_text, qa_text)

        # Fallback: look for repeated "question" / "analyst" speaker labels
        lines = transcript.split('\n')
        question_line_idx = None
        for i, line in enumerate(lines):
            if re.search(r'^(?:analyst|question|participant)',
                         line.strip(), re.IGNORECASE):
                question_line_idx = i
                break

        if question_line_idx and question_line_idx > len(lines) // 4:
            mgmt_text = '\n'.join(lines[:question_line_idx])
            qa_text = '\n'.join(lines[question_line_idx:])
            return (mgmt_text, qa_text)

        # No Q&A split found — return all as management
        return (transcript, '')

    # ------------------------------------------------------------------
    # Tone classification helper
    # ------------------------------------------------------------------
    @staticmethod
    def _classify_tone(score: float) -> str:
        """Classify a sentiment score into a tone label."""
        if score > 0.2:
            return 'BULLISH'
        elif score > 0.05:
            return 'MILDLY POSITIVE'
        elif score > -0.05:
            return 'NEUTRAL'
        elif score > -0.2:
            return 'CAUTIOUS'
        return 'BEARISH'

    # ------------------------------------------------------------------
    # Sentiment Delta (inflection point detection)
    # ------------------------------------------------------------------
    @staticmethod
    def compute_delta(current_score: float,
                      previous_score: float) -> dict:
        """
        Compute the Sentiment Delta between two quarters.

        A sudden negative shift often precedes stock decline.
        """
        delta = current_score - previous_score

        if delta < -0.15:
            signal = '🔴 SHARP DECLINE in management confidence'
            severity = 'HIGH'
        elif delta < -0.05:
            signal = '🟡 Noticeable dip in management tone'
            severity = 'MEDIUM'
        elif delta > 0.15:
            signal = '🟢 Significant improvement in management confidence'
            severity = 'POSITIVE'
        elif delta > 0.05:
            signal = '🟢 Mild improvement in tone'
            severity = 'LOW_POSITIVE'
        else:
            signal = '⚪ Sentiment stable quarter-on-quarter'
            severity = 'STABLE'

        return {
            'delta': round(delta, 4),
            'signal': signal,
            'severity': severity,
            'current': round(current_score, 4),
            'previous': round(previous_score, 4),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _smart_chunk(self, text: str) -> list:
        """Split transcript into scoring chunks at paragraph boundaries."""
        # Split on double newlines or speaker markers
        paragraphs = re.split(r'\n\s*\n|\n(?=[A-Z][a-z]+ [A-Z])', text)
        chunks = []
        current = []
        word_count = 0

        for para in paragraphs:
            words = para.split()
            if word_count + len(words) > self.MAX_CHUNK_WORDS:
                if current:
                    chunks.append(' '.join(current))
                current = words
                word_count = len(words)
            else:
                current.extend(words)
                word_count += len(words)

        if current:
            chunks.append(' '.join(current))

        # Filter out very short chunks
        return [c for c in chunks if len(c.split()) > 30]
