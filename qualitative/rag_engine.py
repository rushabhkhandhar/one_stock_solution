"""
Hybrid RAG Engine — Dense + BM25 with RRF Fusion
==================================================
Architecture:
  • sentence-transformers for dense embeddings (in-memory FAISS-like)
  • BM25 sparse index for exact keyword matching
  • Reciprocal Rank Fusion (RRF) to merge ranked results

Fully local — no external vector DB needed.
"""
import hashlib
import re
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    from rank_bm25 import BM25Okapi
    _DEPS_AVAILABLE = True
except ImportError:
    _DEPS_AVAILABLE = False


class RAGEngine:
    """
    Retrieval-Augmented Generation over financial documents.

    Indexes document chunks with both dense vectors and BM25,
    then fuses results with Reciprocal Rank Fusion (RRF).
    """

    # RRF fusion constant (standard value from literature)
    RRF_K = 60

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self._available = _DEPS_AVAILABLE
        self._model = None
        self._model_name = model_name

        # In-memory stores
        self._chunks = []        # list of {text, metadata, id}
        self._embeddings = None  # np.array  (N × dim)
        self._bm25 = None       # BM25Okapi instance
        self._tokenized = []     # tokenized corpus for BM25

    @property
    def available(self) -> bool:
        return self._available

    def _ensure_model(self):
        """Lazy-load the embedding model on first use."""
        if self._model is None and self._available:
            print("    ⏳ Loading embedding model …")
            self._model = SentenceTransformer(self._model_name)

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------
    def index_document(self, text: str, metadata: dict = None,
                       chunk_size: int = 500, overlap: int = 100) -> int:
        """
        Chunk a document and add to both dense + sparse indices.

        Returns: number of chunks added.
        """
        if not self._available:
            return 0

        self._ensure_model()
        text = self.clean_transcript_noise(text)
        chunks = self._chunk_text(text, chunk_size, overlap)
        added = 0

        for chunk in chunks:
            doc_id = hashlib.md5(chunk.encode()).hexdigest()[:12]
            entry = {
                'id': doc_id,
                'text': chunk,
                'metadata': metadata or {},
            }
            self._chunks.append(entry)
            self._tokenized.append(self._tokenize(chunk))
            added += 1

        # Rebuild indices
        self._rebuild_indices()
        return added

    def index_chunks(self, chunks: list) -> int:
        """
        Add pre-chunked documents.

        Each chunk: {'text': str, 'metadata': dict}
        """
        if not self._available:
            return 0

        self._ensure_model()
        added = 0
        for ch in chunks:
            doc_id = hashlib.md5(ch['text'].encode()).hexdigest()[:12]
            entry = {
                'id': doc_id,
                'text': ch['text'],
                'metadata': ch.get('metadata', {}),
            }
            self._chunks.append(entry)
            self._tokenized.append(self._tokenize(ch['text']))
            added += 1

        self._rebuild_indices()
        return added

    def _rebuild_indices(self):
        """Rebuild dense embeddings + BM25 index."""
        if not self._chunks:
            return
        texts = [c['text'] for c in self._chunks]
        self._embeddings = self._model.encode(texts,
                                               show_progress_bar=False)
        self._bm25 = BM25Okapi(self._tokenized)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search(self, query: str, top_k: int = 5) -> list:
        """
        Hybrid search: dense + BM25 with Reciprocal Rank Fusion.

        Returns: list of {text, metadata, score, rank}
        """
        if not self._available or not self._chunks:
            return []

        self._ensure_model()

        # 1. Dense search
        q_emb = self._model.encode([query], show_progress_bar=False)
        sims = np.dot(self._embeddings, q_emb.T).flatten()
        dense_order = np.argsort(-sims)

        # 2. BM25 sparse search
        q_tokens = self._tokenize(query)
        bm25_scores = self._bm25.get_scores(q_tokens)
        sparse_order = np.argsort(-bm25_scores)

        # 3. Reciprocal Rank Fusion
        rrf_scores = {}
        for rank, idx in enumerate(dense_order):
            rrf_scores[idx] = rrf_scores.get(idx, 0) + 1.0 / (self.RRF_K + rank + 1)
        for rank, idx in enumerate(sparse_order):
            rrf_scores[idx] = rrf_scores.get(idx, 0) + 1.0 / (self.RRF_K + rank + 1)

        # Sort by fused score
        ranked = sorted(rrf_scores.items(), key=lambda x: -x[1])[:top_k]

        results = []
        for rank, (idx, score) in enumerate(ranked):
            chunk = self._chunks[idx]
            results.append({
                'text': chunk['text'],
                'metadata': chunk['metadata'],
                'score': round(score, 4),
                'rank': rank + 1,
            })
        return results

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def clear(self):
        """Clear all indexed data."""
        self._chunks = []
        self._embeddings = None
        self._bm25 = None
        self._tokenized = []

    @property
    def chunk_count(self) -> int:
        return len(self._chunks)

    @staticmethod
    def clean_transcript_noise(text: str) -> str:
        """Strip common PDF artefacts from BSE concall transcript text.

        Handles both full-document cleaning and snippet-level cleaning.
        Safe to call multiple times (idempotent).
        """
        if not text:
            return text

        # ── Full-document preamble removal ────────────────────
        # Cut everything before the actual transcript body
        for marker_re in [
            r'Media\s*&?\s*Analyst\s+Call\s+Transcript',
            r'Concall\s+Transcript',
            r'Earnings\s+Call\s+Transcript',
        ]:
            m = re.search(marker_re, text, re.IGNORECASE)
            if m:
                text = text[m.end():]
                break

        # Remove digital-signature blocks
        text = re.sub(
            r'Digitally\s+signed\s+by.*?[\+\-]\d{2}\'\d{2}\'',
            '', text, flags=re.DOTALL)

        # ── Page-number + copyright footer (separate lines) ──
        #    "42\n© Reliance Industries Limited 2020"
        text = re.sub(
            r'^\s*\d{1,3}\s*\n\s*©.*?(?:Limited|Ltd\.?)\s*\d{4}\s*$',
            '', text, flags=re.MULTILINE)
        # Standalone copyright line
        text = re.sub(
            r'^\s*©.*?(?:Limited|Ltd\.?)\s*\d{4}\s*$',
            '', text, flags=re.MULTILINE)

        # ── Inline page+copyright that lands mid-sentence ────
        #    "...margins of 50% 8 © Reliance Industries Limited 2020 and this..."
        text = re.sub(
            r'\s*\d{1,3}\s+©\s+\w[\w\s]*?(?:Limited|Ltd\.?)\s*\d{4}\s*',
            ' ', text)

        # ── Speaker / questioner labels (own line) ───────────
        text = re.sub(
            r'^\s*Company\s+Speaker\s*\([^)]*\)\s*$',
            '', text, flags=re.MULTILINE)
        text = re.sub(
            r'^\s*Questioner\s*\([^)]*\)\s*$',
            '', text, flags=re.MULTILINE)

        # ── Inline speaker prefix at start of a snippet ──────
        text = re.sub(
            r'^\s*(?:Company\s+Speaker|Questioner)\s*\([^)]*\)\s*',
            '', text)

        # ── Inline speaker/questioner labels anywhere in text ─
        text = re.sub(
            r'\s*(?:Company\s+Speaker|Questioner)\s*\([^)]*\)\s*',
            ' ', text)

        # ── Trailing ")" from a cut speaker name, e.g. "Srikanth) So we..." ──
        text = re.sub(r'^[A-Z][a-zA-Z .]+\)\s+', '', text)

        # ── Timestamp headers: "Name HH:MM:SS – HH:MM:SS (Topic)" ─
        text = re.sub(
            r'^.*?\d{1,2}:\d{2}:\d{2}\s*[–\-]\s*\d{1,2}:\d{2}:\d{2}.*$',
            '', text, flags=re.MULTILINE)

        # ── "RIL Q3 2025-2026" style headers ─────────────────
        text = re.sub(
            r'^\s*RIL\s+Q\d.*?\d{4}\s*$', '', text, flags=re.MULTILINE)

        # ── Bare page numbers on their own line ──────────────
        text = re.sub(r'^\s*\d{1,3}\s*$', '', text, flags=re.MULTILINE)

        # ── Collapse whitespace ──────────────────────────────
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'  +', ' ', text)
        return text.strip()

    @staticmethod
    def smart_truncate(text: str, max_chars: int = 300,
                        ellipsis: bool = True) -> str:
        """Truncate text at the nearest sentence boundary.

        Prefers cutting after a full stop / question-mark / exclamation.
        Falls back to the last comma / semicolon, then last word boundary.
        Never cuts mid-word.
        """
        # Strip any residual transcript noise from the snippet
        text = RAGEngine.clean_transcript_noise(text)
        if len(text) <= max_chars:
            return text

        window = text[:max_chars]
        # Try sentence boundary (.!?) — search backward from the limit
        for delim in ['. ', '? ', '! ']:
            idx = window.rfind(delim)
            if idx > max_chars * 0.35:          # must keep ≥35 % of budget
                return window[:idx + 1].strip()

        # Try clause boundary (, ; — )
        for delim in [', ', '; ', ' -- ', ' - ']:
            idx = window.rfind(delim)
            if idx > max_chars * 0.4:
                result = window[:idx + 1].strip()
                return result + (' …' if ellipsis else '')

        # Last resort: word boundary
        idx = window.rfind(' ')
        if idx > 0:
            result = window[:idx].rstrip('.,;:!?')
            return result + (' …' if ellipsis else '')

        return window  # shouldn't happen

    @staticmethod
    def _chunk_text(text: str, size: int, overlap: int) -> list:
        """Split text into overlapping chunks by word count."""
        words = text.split()
        chunks = []
        i = 0
        while i < len(words):
            chunk_words = words[i:i + size]
            chunks.append(' '.join(chunk_words))
            i += size - overlap
        return [c for c in chunks if len(c.strip()) > 50]

    @staticmethod
    def _tokenize(text: str) -> list:
        """Simple whitespace + lowercase tokenization for BM25."""
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        return text.split()
