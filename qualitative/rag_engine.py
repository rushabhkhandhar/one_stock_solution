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
    def _chunk_text(text: str, size: int, overlap: int) -> list:
        """Split text into overlapping chunks by character count."""
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
