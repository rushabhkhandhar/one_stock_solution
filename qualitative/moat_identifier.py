"""
Moat Identification Engine
============================
Master Plan Item 3d:
  Detect competitive advantages from:
  - R&D spending (as % of revenue)
  - Patent/IP mentions
  - Market share claims
  - Brand/network effect language
  - Switching cost indicators

Sources: Annual Report text, concall transcripts, footnotes.
"""
import re
import numpy as np


# ── Moat indicator keyword sets ──
MOAT_PATTERNS = {
    'R&D / Innovation': {
        'keywords': [
            'research and development', 'r&d', 'innovation', 'patent',
            'intellectual property', 'proprietary technology',
            'technology platform', 'product development',
        ],
        'weight': 2,
    },
    'Brand Power': {
        'keywords': [
            'brand value', 'brand equity', 'market leader', 'leadership position',
            'premium pricing', 'brand recognition', 'customer loyalty',
            'trusted brand', 'iconic brand',
        ],
        'weight': 2,
    },
    'Network Effect': {
        'keywords': [
            'network effect', 'platform effect', 'ecosystem', 'marketplace',
            'user base', 'active users', 'merchant base', 'flywheel',
        ],
        'weight': 3,
    },
    'Switching Costs': {
        'keywords': [
            'switching cost', 'lock-in', 'long-term contract', 'recurring revenue',
            'subscription', 'retention rate', 'renewal rate',
            'multi-year agreement', 'embedded',
        ],
        'weight': 2,
    },
    'Cost Advantage': {
        'keywords': [
            'cost leadership', 'lowest cost', 'economies of scale',
            'cost advantage', 'operational efficiency', 'cost optimization',
            'lean manufacturing',
        ],
        'weight': 1,
    },
    'Regulatory Moat': {
        'keywords': [
            'license', 'regulatory approval', 'government contract',
            'monopoly', 'exclusive rights', 'concession', 'spectrum',
        ],
        'weight': 2,
    },
    'Market Share': {
        'keywords': [
            'market share', 'market position', 'number one', '#1',
            'largest player', 'dominant position', 'leading position',
        ],
        'weight': 2,
    },
}


class MoatIdentifier:
    """Detect competitive moat indicators from company filings."""

    def analyze(self, ar_parsed: dict, concall_texts: list,
                data: dict) -> dict:
        """
        Scan AR and concall transcripts for moat indicators.

        Returns:
            {
                available, moat_score (0-10), moat_type, indicators,
                r_and_d_pct, patent_count, market_share_claims,
                competitive_advantages
            }
        """
        # Collect all text to scan
        texts = []

        # From concall transcripts
        for t in (concall_texts or []):
            texts.append(t)

        # From AR footnotes
        for fn in ar_parsed.get('footnotes', []):
            texts.append(fn.get('text', ''))

        # From AR sections (RPT, contingent, etc.)
        for key in ['related_party_summary', 'contingent_liabilities']:
            text = ar_parsed.get(key, '')
            if text:
                texts.append(text)

        combined_text = '\n\n'.join(texts)
        if not combined_text or len(combined_text) < 100:
            return {'available': False, 'reason': 'Insufficient text for moat analysis'}

        combined_lower = combined_text.lower()

        # ── Scan for moat indicators ──
        indicators = {}
        total_score = 0
        max_score = 0

        for moat_type, config in MOAT_PATTERNS.items():
            hits = []
            for kw in config['keywords']:
                count = combined_lower.count(kw)
                if count > 0:
                    hits.append({'keyword': kw, 'mentions': count})

            max_score += config['weight']
            if hits:
                # Score proportional to evidence strength
                total_mentions = sum(h['mentions'] for h in hits)
                if total_mentions >= 5:
                    earned = config['weight']
                elif total_mentions >= 2:
                    earned = config['weight'] * 0.6
                else:
                    earned = config['weight'] * 0.3
                total_score += earned

                indicators[moat_type] = {
                    'detected': True,
                    'evidence': hits[:5],
                    'total_mentions': total_mentions,
                }
            else:
                indicators[moat_type] = {
                    'detected': False,
                    'evidence': [],
                    'total_mentions': 0,
                }

        # ── R&D as % of Revenue ──
        rnd_pct = self._extract_rnd_pct(ar_parsed, data)

        # ── Patent mentions ──
        patent_count = len(re.findall(
            r'patent[s]?\s+(?:granted|filed|registered|obtained)',
            combined_lower
        ))
        patent_mentions = combined_lower.count('patent')

        # ── Market share claims ──
        share_claims = self._extract_market_share(combined_text)

        # ── Determine dominant moat type ──
        detected_moats = [
            k for k, v in indicators.items() if v['detected']
        ]
        dominant_moat = None
        if detected_moats:
            dominant_moat = max(
                detected_moats,
                key=lambda k: indicators[k]['total_mentions']
            )

        # Normalize score to 0-10
        moat_score = round(total_score / max_score * 10, 1) if max_score > 0 else 0

        # ── Compile competitive advantages ──
        advantages = []
        for moat_type, info in indicators.items():
            if info['detected']:
                advantages.append(
                    f"**{moat_type}** — {info['total_mentions']} references "
                    f"({', '.join(h['keyword'] for h in info['evidence'][:3])})"
                )

        return {
            'available': True,
            'moat_score': moat_score,
            'dominant_moat': dominant_moat,
            'moat_types_detected': len(detected_moats),
            'indicators': indicators,
            'r_and_d_pct': rnd_pct,
            'patent_mentions': patent_mentions,
            'patent_grants': patent_count,
            'market_share_claims': share_claims,
            'competitive_advantages': advantages,
        }

    # ------------------------------------------------------------------
    # R&D extraction
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_rnd_pct(ar_parsed: dict, data: dict) -> float:
        """Extract R&D spending as percentage of revenue."""
        # Check footnotes for R&D disclosure
        for fn in ar_parsed.get('footnotes', []):
            title = fn.get('title', '').lower()
            text = fn.get('text', '')
            if any(kw in title for kw in ['research', 'r&d', 'development']):
                # Try to extract amount
                amounts = re.findall(
                    r'([\d,]+(?:\.\d+)?)\s*(?:crore|cr)',
                    text, re.IGNORECASE
                )
                if amounts:
                    try:
                        rnd_amount = float(amounts[0].replace(',', ''))
                        pnl = data.get('pnl')
                        if pnl is not None and not pnl.empty:
                            from data.preprocessing import DataPreprocessor, get_value
                            pp = DataPreprocessor()
                            revenue = get_value(pp.get(pnl, 'sales'))
                            if not np.isnan(revenue) and revenue > 0:
                                return round(rnd_amount / revenue * 100, 2)
                    except (ValueError, IndexError):
                        pass
        return None

    # ------------------------------------------------------------------
    # Market share claim extraction
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_market_share(text: str) -> list:
        """Extract market share claims from text."""
        claims = []
        patterns = [
            re.compile(
                r'(?:market\s+share\s+(?:of\s+)?)'
                r'(\d+(?:\.\d+)?)\s*%',
                re.IGNORECASE
            ),
            re.compile(
                r'(?:#1|number\s+one|largest|leading)\s+(?:player\s+)?'
                r'(?:in|across)\s+(.{10,60}?)(?:\.|,|\n)',
                re.IGNORECASE
            ),
        ]

        for pat in patterns:
            for m in pat.finditer(text):
                start = max(0, m.start() - 50)
                end = min(len(text), m.end() + 50)
                context = text[start:end].replace('\n', ' ').strip()
                claims.append(context)

        return claims[:10]  # Cap at 10 claims
