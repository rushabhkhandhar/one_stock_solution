"""
Say-Do Ratio — Management Credibility Tracker
===============================================
Institutional analysts measure management's credibility by tracking
the Say-Do Ratio: how often management meets its own guidance.

Methodology:
  1. Extract forward-looking guidance from transcripts 4-8 quarters ago
     (e.g., "we expect 15% margin expansion", "targeting ₹50,000 Cr revenue")
  2. Parse numerical targets (%, ₹ amounts, growth rates)
  3. Cross-reference with actual quarterly/annual results from scraper
  4. Calculate: Say-Do Ratio = Achieved / Promised
  5. Flag Governance Risk if Say-Do < 0.8

Signal:
  Say-Do ≥ 1.0  →  Management exceeds promises (bullish)
  0.8 ≤ SD < 1.0 →  Largely meets guidance (neutral)
  SD < 0.8       →  Governance risk — management over-promises
"""
import re
from typing import Optional
from collections import defaultdict


# ─── Patterns for extracting numerical guidance ──────────────────────

# "expect|target|guide|anticipate X% growth/margin/EBITDA"
_PCT_GUIDANCE = re.compile(
    r'(?:expect|target|guid|anticipat|aim|project|forecast|achiev)'
    r'[a-z]*\s+.*?'
    r'(\d{1,3}(?:\.\d+)?)\s*(?:%|percent|basis\s+point|bps)',
    re.IGNORECASE
)

# "revenue of ₹X,XXX crore"
_AMOUNT_GUIDANCE = re.compile(
    r'(?:expect|target|guid|anticipat|aim|project|forecast|achiev)'
    r'[a-z]*\s+.*?'
    r'(?:₹|Rs\.?\s*|INR\s*)?'
    r'([\d,]+(?:\.\d+)?)\s*'
    r'(?:crore|cr\.?|billion|lakh)',
    re.IGNORECASE
)

# "grow by X%", "growth of X%"
_GROWTH_GUIDANCE = re.compile(
    r'(?:grow|growth|increase|expand)'
    r'[a-z]*\s+.*?'
    r'(\d{1,3}(?:\.\d+)?)\s*(?:%|percent)',
    re.IGNORECASE
)

# "margin of X%", "EBITDA margin X%"
_MARGIN_GUIDANCE = re.compile(
    r'(?:ebitda|operating|net\s+profit|pat|opm|gross)\s*margin'
    r'[a-z\s]*?'
    r'(\d{1,3}(?:\.\d+)?)\s*(?:%|percent)',
    re.IGNORECASE
)

# "X% CAGR", "X% year-on-year"
_CAGR_GUIDANCE = re.compile(
    r'(\d{1,3}(?:\.\d+)?)\s*(?:%|percent)\s*'
    r'(?:cagr|yoy|year.on.year|annually)',
    re.IGNORECASE
)

# Topic context patterns
_GUIDANCE_TOPICS = {
    'Revenue Growth': [
        r'revenue\s+growth', r'top[\s-]?line\s+growth', r'sales\s+growth',
        r'revenue\s+target', r'order\s+book',
    ],
    'Margin': [
        r'ebitda\s+margin', r'operating\s+margin', r'opm',
        r'margin\s+expansion', r'margin\s+improvement',
    ],
    'Profitability': [
        r'profit\s+growth', r'pat\s+growth', r'earnings\s+growth',
        r'bottom[\s-]?line',
    ],
    'Capex': [
        r'capex', r'capital\s+expenditure', r'investment\s+plan',
    ],
    'Debt': [
        r'debt\s+reduction', r'deleveraging', r'net\s+debt',
        r'leverage',
    ],
    'Revenue Target': [
        r'revenue\s+of', r'revenue\s+target', r'targeting\s+revenue',
        r'top[\s-]?line\s+of',
    ],
}


class SayDoTracker:
    """
    Track management's Say-Do Ratio across quarters.

    Usage:
        tracker = SayDoTracker()
        result = tracker.analyze(concall_texts, data)
    """

    def analyze(self, concall_texts: list, data: dict) -> dict:
        """
        Analyze management credibility via Say-Do Ratio.

        Parameters
        ----------
        concall_texts : list[str]
            Concall transcripts, ordered newest-first.
            Index 0 = latest, index -1 = oldest.
        data : dict
            Full data dict with pnl, balance_sheet etc.

        Returns
        -------
        dict with say_do_ratio, guidance_items, credibility_rating
        """
        if not concall_texts or len(concall_texts) < 2:
            return {
                'available': False,
                'reason': 'Need ≥2 transcripts to compute Say-Do ratio',
            }

        # ── Step 1: Extract guidance from older transcripts ──
        # Use transcripts[1:] (prior quarters) for guidance extraction
        prior_guidances = []
        for idx, transcript in enumerate(concall_texts[1:], start=1):
            if not isinstance(transcript, str) or len(transcript) < 100:
                continue
            guidances = self._extract_guidance(transcript)
            for g in guidances:
                g['source_quarter_offset'] = idx  # 1 = previous quarter
            prior_guidances.extend(guidances)

        if not prior_guidances:
            return {
                'available': True,
                'say_do_ratio': None,
                'num_guidances_found': 0,
                'guidance_items': [],
                'credibility_rating': 'INSUFFICIENT_DATA',
                'reason': 'No numerical guidance found in prior transcripts',
            }

        # ── Step 2: Cross-reference with actuals ────────────
        actuals = self._extract_actuals(data)
        comparison_items = []

        for guidance in prior_guidances:
            actual = self._match_actual(guidance, actuals)
            if actual is not None:
                if guidance['value'] > 0:
                    ratio = actual / guidance['value']
                else:
                    continue  # Skip — can't compute ratio with zero guidance
                from config import config as _cfg
                _sd_thresh = _cfg.validation.say_do_threshold
                comparison_items.append({
                    'topic': guidance['topic'],
                    'type': guidance['type'],
                    'promised': round(guidance['value'], 2),
                    'actual': round(actual, 2),
                    'say_do': round(ratio, 3),
                    'met': ratio >= _sd_thresh,
                    'exceeded': ratio >= 1.0,
                    'quarter_offset': guidance['source_quarter_offset'],
                    'snippet': guidance.get('snippet', '')[:200],
                })

        if not comparison_items:
            return {
                'available': True,
                'say_do_ratio': None,
                'num_guidances_found': len(prior_guidances),
                'num_matched': 0,
                'num_promises_tracked': 0,
                'num_delivered': 0,
                'num_missed': 0,
                'guidance_items': prior_guidances[:10],
                'credibility_rating': 'INSUFFICIENT_DATA',
                'reason': 'Could not match guidance to actual results',
            }

        # ── Step 3: Calculate overall Say-Do ratio ──────────
        # Rule 5: Apply exponential time-decay weighting.
        # Recent quarters get much higher weight than older ones.
        # weight = exp(-decay * quarter_offset)  where decay = 0.5
        import math
        _DECAY = 0.5

        _weighted_met = 0.0
        _weighted_total = 0.0
        for item in comparison_items:
            _offset = item.get('quarter_offset', 1)
            _w = math.exp(-_DECAY * max(_offset - 1, 0))
            _weighted_total += _w
            if item['met']:
                _weighted_met += _w

        # Weighted ratio (gives recent quarters 2-3× more influence)
        overall_ratio = (_weighted_met / _weighted_total
                         if _weighted_total > 0 else 0.0)

        # Also keep the simple unweighted metric for transparency
        met_count = sum(1 for item in comparison_items if item['met'])
        met_pct = met_count / len(comparison_items) * 100
        unweighted_ratio = met_count / len(comparison_items)

        # Credibility rating — based on delivery rate
        if overall_ratio >= 0.9:
            credibility = 'EXCELLENT'
            assessment = 'Management consistently exceeds guidance — high credibility'
        elif overall_ratio >= 0.7:
            credibility = 'GOOD'
            assessment = 'Management largely meets promises — trustworthy'
        elif met_pct >= 60:
            credibility = 'FAIR'
            assessment = 'Management delivers near guidance — acceptable'
        elif met_pct >= 40:
            credibility = 'POOR'
            assessment = '⚠️ Management frequently misses guidance — credibility concern'
        else:
            credibility = 'VERY_POOR'
            assessment = '🔴 Management significantly over-promises — governance risk'

        # Governance risk if less than half of guidance items were met
        is_governance_risk = met_pct < 50

        _delivered = sum(1 for item in comparison_items if item['met'])
        _missed = len(comparison_items) - _delivered

        return {
            'available': True,
            'say_do_ratio': round(overall_ratio, 3),
            'unweighted_ratio': round(unweighted_ratio, 3),
            'met_pct': round(met_pct, 1),
            'num_guidances_found': len(prior_guidances),
            'num_matched': len(comparison_items),
            'num_promises_tracked': len(comparison_items),
            'num_delivered': _delivered,
            'num_missed': _missed,
            'comparison_items': comparison_items[:15],
            'credibility_rating': credibility,
            'assessment': assessment,
            'is_governance_risk': is_governance_risk,
            'time_decay_applied': True,
        }

    # ==================================================================
    # Guidance Extraction
    # ==================================================================
    def _extract_guidance(self, transcript: str) -> list:
        """Extract numerical forward-looking guidance from a transcript."""
        guidances = []
        sentences = self._split_sentences(transcript)

        for sent in sentences:
            sent_lower = sent.lower()

            # Check if sentence contains forward-looking keywords
            if not any(kw in sent_lower for kw in [
                'expect', 'target', 'guid', 'anticipat', 'aim',
                'project', 'forecast', 'achiev', 'grow', 'growth',
                'increase', 'expand', 'margin',
            ]):
                continue

            # Determine topic
            topic = self._classify_topic(sent_lower)

            # Try percentage guidance
            for pattern in [_PCT_GUIDANCE, _GROWTH_GUIDANCE,
                            _MARGIN_GUIDANCE, _CAGR_GUIDANCE]:
                match = pattern.search(sent)
                if match:
                    try:
                        value = float(match.group(1))
                        if 0 < value < 200:  # Sanity check
                            guidances.append({
                                'topic': topic,
                                'type': 'percentage',
                                'value': value,
                                'snippet': sent.strip()[:300],
                            })
                            break
                    except (ValueError, IndexError):
                        continue

            # Try absolute amount guidance
            match = _AMOUNT_GUIDANCE.search(sent)
            if match:
                try:
                    value = float(match.group(1).replace(',', ''))
                    if value > 10:  # Must be meaningful amount
                        guidances.append({
                            'topic': topic,
                            'type': 'amount',
                            'value': value,
                            'snippet': sent.strip()[:300],
                        })
                except (ValueError, IndexError):
                    pass

        # Deduplicate by topic + type
        seen = set()
        unique = []
        for g in guidances:
            key = (g['topic'], g['type'], g['value'])
            if key not in seen:
                seen.add(key)
                unique.append(g)
        return unique[:20]  # Cap

    def _classify_topic(self, text: str) -> str:
        """Classify a sentence into a guidance topic."""
        for topic, patterns in _GUIDANCE_TOPICS.items():
            for pat in patterns:
                if re.search(pat, text, re.IGNORECASE):
                    return topic
        return 'General'

    # ==================================================================
    # Actual Results Extraction
    # ==================================================================
    def _extract_actuals(self, data: dict) -> dict:
        """Extract actual financial results for comparison."""
        import pandas as pd
        from data.preprocessing import DataPreprocessor, get_value
        pp = DataPreprocessor()

        actuals = {}
        pnl = data.get('pnl', pd.DataFrame())
        bs = data.get('balance_sheet', pd.DataFrame())

        if not pnl.empty:
            # Revenue growth
            sales = pp.get(pnl, 'sales')
            if len(sales.dropna()) >= 2:
                latest = get_value(sales, -1)
                prev = get_value(sales, -2)
                if prev > 0:
                    actuals['Revenue Growth'] = round(
                        (latest / prev - 1) * 100, 2)
                actuals['Revenue Target'] = latest

            # Profit growth
            pat = pp.get(pnl, 'net_profit')
            if len(pat.dropna()) >= 2:
                latest = get_value(pat, -1)
                prev = get_value(pat, -2)
                if prev > 0:
                    actuals['Profitability'] = round(
                        (latest / prev - 1) * 100, 2)

            # Operating margin
            opm = pp.get(pnl, 'opm')
            if not opm.dropna().empty:
                actuals['Margin'] = float(get_value(opm))

        if not bs.empty:
            # Net debt
            borr = pp.get(bs, 'borrowings')
            if not borr.dropna().empty:
                actuals['Debt'] = float(get_value(borr))

        return actuals

    def _match_actual(self, guidance: dict, actuals: dict) -> Optional[float]:
        """Match a guidance item to an actual result."""
        topic = guidance['topic']
        g_type = guidance['type']

        if topic in actuals:
            return actuals[topic]

        # Fuzzy matching
        topic_lower = topic.lower()
        for key, val in actuals.items():
            if any(word in key.lower() for word in topic_lower.split()):
                return val
        return None

    # ==================================================================
    # Utility
    # ==================================================================
    @staticmethod
    def _split_sentences(text: str) -> list:
        """Split text into sentences."""
        text = re.sub(r'\s+', ' ', text)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        return [s.strip() for s in sentences if len(s.strip()) > 30]
