"""
Synthesis Agent — Final Rating (Enhanced)
==========================================
Combines all quantitative + qualitative + technical + predictive
signals into a Buy / Hold / Sell recommendation using a
comprehensive weighted scoring rubric.

Signal Weights:
  • DCF Valuation       (3)
  • Piotroski F-Score   (3)
  • Beneish M-Score     (2)
  • Growth Metrics      (2)
  • 5-Year Trend Health (2)
  • Technical Signal    (2)
  • Peer Comparison     (1)
  • Sentiment           (1)
  • Predictive Trend    (1)
  • CFO/EBITDA Quality  (1)
  • Governance          (1)
  • Moat Strength       (1)
  • ESG Score           (1)
  • Text Intelligence   (1)
"""


class SynthesisAgent:
    """
    Enhanced synthesis that integrates all analysis modules
    into a single weighted Buy/Hold/Sell recommendation.
    """

    def __init__(self):
        pass

    @property
    def available(self) -> bool:
        return True

    def run(self, analysis: dict) -> dict:
        """
        Produce final rating from all analysis components.

        Parameters:
            analysis: dict with keys — dcf, fscore, mscore, ratios,
                      sentiment, prediction, cfo_ebitda_check, peer_cca,
                      technicals, trends, text_intel, esg, governance, moat
        Returns:
            {recommendation, score, max_score, thesis, horizon, confidence}
        """
        score     = 0
        max_score = 0
        thesis    = []

        # ── DCF Valuation (weight 3) ─────────────────────────
        dcf = analysis.get('dcf', {})
        if dcf.get('available'):
            max_score += 3
            up = dcf.get('upside_pct', 0) or 0
            if up > 20:
                score += 3
                thesis.append(f"Significantly undervalued per DCF (upside {up:+.1f}%)")
            elif up > 0:
                score += 2
                thesis.append(f"Moderately undervalued per DCF (upside {up:+.1f}%)")
            elif up > -15:
                score += 1
                thesis.append(f"Fairly valued per DCF ({up:+.1f}%)")
            else:
                thesis.append(f"Overvalued per DCF ({up:+.1f}%)")

        # ── Piotroski F-Score (weight 3) ─────────────────────
        fs = analysis.get('fscore', {})
        if fs.get('available'):
            max_score += 3
            f = fs['f_score']
            if f >= 8:
                score += 3; thesis.append(f"Strong financial health (F-Score {f}/9)")
            elif f >= 5:
                score += 2; thesis.append(f"Moderate financial health (F-Score {f}/9)")
            else:
                thesis.append(f"Weak financial health (F-Score {f}/9)")

        # ── Beneish M-Score (weight 2) ───────────────────────
        ms = analysis.get('mscore', {})
        if ms.get('available'):
            max_score += 2
            if ms['risk_level'] == 'LOW':
                score += 2; thesis.append("Clean earnings — no manipulation signals")
            elif ms['risk_level'] == 'MEDIUM':
                score += 1; thesis.append("Inconclusive earnings quality — monitor closely")
            else:
                thesis.append("⚠️ Earnings manipulation risk detected")

        # ── Growth (weight 2) ────────────────────────────────
        ratios = analysis.get('ratios', {})
        rg = ratios.get('revenue_growth')
        pg = ratios.get('profit_growth')
        if rg is not None:
            max_score += 1
            if rg > 10:
                score += 1; thesis.append(f"Healthy revenue growth {rg}% YoY")
            elif rg < -5:
                thesis.append(f"Revenue declining {rg}% YoY")
        if pg is not None:
            max_score += 1
            if pg > 10:
                score += 1; thesis.append(f"Strong profit growth {pg}% YoY")

        # ── 5-Year Trend Health (weight 2) ───────────────────
        trends = analysis.get('trends', {})
        if trends.get('available'):
            max_score += 2
            health = trends.get('health_score', 5)
            direction = trends.get('overall_direction', 'STABLE')
            if direction == 'IMPROVING' and health >= 7:
                score += 2
                thesis.append(f"Strong 5Y trend (health {health}/10, {direction})")
            elif health >= 4:
                score += 1
                thesis.append(f"Mixed 5Y trend (health {health}/10, {direction})")
            else:
                thesis.append(f"Weak 5Y trend (health {health}/10, {direction})")

        # ── Technical Signal (weight 2) ──────────────────────
        tech = analysis.get('technicals', {})
        if tech.get('available'):
            max_score += 2
            sig = tech.get('overall_signal', {})
            signal = sig.get('signal', 'NEUTRAL')
            if signal in ('STRONG_BULLISH',):
                score += 2
                thesis.append("Strong bullish technical setup")
            elif signal in ('MILDLY_BULLISH',):
                score += 1
                thesis.append("Mildly bullish technicals")
            elif signal in ('STRONG_BEARISH',):
                thesis.append("⚠️ Bearish technical structure")
            elif signal in ('MILDLY_BEARISH',):
                thesis.append("Cautious — mildly bearish technicals")

            # Volume confirmation
            vol = tech.get('volume_analysis', {})
            if vol.get('divergence') == 'BEARISH_DIVERGENCE':
                thesis.append("⚠️ Bearish volume divergence detected")
            elif vol.get('divergence') == 'BULLISH_DIVERGENCE':
                thesis.append("Bullish volume divergence — possible reversal signal")

        # ── Peer Comparison (weight 1) ───────────────────────
        peer = analysis.get('peer_cca', {})
        if peer.get('available'):
            max_score += 1
            pe_prem = peer.get('pe_premium_pct')
            if pe_prem is not None:
                if pe_prem < -15:
                    score += 1
                    thesis.append(f"Discount to sector P/E ({pe_prem:+.1f}%)")
                elif pe_prem > 30:
                    thesis.append(f"Significant premium to sector P/E ({pe_prem:+.1f}%)")

        # ── Sentiment (weight 1) ─────────────────────────────
        sentiment = analysis.get('sentiment', {})
        if sentiment.get('available'):
            max_score += 1
            tone = sentiment.get('tone', 'NEUTRAL')
            if tone in ('BULLISH', 'MILDLY POSITIVE'):
                score += 1
                thesis.append(f"Positive management tone ({tone})")
            elif tone in ('BEARISH', 'CAUTIOUS'):
                thesis.append(f"Cautious management tone ({tone})")

        # ── Predictive trend (weight 1) ──────────────────────
        prediction = analysis.get('prediction', {})
        if prediction.get('available'):
            max_score += 1
            trend = prediction.get('trend', 'SIDEWAYS')
            if trend in ('BULLISH', 'MILDLY BULLISH'):
                score += 1
                thesis.append(f"Price forecast: {trend} (30-day model)")
            elif trend in ('BEARISH', 'MILDLY BEARISH'):
                thesis.append(f"Price forecast: {trend} (30-day model)")

        # ── CFO/EBITDA quality (weight 1) ────────────────────
        cfo_check = analysis.get('cfo_ebitda_check', {})
        if cfo_check.get('available'):
            max_score += 1
            if not cfo_check.get('is_red_flag'):
                score += 1
                thesis.append("Cash flow quality: CFO/EBITDA ratio healthy")
            else:
                thesis.append("⚠️ CFO/EBITDA ratio weak — profits may not be cash-backed")

        # ── Governance (weight 1) ────────────────────────────
        governance = analysis.get('governance', {})
        if governance.get('available'):
            max_score += 1
            gov_score = governance.get('governance_score', 5)
            if gov_score >= 8:
                score += 1
                thesis.append(f"Strong corporate governance (score {gov_score}/10)")
            elif gov_score < 5:
                thesis.append(f"⚠️ Governance concerns (score {gov_score}/10)")

        # ── Moat strength (weight 1) ─────────────────────────
        moat = analysis.get('moat', {})
        if moat.get('available'):
            max_score += 1
            moat_score = moat.get('moat_score', 0)
            if moat_score >= 6:
                score += 1
                thesis.append(f"Competitive moat detected ({moat.get('dominant_moat', 'Multiple')})")
            elif moat_score < 3:
                thesis.append("Weak competitive positioning — no clear moat detected")

        # ── ESG Score (weight 1) ─────────────────────────────
        esg = analysis.get('esg', {})
        if esg.get('available'):
            max_score += 1
            esg_score = esg.get('esg_score', 5)
            if esg_score >= 7:
                score += 1
                thesis.append(f"Strong ESG profile ({esg_score}/10)")
            elif esg_score < 4:
                thesis.append(f"Weak ESG profile ({esg_score}/10)")

        # ── Text Intelligence (weight 1) ─────────────────────
        text_intel = analysis.get('text_intel', {})
        if text_intel.get('available'):
            max_score += 1
            overall_tone = text_intel.get('overall_tone', 'NEUTRAL')
            if overall_tone == 'POSITIVE':
                score += 1
                thesis.append("Text analysis: Positive sentiment across documents")
            elif overall_tone == 'NEGATIVE':
                thesis.append("Text analysis: Negative sentiment detected")

        # ── Final recommendation ─────────────────────────────
        if max_score == 0:
            return {
                'recommendation': 'HOLD ⏸️',
                'thesis': ['Insufficient data for rating'],
                'horizon': 'N/A',
                'score': 0,
                'max_score': 0,
                'confidence': 'LOW',
            }

        pct = score / max_score
        if pct >= 0.70:
            rec = "BUY 🟢";   horizon = "12–18 months"
        elif pct >= 0.40:
            rec = "HOLD 🟡";  horizon = "6–12 months (review)"
        else:
            rec = "SELL 🔴";  horizon = "Consider exit within 3–6 months"

        # Confidence based on how many modules contributed
        if max_score >= 14:
            confidence = 'HIGH'
        elif max_score >= 8:
            confidence = 'MEDIUM'
        else:
            confidence = 'LOW'

        return {
            'recommendation': rec,
            'score': score,
            'max_score': max_score,
            'thesis': thesis,
            'horizon': horizon,
            'confidence': confidence,
        }
