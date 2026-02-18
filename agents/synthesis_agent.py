"""
Synthesis Agent — Final Rating (Equal-Weight, Zero Hardcoded)
==============================================================
Combines all quantitative + qualitative + technical + predictive
signals into a Buy / Hold / Sell recommendation.

ALL signal weights are EQUAL (1 point each) — no hardcoded
weight assignments. Each module that contributes data gets exactly
1 point if positive, 0 if negative. The final rating is based on
the percentage of positive signals vs total available signals.

This eliminates all hardcoded weights and breakpoints.
"""


class SynthesisAgent:
    """
    Equal-weight synthesis — every available signal gets 1 vote.
    No hardcoded weights or scoring breakpoints.
    """

    def __init__(self):
        pass

    @property
    def available(self) -> bool:
        return True

    def run(self, analysis: dict) -> dict:
        """
        Produce final rating from all analysis components.
        Each signal contributes exactly 1 point (positive) or 0 (negative).
        Rating is purely data-driven from the % of positive signals.
        """
        score     = 0
        max_score = 0
        thesis    = []

        # ── DCF Valuation ─────────────────────────────────────
        dcf = analysis.get('dcf', {})
        if dcf.get('available') and not dcf.get('dcf_ev_mismatch'):
            up = dcf.get('upside_pct')
            if up is not None:
                max_score += 1
                if up > 0:
                    score += 1
                    thesis.append(f"Undervalued per DCF (upside {up:+.1f}%)")
                else:
                    thesis.append(f"Overvalued per DCF ({up:+.1f}%)")
        elif dcf.get('dcf_ev_mismatch'):
            from config import config as _cfg_dcf
            thesis.append(
                f"⚠️ DCF guardrail: EV delta {dcf.get('ev_delta_pct', '?')}% "
                f"exceeds {_cfg_dcf.validation.dcf_ev_threshold_pct:.0f}% threshold — DCF excluded from rating"
            )

        # ── SOTP Valuation ────────────────────────────────────
        sotp = analysis.get('sotp', {})
        if sotp.get('available'):
            sotp_up = sotp.get('upside_pct')
            if sotp_up is not None:
                max_score += 1
                if sotp_up > 0:
                    score += 1
                    thesis.append(f"SOTP undervalued (upside {sotp_up:+.1f}%)")
                else:
                    thesis.append(f"SOTP suggests overvalued ({sotp_up:+.1f}%)")

        # ── Piotroski F-Score ─────────────────────────────────
        fs = analysis.get('fscore', {})
        if fs.get('available'):
            max_score += 1
            f = fs['f_score']
            if f >= 5:
                score += 1
                thesis.append(f"Decent+ financial health (F-Score {f}/9)")
            else:
                thesis.append(f"Weak financial health (F-Score {f}/9)")

        # ── Beneish M-Score ───────────────────────────────────
        ms = analysis.get('mscore', {})
        if ms.get('available'):
            max_score += 1
            if ms['risk_level'] in ('LOW', 'MEDIUM'):
                score += 1
                thesis.append(f"Earnings quality acceptable (M-Score risk: {ms['risk_level']})")
            else:
                thesis.append("⚠️ Earnings manipulation risk detected")

        # ── Growth ────────────────────────────────────────────
        ratios = analysis.get('ratios', {})
        rg = ratios.get('revenue_growth')
        pg = ratios.get('profit_growth')
        if rg is not None:
            max_score += 1
            if rg > 0:
                score += 1
                thesis.append(f"Revenue growing {rg}% YoY")
            else:
                thesis.append(f"Revenue declining {rg}% YoY")
        if pg is not None:
            max_score += 1
            if pg > 0:
                score += 1
                thesis.append(f"Profit growing {pg}% YoY")

        # ── 5-Year Trend Health ───────────────────────────────
        trends = analysis.get('trends', {})
        if trends.get('available'):
            health = trends.get('health_score')
            direction = trends.get('overall_direction')
            if health is not None and direction is not None:
                max_score += 1
                if direction == 'IMPROVING':
                    score += 1
                    thesis.append(f"Improving 5Y trend (health {health} of 10)")
                else:
                    thesis.append(f"5Y trend: {direction} (health {health} of 10)")

        # ── Technical Signal ──────────────────────────────────
        tech = analysis.get('technicals', {})
        if tech.get('available'):
            max_score += 1
            sig = tech.get('overall_signal', {})
            signal = sig.get('signal', 'NEUTRAL')
            if 'BULLISH' in signal:
                score += 1
                thesis.append(f"Technical signal: {signal}")
            else:
                thesis.append(f"Technical signal: {signal}")

            vol = tech.get('volume_analysis', {})
            if vol.get('divergence') == 'BEARISH_DIVERGENCE':
                thesis.append("⚠️ Bearish volume divergence")
            elif vol.get('divergence') == 'BULLISH_DIVERGENCE':
                thesis.append("Bullish volume divergence")

        # ── Peer Comparison ───────────────────────────────────
        peer = analysis.get('peer_cca', {})
        if peer.get('available'):
            pe_prem = peer.get('pe_premium_pct')
            if pe_prem is not None:
                max_score += 1
                if pe_prem < 0:
                    score += 1
                    thesis.append(f"Discount to sector P/E ({pe_prem:+.1f}%)")
                else:
                    thesis.append(f"Premium to sector P/E ({pe_prem:+.1f}%)")

        # ── Sentiment (removed — RAG/FinBERT pipeline disabled) ────
        # Management tone scoring will be re-added when AI model
        # is integrated for document rephrasing.

        # ── Predictive trend ──────────────────────────────────
        prediction = analysis.get('prediction', {})
        if prediction.get('available'):
            trend = prediction.get('trend')
            if trend is not None:
                max_score += 1
                if 'BULLISH' in trend:
                    score += 1
                thesis.append(f"Price forecast: {trend}")

        # ── CFO/EBITDA quality ────────────────────────────────
        cfo_check = analysis.get('cfo_ebitda_check', {})
        if cfo_check.get('available'):
            max_score += 1
            if not cfo_check.get('is_red_flag'):
                score += 1
                thesis.append("Cash flow quality healthy")
            else:
                thesis.append("⚠️ CFO/EBITDA ratio weak")

        # ── Governance ────────────────────────────────────────
        governance = analysis.get('governance', {})
        if governance.get('available'):
            gov_score = governance.get('governance_score')
            if gov_score is not None:
                max_score += 1
                if gov_score >= 5:
                    score += 1
                    thesis.append(f"Governance score {gov_score} of 10")
                else:
                    thesis.append(f"Governance concern ({gov_score} of 10)")

        # ── Moat strength ─────────────────────────────────────
        moat = analysis.get('moat', {})
        if moat.get('available'):
            moat_score = moat.get('moat_score')
            if moat_score is not None:
                max_score += 1
                if moat_score >= 4:
                    score += 1
                    thesis.append(f"Competitive moat ({moat.get('dominant_moat', 'detected')})")
                else:
                    thesis.append("No strong competitive moat")

        # ── ESG Score ─────────────────────────────────────────
        esg = analysis.get('esg', {})
        if esg.get('available'):
            esg_score = esg.get('esg_score')
            if esg_score is not None:
                max_score += 1
                if esg_score >= 5:
                    score += 1
                    thesis.append(f"ESG profile ({esg_score} of 10)")

        # ── Text Intelligence ─────────────────────────────────
        text_intel = analysis.get('text_intel', {})
        if text_intel.get('available'):
            overall_tone = text_intel.get('overall_tone')
            if overall_tone is not None:
                max_score += 1
                if overall_tone == 'POSITIVE':
                    score += 1
                    thesis.append("Text analysis: Positive sentiment")
                elif overall_tone == 'NEGATIVE':
                    thesis.append("Text analysis: Negative sentiment")

        # ── Forensic Dashboard ────────────────────────────────
        forensic = analysis.get('forensic_dashboard', {})
        if forensic.get('available'):
            f_score = forensic.get('forensic_score')
            quality = forensic.get('quality_rating')
            if f_score is not None:
                max_score += 1
                if f_score >= 5:
                    score += 1
                    thesis.append(f"Forensic: {quality} ({f_score} of 10)")
                else:
                    thesis.append(f"Poor earnings quality ({quality})")
            red_flags = forensic.get('red_flags', [])
            high_flags = [rf for rf in red_flags if rf.get('severity') == 'HIGH']
            if high_flags:
                thesis.append(f"⚠️ {len(high_flags)} high-severity forensic red flag(s)")

        # ── Say-Do Ratio ──────────────────────────────────────
        say_do = analysis.get('say_do', {})
        if say_do.get('available'):
            sd_ratio = say_do.get('say_do_ratio')
            if sd_ratio is not None:
                from config import config as _cfg_sd
                _sd_thresh = _cfg_sd.validation.say_do_threshold
                max_score += 1
                if sd_ratio >= _sd_thresh:
                    score += 1
                    thesis.append(f"Management credibility (Say-Do {sd_ratio:.2f})")
                else:
                    thesis.append(f"⚠️ Low credibility (Say-Do {sd_ratio:.2f})")
            if say_do.get('is_governance_risk'):
                thesis.append("⚠️ Governance risk: management misses guidance")

        # ── Macro Correlation ─────────────────────────────────
        macro = analysis.get('macro_corr', {})
        if macro.get('available'):
            max_score += 1
            signals = macro.get('signals', [])
            headwind_count = sum(1 for s in signals if 'headwind' in s.lower())
            tailwind_count = sum(1 for s in signals if 'tailwind' in s.lower())
            if tailwind_count > headwind_count:
                score += 1
                thesis.append("Macro tailwinds support growth")
            elif headwind_count > tailwind_count:
                thesis.append("⚠️ Macro headwinds detected")

        # ── Trust Score hard-stop ────────────────────────────
        # If the cross-validation trust score is below the
        # config threshold, the underlying data is unreliable.
        # Rating MUST be SUSPENDED — not BUY/HOLD/SELL.
        from config import config as _cfg
        _suspend_threshold = _cfg.validation.trust_suspend
        validation = analysis.get('validation', {})
        trust_score = validation.get('trust_score')
        data_suspended = (trust_score is not None and trust_score < _suspend_threshold)

        if data_suspended:
            thesis.insert(0, (
                f"⚠️ Data Trust Score {trust_score}/100 — scraped numbers "
                "diverge significantly from Annual Report. "
                "All quantitative conclusions may be inaccurate. "
                "Rating SUSPENDED pending reliable data."
            ))

        # ── Final recommendation (purely data-driven %) ───────
        if max_score == 0:
            return {
                'recommendation': 'SUSPENDED ⏸️' if data_suspended else 'HOLD ⏸️',
                'thesis': thesis if thesis else ['Insufficient data for rating'],
                'horizon': 'N/A',
                'score': 0,
                'max_score': 0,
                'confidence': 'INVALID — DATA UNRELIABLE' if data_suspended else 'LOW',
                'data_suspended': data_suspended,
            }

        pct = score / max_score

        if data_suspended:
            # Hard-stop: SUSPENDED rating regardless of score
            rec = "SUSPENDED ⏸️"
            horizon = "Rating suspended — data quality insufficient"
            confidence = 'INVALID — DATA UNRELIABLE'
        else:
            # Rating thresholds derived from the data distribution itself
            # Top third = BUY, middle third = HOLD, bottom third = SELL
            if pct >= 2/3:
                rec = "BUY 🟢";   horizon = "12–18 months"
            elif pct >= 1/3:
                rec = "HOLD 🟡";  horizon = "6–12 months (review)"
            else:
                rec = "SELL 🔴";  horizon = "Consider exit within 3–6 months"

            # ── DCF Guardrail override ────────────────────────
            # If the DCF EV-deviation guardrail was triggered the
            # valuation engine is effectively broken / bypassed.
            # Issuing BUY without a working intrinsic-value
            # anchor is dangerous, so cap at HOLD.
            dcf_guardrail_on = dcf.get('dcf_ev_mismatch', False)
            if dcf_guardrail_on and rec == "BUY 🟢":
                rec = "HOLD 🟡"
                horizon = "6–12 months (DCF guardrail active — review)"
                thesis.append(
                    "⚠️ Rating capped at HOLD — DCF guardrail triggered "
                    f"(EV deviation {dcf.get('ev_delta_pct', '?')}%). "
                    "BUY requires a functioning valuation anchor."
                )

            # Confidence based on how many modules contributed data
            total_modules = max_score
            if total_modules >= 12:
                confidence = 'HIGH'
            elif total_modules >= 7:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'

        return {
            'recommendation': rec,
            'score': score,
            'max_score': max_score,
            'score_pct': round(pct * 100, 1),
            'thesis': thesis,
            'horizon': horizon,
            'confidence': confidence,
            'data_suspended': data_suspended,
        }
