"""
Investment Committee Pack Mode
==============================
Builds concise bull/base/bear outputs and a decision card for IC discussion.
"""
from __future__ import annotations

from typing import Dict, Optional


class InvestmentCommitteePack:
    """Create concise investment-committee outputs from existing analysis."""

    @staticmethod
    def _case_label(label: str) -> str:
        return {
            "bull": "BULL",
            "base": "BASE",
            "bear": "BEAR",
        }.get(label, label.upper())

    @staticmethod
    def _vote(signal: Optional[float], positive_if_high: bool = True) -> str:
        if signal is None:
            return "NEUTRAL"
        if positive_if_high:
            return "BULLISH" if signal > 0 else "BEARISH"
        return "BULLISH" if signal < 0 else "BEARISH"

    def build(self, analysis: Dict) -> Dict:
        rating = analysis.get("rating", {})
        scenario = analysis.get("scenario", {})

        if not rating:
            return {
                "available": False,
                "reason": "Rating data unavailable",
            }
        if not scenario.get("available"):
            return {
                "available": False,
                "reason": "Scenario analysis unavailable",
            }

        sc_map = scenario.get("scenarios", {})
        if not sc_map:
            return {
                "available": False,
                "reason": "Scenario map unavailable",
            }

        # Build concise case summaries directly from computed scenario outputs
        cases = []
        for key in ("bull", "base", "bear"):
            case = sc_map.get(key, {})
            cases.append({
                "case": self._case_label(key),
                "probability": case.get("probability"),
                "target_price": case.get("target_price"),
                "upside_pct": case.get("upside_pct"),
                "revenue_growth_pct": case.get("revenue_growth_pct"),
                "pat_margin_pct": case.get("pat_margin_pct"),
                "exit_pe": case.get("exit_pe"),
                "concise_line": (
                    f"{self._case_label(key)}: "
                    f"Target ₹{case.get('target_price', 0):,.2f}, "
                    f"{case.get('upside_pct', 0):+.1f}% upside, "
                    f"P={case.get('probability', 0):.0%}"
                ),
            })

        # Decision card signals from already-computed components
        dcf = analysis.get("dcf", {})
        pred = analysis.get("prediction", {})
        forensic = analysis.get("forensic_dashboard", {})
        macro = analysis.get("macro_corr", {})

        valuation_signal = dcf.get("upside_pct")
        quality_signal = forensic.get("forensic_score")
        momentum_trend = pred.get("trend")
        macro_signals = macro.get("signals", [])

        macro_tailwinds = sum(1 for s in macro_signals if "tailwind" in s.lower())
        macro_headwinds = sum(1 for s in macro_signals if "headwind" in s.lower())
        macro_bias = macro_tailwinds - macro_headwinds

        momentum_vote = "NEUTRAL"
        if isinstance(momentum_trend, str):
            t = momentum_trend.upper()
            if "BULLISH" in t:
                momentum_vote = "BULLISH"
            elif "BEARISH" in t:
                momentum_vote = "BEARISH"

        quality_vote = "NEUTRAL"
        if quality_signal is not None:
            quality_vote = "BULLISH" if quality_signal >= 5 else "BEARISH"

        valuation_vote = self._vote(valuation_signal, positive_if_high=True)
        macro_vote = self._vote(float(macro_bias), positive_if_high=True) if macro_signals else "NEUTRAL"

        votes = [valuation_vote, quality_vote, momentum_vote, macro_vote]
        bull_votes = sum(1 for v in votes if v == "BULLISH")
        bear_votes = sum(1 for v in votes if v == "BEARISH")

        if bull_votes > bear_votes:
            net_bias = "BULLISH"
        elif bear_votes > bull_votes:
            net_bias = "BEARISH"
        else:
            net_bias = "BALANCED"

        return {
            "available": True,
            "recommendation": rating.get("recommendation"),
            "confidence": rating.get("confidence"),
            "horizon": rating.get("horizon"),
            "current_price": scenario.get("current_price"),
            "weighted_target": scenario.get("weighted_target"),
            "weighted_upside_pct": scenario.get("weighted_upside_pct"),
            "cases": cases,
            "decision_card": {
                "valuation_vote": valuation_vote,
                "quality_vote": quality_vote,
                "momentum_vote": momentum_vote,
                "macro_vote": macro_vote,
                "bull_votes": bull_votes,
                "bear_votes": bear_votes,
                "net_bias": net_bias,
            },
        }
