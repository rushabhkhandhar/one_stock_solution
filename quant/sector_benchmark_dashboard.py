"""
Sector and Industry Benchmark Dashboard
======================================
Computes contextual benchmarking metrics for the stock against its peer set.
"""
from __future__ import annotations

from typing import Dict, List, Optional


class SectorBenchmarkDashboard:
    """Build percentile-based benchmark context from peer comparable output."""

    @staticmethod
    def _percentile_rank(
        value: Optional[float],
        peer_values: List[float],
        higher_is_better: bool,
    ) -> Optional[float]:
        if value is None:
            return None
        vals = [float(v) for v in peer_values if v is not None]
        if not vals:
            return None

        if higher_is_better:
            below_or_equal = sum(1 for v in vals if v <= value)
        else:
            below_or_equal = sum(1 for v in vals if v >= value)

        pct = below_or_equal / len(vals) * 100
        return round(pct, 2)

    @staticmethod
    def _verdict_from_score(score: float) -> str:
        if score >= 75:
            return "TOP_QUARTILE"
        if score >= 50:
            return "ABOVE_MEDIAN"
        if score >= 25:
            return "BELOW_MEDIAN"
        return "BOTTOM_QUARTILE"

    def analyze(self, analysis: Dict) -> Dict:
        peer = analysis.get("peer_cca", {})
        ratios = analysis.get("ratios", {})

        if not peer.get("available"):
            return {
                "available": False,
                "reason": "Peer comparable data unavailable",
            }

        peers = peer.get("peers", [])
        if not peers:
            return {
                "available": False,
                "reason": "No peer records available for benchmarking",
            }

        # Stock metrics from existing analyses
        stock_metrics = {
            "pe": peer.get("stock_pe"),
            "ev_ebitda": peer.get("stock_ev_ebitda"),
            "market_cap_cr": peer.get("stock_mcap_cr"),
            "roe": ratios.get("roe"),
            "dividend_yield": ratios.get("dividend_yield"),
        }

        # Peer distributions
        peer_metrics = {
            "pe": [p.get("pe") for p in peers],
            "ev_ebitda": [p.get("ev_ebitda") for p in peers],
            "market_cap_cr": [p.get("market_cap_cr") for p in peers],
            "roe": [p.get("roe") for p in peers],
            "dividend_yield": [p.get("dividend_yield") for p in peers],
        }

        # Metric directions: valuation multiples lower is better; returns and scale higher is better
        metric_direction = {
            "pe": False,
            "ev_ebitda": False,
            "market_cap_cr": True,
            "roe": True,
            "dividend_yield": True,
        }

        benchmark_rows = []
        score_values = []
        for metric, stock_value in stock_metrics.items():
            peer_values = peer_metrics.get(metric, [])
            percentile = self._percentile_rank(
                stock_value,
                peer_values,
                higher_is_better=metric_direction[metric],
            )
            if percentile is not None:
                score_values.append(percentile)
            benchmark_rows.append({
                "metric": metric,
                "stock_value": stock_value,
                "peer_count": len([v for v in peer_values if v is not None]),
                "percentile": percentile,
                "direction": "HIGHER_BETTER" if metric_direction[metric] else "LOWER_BETTER",
            })

        if not score_values:
            return {
                "available": False,
                "reason": "No overlapping stock/peer metrics for benchmarking",
            }

        benchmark_score = round(sum(score_values) / len(score_values), 2)
        verdict = self._verdict_from_score(benchmark_score)

        return {
            "available": True,
            "sector": peer.get("sector"),
            "industry": peer.get("industry"),
            "peer_count": peer.get("peer_count", len(peers)),
            "stock_mcap_tier": peer.get("stock_mcap_tier"),
            "benchmark_score": benchmark_score,
            "benchmark_verdict": verdict,
            "rows": benchmark_rows,
        }
