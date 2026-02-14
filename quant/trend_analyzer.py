"""
5-Year Trend Analyzer
======================
Computes 5-year trends for key financial metrics:
  • Revenue, PAT, Operating Profit, EPS, OPM
  • ROE, ROCE, Debt-to-Equity
  • Cash Flow from Operations
  • CAGR, direction, acceleration, and model-based projections

Also produces a compact "Trend Summary" for the synthesis agent.
"""
import numpy as np
import pandas as pd
from typing import Optional
from data.preprocessing import DataPreprocessor

pp = DataPreprocessor()


class TrendAnalyzer:
    """Analyze 5-year historical trends and project next 1–2 years."""

    # Metrics we track from each DataFrame
    PNL_METRICS = [
        ('sales', 'Revenue'),
        ('operating_profit', 'Operating Profit'),
        ('opm', 'OPM %'),
        ('net_profit', 'Net Profit (PAT)'),
        ('eps', 'EPS'),
    ]
    BS_METRICS = [
        ('borrowings', 'Total Borrowings'),
        ('reserves', 'Reserves'),
    ]
    CF_METRICS = [
        ('operating_cf', 'Cash from Operations'),
    ]

    def analyze(self, data: dict) -> dict:
        """
        Full 5-year trend analysis.

        Parameters
        ----------
        data : dict
            Output from DataIngestion.load_company() + preprocessing.

        Returns
        -------
        dict with keys:
            available, metrics (list of trend dicts), summary (compact text),
            overall_direction ('IMPROVING' | 'STABLE' | 'DETERIORATING'),
            health_score (0–10).
        """
        pnl = data.get('pnl', pd.DataFrame())
        bs = data.get('balance_sheet', pd.DataFrame())
        cf = data.get('cash_flow', pd.DataFrame())

        if pnl.empty:
            return {'available': False, 'reason': 'No P&L data'}

        trends = []

        # P&L trends
        for canonical, label in self.PNL_METRICS:
            series = pp.get(pnl, canonical).dropna()
            trend = self._compute_trend(series, label)
            if trend:
                trends.append(trend)

        # Balance Sheet trends
        for canonical, label in self.BS_METRICS:
            series = pp.get(bs, canonical).dropna()
            trend = self._compute_trend(series, label)
            if trend:
                trends.append(trend)

        # Cash Flow trends
        for canonical, label in self.CF_METRICS:
            series = pp.get(cf, canonical).dropna()
            trend = self._compute_trend(series, label)
            if trend:
                trends.append(trend)

        # Derived ratios (from preprocessing)
        for key, label in [('roe', 'ROE'), ('debt_to_equity', 'D/E Ratio'),
                           ('pat_margin', 'PAT Margin')]:
            series = data.get(key)
            if series is not None and isinstance(series, pd.Series):
                series = series.dropna()
                trend = self._compute_trend(series, label, is_ratio=True)
                if trend:
                    trends.append(trend)

        # ROCE from ratios DF
        ratios_df = data.get('ratios', pd.DataFrame())
        if not ratios_df.empty:
            roce = pp.get(ratios_df, 'roce').dropna()
            trend = self._compute_trend(roce, 'ROCE %', is_ratio=True)
            if trend:
                trends.append(trend)

        if not trends:
            return {'available': False, 'reason': 'Insufficient data for trends'}

        # Overall health assessment
        directions = [t['direction'] for t in trends]
        improving = sum(1 for d in directions if d == 'UP')
        declining = sum(1 for d in directions if d == 'DOWN')
        total = len(directions)

        if improving > total * 0.6:
            overall = 'IMPROVING'
        elif declining > total * 0.6:
            overall = 'DETERIORATING'
        else:
            overall = 'STABLE'

        # Health score: 0–10
        health = round(10 * improving / max(total, 1))

        # Summary text
        summary_parts = []
        for t in trends:
            cagr = t.get('cagr_5y')
            cagr_str = f"(5Y CAGR {cagr:+.1f}%)" if cagr is not None else ""
            arrow = {'UP': '↑', 'DOWN': '↓', 'FLAT': '→'}.get(t['direction'], '→')
            summary_parts.append(f"{t['label']} {arrow} {cagr_str}")

        return {
            'available': True,
            'metrics': trends,
            'overall_direction': overall,
            'health_score': health,
            'summary': " | ".join(summary_parts),
            'num_years': min(5, max((t.get('data_points', 0) for t in trends), default=0)),
        }

    # ------------------------------------------------------------------
    def _compute_trend(self, series: pd.Series, label: str,
                       is_ratio: bool = False) -> Optional[dict]:
        """Compute trend stats for a single metric series."""
        if series is None or len(series) < 2:
            return None

        # Take last 5 data points (years)
        series = series.tail(5)
        values = series.values.astype(float)
        n = len(values)

        first, last = values[0], values[-1]

        # Direction
        if n >= 3:
            # Use linear regression slope
            x = np.arange(n)
            slope, intercept = np.polyfit(x, values, 1)
            direction = 'UP' if slope > 0.01 * abs(np.mean(values)) else \
                        ('DOWN' if slope < -0.01 * abs(np.mean(values)) else 'FLAT')
        else:
            direction = 'UP' if last > first * 1.02 else \
                        ('DOWN' if last < first * 0.98 else 'FLAT')
            slope = (last - first) / max(1, n - 1)
            intercept = first

        # CAGR (5-year or however many years we have)
        cagr = None
        if first > 0 and last > 0 and n >= 2:
            cagr = round(((last / first) ** (1.0 / (n - 1)) - 1) * 100, 2)
        elif first < 0 and last > 0:
            cagr = None  # Can't compute CAGR when base is negative

        # YoY changes
        yoy_changes = []
        for i in range(1, n):
            if values[i - 1] != 0:
                yoy = round(((values[i] / values[i - 1]) - 1) * 100, 2)
                yoy_changes.append(yoy)

        # Acceleration (is growth accelerating or decelerating?)
        acceleration = None
        if len(yoy_changes) >= 3:
            recent = np.mean(yoy_changes[-2:])
            older = np.mean(yoy_changes[:-2])
            if recent > older + 2:
                acceleration = 'ACCELERATING'
            elif recent < older - 2:
                acceleration = 'DECELERATING'
            else:
                acceleration = 'STEADY'

        # Simple projection (next 1–2 years using linear model)
        proj_1y = round(float(slope * n + intercept), 2)
        proj_2y = round(float(slope * (n + 1) + intercept), 2)

        # Historical values with dates
        history = []
        for idx, val in zip(series.index, values):
            year_label = str(idx.year) if hasattr(idx, 'year') else str(idx)
            history.append({'year': year_label, 'value': round(float(val), 2)})

        return {
            'label': label,
            'direction': direction,
            'data_points': n,
            'latest': round(float(last), 2),
            'first': round(float(first), 2),
            'cagr_5y': cagr,
            'yoy_changes': yoy_changes,
            'acceleration': acceleration,
            'slope': round(float(slope), 4),
            'projection_1y': proj_1y,
            'projection_2y': proj_2y,
            'history': history,
            'is_ratio': is_ratio,
        }
