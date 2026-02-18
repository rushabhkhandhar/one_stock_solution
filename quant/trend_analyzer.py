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

        # ── Rule 4: Detect corporate-action distortions ──────
        # If shares outstanding jumped > 80% YoY in a single year,
        # it signals a bonus issue, stock split, or mega-merger.
        # We flag this so the acceleration label is softened.
        _corp_action_detected = False
        _corp_action_year = None
        try:
            _shares = data.get('shares_outstanding')
            if _shares is not None and isinstance(_shares, pd.Series):
                _sh = _shares.dropna().tail(5)
                if len(_sh) >= 2:
                    for i in range(1, len(_sh)):
                        _prev = float(_sh.iloc[i - 1])
                        _curr = float(_sh.iloc[i])
                        if _prev > 0 and abs(_curr / _prev - 1) > 0.80:
                            _corp_action_detected = True
                            _idx = _sh.index[i]
                            _corp_action_year = (str(_idx.year)
                                                 if hasattr(_idx, 'year')
                                                 else str(_idx))
                            break
        except Exception:
            pass

        trends = []

        # P&L trends
        for canonical, label in self.PNL_METRICS:
            series = pp.get(pnl, canonical).dropna()
            is_ratio = (canonical == 'opm')
            trend = self._compute_trend(series, label, is_ratio=is_ratio)
            if trend:
                # Screener stores OPM% as decimal (0.14 = 14%)
                if canonical == 'opm':
                    trend['is_pct_decimal'] = True
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
        # roe, pat_margin are decimal (0.14 = 14%) → is_pct_decimal
        # debt_to_equity is a pure ratio (0.37x)  → is_pure_ratio
        _derived_ratios = [
            ('roe',            'ROE',        True,  False),
            ('debt_to_equity', 'D/E Ratio',  False, True),
            ('pat_margin',     'PAT Margin', True,  False),
        ]
        for key, label, pct_dec, pure_ratio in _derived_ratios:
            series = data.get(key)
            if series is not None and isinstance(series, pd.Series):
                series = series.dropna()
                trend = self._compute_trend(series, label, is_ratio=True)
                if trend:
                    trend['is_pct_decimal'] = pct_dec
                    trend['is_pure_ratio'] = pure_ratio
                    trends.append(trend)

        # ROCE from ratios DF
        ratios_df = data.get('ratios', pd.DataFrame())
        if not ratios_df.empty:
            roce = pp.get(ratios_df, 'roce').dropna()
            trend = self._compute_trend(roce, 'ROCE %', is_ratio=True)
            if trend:
                # Screener stores ROCE% as decimal (0.14 = 14%)
                trend['is_pct_decimal'] = True
                trends.append(trend)

        if not trends:
            return {'available': False, 'reason': 'Insufficient data for trends'}

        # ── Rule 4: Soften DECELERATING labels when corporate
        #    action (bonus / split / merger) detected in window.
        if _corp_action_detected:
            _eps_affected = {'EPS', 'Revenue', 'Net Profit (PAT)',
                             'Operating Profit'}
            for t in trends:
                if (t.get('acceleration') == 'DECELERATING'
                        and t['label'] in _eps_affected):
                    t['acceleration'] = 'DECELERATING_CORP_ACTION'
                    t['corp_action_note'] = (
                        f'Corporate action ({_corp_action_year}) expanded '
                        f'the denominator base — deceleration is structural, '
                        f'not fundamental deterioration.')

        # Overall health assessment — direction from proportion of improving metrics
        directions = [t['direction'] for t in trends]
        improving = sum(1 for d in directions if d == 'UP')
        declining = sum(1 for d in directions if d == 'DOWN')
        total = len(directions)

        # Use natural thirds: >2/3 improving = IMPROVING, >2/3 declining = DETERIORATING
        if total > 0 and improving > total * 2 / 3:
            overall = 'IMPROVING'
        elif total > 0 and declining > total * 2 / 3:
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
            'corp_action_detected': _corp_action_detected,
            'corp_action_year': _corp_action_year,
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

        # Direction — derived from data variability
        if n >= 3:
            # Use linear regression slope
            x = np.arange(n)
            slope, intercept = np.polyfit(x, values, 1)
            # Use coefficient of variation as the noise floor
            mean_val = abs(np.mean(values))
            std_val = np.std(values)
            # Slope is significant if it exceeds noise / sqrt(n)
            noise_floor = std_val / np.sqrt(n) if n > 0 else 0
            direction = 'UP' if slope > noise_floor else \
                        ('DOWN' if slope < -noise_floor else 'FLAT')
        else:
            # Two data points — compare directly
            pct_diff = (last - first) / abs(first) if first != 0 else 0
            direction = 'UP' if pct_diff > 0.02 else \
                        ('DOWN' if pct_diff < -0.02 else 'FLAT')
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
            # Threshold = 1 std of YoY changes (data-relative)
            yoy_std = np.std(yoy_changes) if len(yoy_changes) > 1 else 1.0
            accel_thresh = max(yoy_std * 0.5, 0.5)  # at least 0.5pp to register
            if recent > older + accel_thresh:
                acceleration = 'ACCELERATING'
            elif recent < older - accel_thresh:
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
