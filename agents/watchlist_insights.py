"""
Watchlist Insights Engine
=========================
Post-run analysis utilities for watchlist workflows:
  1) Daily drift report (upgrade/downgrade deltas)
  2) Alert engine (valuation gap, risk breach, technical breaks)
  3) Re-run comparison (current vs previous run summary)
"""
from __future__ import annotations

import csv
import datetime
import os
import re
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ReportSnapshot:
    symbol: str
    report_path: str
    rating: Optional[str]
    target_price: Optional[float]
    current_price: Optional[float]
    upside_pct: Optional[float]
    trend_signal: Optional[str]
    risk_high_count: int
    risk_medium_count: int
    has_bearish_technical_risk: bool


class WatchlistInsightsEngine:
    """Build drift, alerts, and run-over-run comparisons from report history."""

    _RATING_ORDER = {
        "SELL": 0,
        "HOLD": 1,
        "BUY": 2,
        "SUSPENDED": -1,
    }

    def __init__(self, output_dir: str = "./output"):
        self.output_dir = output_dir
        self.history_dir = os.path.join(output_dir, "history")
        os.makedirs(self.history_dir, exist_ok=True)

    def create_snapshot(self, symbol: str, report_path: str) -> str:
        """Create immutable timestamped snapshot for re-run comparisons."""
        if not os.path.exists(report_path):
            raise FileNotFoundError(f"Report file not found for snapshot: {report_path}")

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        md_name = f"{symbol.upper()}_Research_{ts}.md"
        md_snapshot = os.path.join(self.history_dir, md_name)
        shutil.copy2(report_path, md_snapshot)

        pdf_path = os.path.splitext(report_path)[0] + ".pdf"
        if os.path.exists(pdf_path):
            pdf_name = f"{symbol.upper()}_Research_{ts}.pdf"
            pdf_snapshot = os.path.join(self.history_dir, pdf_name)
            shutil.copy2(pdf_path, pdf_snapshot)

        return md_snapshot

    def parse_report_file(self, symbol: str, report_path: str) -> ReportSnapshot:
        with open(report_path, "r", encoding="utf-8") as f:
            text = f.read()

        rating = self._extract_rating(text)
        target_price = self._extract_table_currency(text, "Target Price (DCF)")
        current_price = self._extract_table_currency(text, "Current Price")
        upside_pct = self._extract_table_percent(text, "Upside / Downside")

        # Fallback to scenario weighted-target block when DCF table values
        # are unavailable (common for non-DCF or data-limited runs).
        weighted = self._extract_weighted_target_triplet(text)
        if weighted is not None:
            w_target, w_upside, w_current = weighted
            if target_price is None:
                target_price = w_target
            if upside_pct is None:
                upside_pct = w_upside
            if current_price is None:
                current_price = w_current

        # Final fallback: use consensus reconciliation row if available.
        consensus = self._extract_consensus_target_upside(text)
        if consensus is not None:
            c_target, c_upside = consensus
            if target_price is None:
                target_price = c_target
            if upside_pct is None:
                upside_pct = c_upside

        trend_signal = self._extract_table_text(text, "Trend Signal")

        risk_section = self._extract_section(text, "## ⚠️ Risk Factors & Red Flags")
        risk_lines = [ln.strip() for ln in risk_section.splitlines() if ln.strip().startswith("-")]
        risk_high_count = sum(1 for ln in risk_lines if "🔴" in ln)
        risk_medium_count = sum(1 for ln in risk_lines if "🟡" in ln)
        has_bearish_technical_risk = any("Bearish Technical Signal" in ln for ln in risk_lines)

        return ReportSnapshot(
            symbol=symbol.upper(),
            report_path=report_path,
            rating=rating,
            target_price=target_price,
            current_price=current_price,
            upside_pct=upside_pct,
            trend_signal=trend_signal,
            risk_high_count=risk_high_count,
            risk_medium_count=risk_medium_count,
            has_bearish_technical_risk=has_bearish_technical_risk,
        )

    def build_rerun_comparison(self, batch_items: List[dict]) -> List[dict]:
        rows = []
        for item in batch_items:
            if item.get("status") != "SUCCESS":
                continue

            symbol = item["symbol"].upper()
            current_snapshot_path = item.get("snapshot_path")
            if not current_snapshot_path:
                continue

            current = self.parse_report_file(symbol, current_snapshot_path)
            previous_path = self._find_previous_snapshot_path(symbol, current_snapshot_path)

            if previous_path is None:
                rows.append({
                    "symbol": symbol,
                    "comparison_status": "NEW_RUN",
                    "previous_report": "",
                    "current_report": current.report_path,
                    "previous_rating": "",
                    "current_rating": current.rating or "",
                    "rating_delta": "NEW",
                    "previous_upside_pct": "",
                    "current_upside_pct": self._fmt_float(current.upside_pct),
                    "upside_delta_pp": "",
                    "previous_target_price": "",
                    "current_target_price": self._fmt_float(current.target_price),
                    "target_delta_pct": "",
                    "previous_risk_high_count": "",
                    "current_risk_high_count": current.risk_high_count,
                    "risk_high_delta": "",
                    "previous_trend_signal": "",
                    "current_trend_signal": current.trend_signal or "",
                    "technical_changed": "",
                    "change_summary": "First tracked run for symbol",
                })
                continue

            previous = self.parse_report_file(symbol, previous_path)

            rating_delta = self._rating_delta(previous.rating, current.rating)
            upside_delta = self._delta_pp(previous.upside_pct, current.upside_pct)
            target_delta_pct = self._delta_pct(previous.target_price, current.target_price)
            risk_delta = current.risk_high_count - previous.risk_high_count
            technical_changed = (previous.trend_signal or "") != (current.trend_signal or "")

            changed_fields = []
            if rating_delta != "UNCHANGED":
                changed_fields.append(f"rating={rating_delta}")
            if upside_delta is not None and abs(upside_delta) > 0:
                changed_fields.append(f"upside_delta={upside_delta:+.2f}pp")
            if target_delta_pct is not None and abs(target_delta_pct) > 0:
                changed_fields.append(f"target_delta={target_delta_pct:+.2f}%")
            if risk_delta != 0:
                changed_fields.append(f"risk_high_delta={risk_delta:+d}")
            if technical_changed:
                changed_fields.append("technical_signal_changed")

            rows.append({
                "symbol": symbol,
                "comparison_status": "COMPARED",
                "previous_report": previous.report_path,
                "current_report": current.report_path,
                "previous_rating": previous.rating or "",
                "current_rating": current.rating or "",
                "rating_delta": rating_delta,
                "previous_upside_pct": self._fmt_float(previous.upside_pct),
                "current_upside_pct": self._fmt_float(current.upside_pct),
                "upside_delta_pp": self._fmt_float(upside_delta),
                "previous_target_price": self._fmt_float(previous.target_price),
                "current_target_price": self._fmt_float(current.target_price),
                "target_delta_pct": self._fmt_float(target_delta_pct),
                "previous_risk_high_count": previous.risk_high_count,
                "current_risk_high_count": current.risk_high_count,
                "risk_high_delta": risk_delta,
                "previous_trend_signal": previous.trend_signal or "",
                "current_trend_signal": current.trend_signal or "",
                "technical_changed": technical_changed,
                "change_summary": "; ".join(changed_fields) if changed_fields else "No material change",
            })

        return rows

    def build_drift_report(self, rerun_rows: List[dict]) -> List[dict]:
        drift_rows = []
        for row in rerun_rows:
            status = row.get("comparison_status")
            if status == "NEW_RUN":
                drift_rows.append({
                    "symbol": row["symbol"],
                    "drift_status": "NEW",
                    "previous_rating": row.get("previous_rating", ""),
                    "current_rating": row.get("current_rating", ""),
                    "rating_delta": row.get("rating_delta", ""),
                    "previous_upside_pct": row.get("previous_upside_pct", ""),
                    "current_upside_pct": row.get("current_upside_pct", ""),
                    "upside_delta_pp": row.get("upside_delta_pp", ""),
                    "risk_high_delta": row.get("risk_high_delta", ""),
                    "technical_changed": row.get("technical_changed", ""),
                    "summary": row.get("change_summary", ""),
                })
                continue

            rating_delta = row.get("rating_delta", "UNCHANGED")
            if rating_delta == "UPGRADE":
                drift_status = "UPGRADE"
            elif rating_delta == "DOWNGRADE":
                drift_status = "DOWNGRADE"
            elif row.get("technical_changed"):
                drift_status = "TECHNICAL_SHIFT"
            elif row.get("upside_delta_pp") not in ("", None) and abs(float(row.get("upside_delta_pp"))) > 0:
                drift_status = "VALUATION_DRIFT"
            else:
                drift_status = "UNCHANGED"

            drift_rows.append({
                "symbol": row["symbol"],
                "drift_status": drift_status,
                "previous_rating": row.get("previous_rating", ""),
                "current_rating": row.get("current_rating", ""),
                "rating_delta": rating_delta,
                "previous_upside_pct": row.get("previous_upside_pct", ""),
                "current_upside_pct": row.get("current_upside_pct", ""),
                "upside_delta_pp": row.get("upside_delta_pp", ""),
                "risk_high_delta": row.get("risk_high_delta", ""),
                "technical_changed": row.get("technical_changed", ""),
                "summary": row.get("change_summary", ""),
            })

        return drift_rows

    def build_alerts(self, batch_items: List[dict]) -> List[dict]:
        snapshots = []
        for item in batch_items:
            if item.get("status") != "SUCCESS":
                continue
            symbol = item["symbol"].upper()
            snapshot_path = item.get("snapshot_path")
            if not snapshot_path:
                continue
            snapshots.append(self.parse_report_file(symbol, snapshot_path))

        if not snapshots:
            return []

        valuation_values = [abs(s.upside_pct) for s in snapshots if s.upside_pct is not None]
        risk_values = [s.risk_high_count for s in snapshots]

        valuation_threshold = self._quantile(valuation_values, 0.75)
        risk_threshold = self._quantile(risk_values, 0.75)

        alerts = []
        for snap in snapshots:
            # Valuation gap alert
            if (
                valuation_threshold is not None
                and snap.upside_pct is not None
                and abs(snap.upside_pct) >= valuation_threshold
                and abs(snap.upside_pct) > 0
            ):
                alerts.append({
                    "symbol": snap.symbol,
                    "alert_type": "VALUATION_GAP",
                    "severity": "HIGH" if abs(snap.upside_pct) == max(valuation_values) else "MEDIUM",
                    "message": f"Upside/Downside at {snap.upside_pct:+.2f}% exceeds watchlist valuation drift threshold",
                    "report_path": snap.report_path,
                })

            # Risk breach alert
            if snap.risk_high_count > 0 and snap.risk_high_count >= risk_threshold:
                alerts.append({
                    "symbol": snap.symbol,
                    "alert_type": "RISK_BREACH",
                    "severity": "HIGH" if snap.risk_high_count >= 2 else "MEDIUM",
                    "message": f"High-severity risk flags detected: {snap.risk_high_count}",
                    "report_path": snap.report_path,
                })

            # Technical break alert
            trend = (snap.trend_signal or "").upper()
            is_bearish = "BEARISH" in trend
            if is_bearish or snap.has_bearish_technical_risk:
                severity = "HIGH" if "STRONG_BEARISH" in trend else "MEDIUM"
                alerts.append({
                    "symbol": snap.symbol,
                    "alert_type": "TECHNICAL_BREAK",
                    "severity": severity,
                    "message": f"Technical breakdown signal detected ({snap.trend_signal or 'Bearish risk flag'})",
                    "report_path": snap.report_path,
                })

        return alerts

    def save_rows_csv(self, rows: List[dict], prefix: str) -> str:
        os.makedirs(self.output_dir, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.output_dir, f"{prefix}_{ts}.csv")

        with open(path, "w", encoding="utf-8", newline="") as f:
            if not rows:
                writer = csv.writer(f)
                writer.writerow(["status", "message"])
                writer.writerow(["EMPTY", "No rows generated"])
                return path

            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------
    def _find_previous_snapshot_path(self, symbol: str, current_path: str) -> Optional[str]:
        files = [
            os.path.join(self.history_dir, name)
            for name in os.listdir(self.history_dir)
            if name.startswith(f"{symbol.upper()}_Research_") and name.endswith(".md")
        ]
        files = sorted(files)
        if current_path not in files:
            return files[-1] if files else None

        idx = files.index(current_path)
        if idx == 0:
            return None
        return files[idx - 1]

    @staticmethod
    def _extract_rating(text: str) -> Optional[str]:
        m = re.search(r"^##\s+.*Rating:\s*([A-Za-z_ ]+)", text, flags=re.MULTILINE)
        if not m:
            return None
        raw = m.group(1).strip().upper()
        token = raw.split()[0] if raw else ""
        return token or None

    @staticmethod
    def _extract_table_currency(text: str, label: str) -> Optional[float]:
        pattern = re.compile(
            rf"\|\s*(?:\*\*)?{re.escape(label)}(?:\*\*)?\s*\|\s*([^|\n]+)\|",
            re.IGNORECASE,
        )
        m = pattern.search(text)
        if not m:
            return None
        return WatchlistInsightsEngine._parse_float(m.group(1))

    @staticmethod
    def _extract_table_percent(text: str, label: str) -> Optional[float]:
        pattern = re.compile(
            rf"\|\s*(?:\*\*)?{re.escape(label)}(?:\*\*)?\s*\|\s*([^|\n]+)\|",
            re.IGNORECASE,
        )
        m = pattern.search(text)
        if not m:
            return None
        return WatchlistInsightsEngine._parse_float(m.group(1))

    @staticmethod
    def _extract_table_text(text: str, label: str) -> Optional[str]:
        pattern = re.compile(rf"\|\s*{re.escape(label)}\s*\|\s*([^|]+)\|", re.IGNORECASE)
        m = pattern.search(text)
        if not m:
            return None
        value = m.group(1)
        value = value.replace("*", "").strip()
        return value or None

    @staticmethod
    def _extract_weighted_target_triplet(text: str) -> Optional[tuple[float, float, float]]:
        pattern = re.compile(
            r"Probability-Weighted\s+Target:\s*₹\s*([0-9,]+(?:\.\d+)?)\*\*"
            r"(?:\s*\n)?\s*\(\s*([+-]?\d+(?:\.\d+)?)%\s*from\s*current\s*₹\s*([0-9,]+(?:\.\d+)?)\s*\)",
            re.IGNORECASE,
        )
        m = pattern.search(text)
        if not m:
            return None
        target = WatchlistInsightsEngine._parse_float(m.group(1))
        upside = WatchlistInsightsEngine._parse_float(m.group(2))
        current = WatchlistInsightsEngine._parse_float(m.group(3))
        if target is None or upside is None or current is None:
            return None
        return target, upside, current

    @staticmethod
    def _extract_consensus_target_upside(text: str) -> Optional[tuple[float, float]]:
        pattern = re.compile(
            r"\|\s*\*\*Consensus\s*\(Average\)\*\*\s*\|\s*\*\*₹\s*([^|*]+)\*\*\s*\|\s*\*\*([+-]?\d+(?:\.\d+)?)%\*\*\s*\|",
            re.IGNORECASE,
        )
        m = pattern.search(text)
        if not m:
            return None
        target = WatchlistInsightsEngine._parse_float(m.group(1))
        upside = WatchlistInsightsEngine._parse_float(m.group(2))
        if target is None or upside is None:
            return None
        return target, upside

    @staticmethod
    def _extract_section(text: str, heading: str) -> str:
        idx = text.find(heading)
        if idx < 0:
            return ""
        after = text[idx + len(heading):]
        next_h = after.find("\n## ")
        if next_h < 0:
            return after
        return after[:next_h]

    @staticmethod
    def _parse_float(value: str) -> Optional[float]:
        cleaned = value.replace(",", "")
        m = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)
        if not m:
            return None
        try:
            return float(m.group(0))
        except ValueError:
            return None

    @staticmethod
    def _delta_pp(previous: Optional[float], current: Optional[float]) -> Optional[float]:
        if previous is None or current is None:
            return None
        return round(current - previous, 4)

    @staticmethod
    def _delta_pct(previous: Optional[float], current: Optional[float]) -> Optional[float]:
        if previous is None or current is None or previous == 0:
            return None
        return round((current - previous) / abs(previous) * 100, 4)

    def _rating_delta(self, previous: Optional[str], current: Optional[str]) -> str:
        if previous is None or current is None:
            return "UNKNOWN"
        p = self._RATING_ORDER.get(previous)
        c = self._RATING_ORDER.get(current)
        if p is None or c is None:
            return "UNKNOWN"
        if c > p:
            return "UPGRADE"
        if c < p:
            return "DOWNGRADE"
        return "UNCHANGED"

    @staticmethod
    def _quantile(values: List[float], q: float) -> Optional[float]:
        if not values:
            return None
        arr = sorted(float(v) for v in values)
        if len(arr) == 1:
            return arr[0]
        idx = (len(arr) - 1) * q
        lo = int(idx)
        hi = min(lo + 1, len(arr) - 1)
        if lo == hi:
            return arr[lo]
        frac = idx - lo
        return arr[lo] + (arr[hi] - arr[lo]) * frac

    @staticmethod
    def _fmt_float(value: Optional[float]) -> str:
        if value is None:
            return ""
        return f"{value:.4f}"
