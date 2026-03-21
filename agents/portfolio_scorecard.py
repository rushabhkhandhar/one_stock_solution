"""
Portfolio Ranking and Scorecard
===============================
Builds a comparable ranking table from screener outputs using percentile
scores across user-selected metrics.
"""
from __future__ import annotations

import csv
import datetime
import os
from dataclasses import dataclass
from typing import List

import pandas as pd

from config import config


@dataclass
class MetricDirection:
    metric: str
    higher_better: bool

    @property
    def label(self) -> str:
        return "desc" if self.higher_better else "asc"


class PortfolioRankingScorecard:
    """Rank screened symbols using percentile-based composite scores."""

    def parse_metric_directions(self, metric_text: str) -> List[MetricDirection]:
        if metric_text is None:
            raise ValueError("Rank metrics cannot be empty")

        specs = []
        seen = set()
        for token in metric_text.split(","):
            entry = token.strip()
            if not entry:
                continue
            if ":" not in entry:
                raise ValueError(
                    f"Invalid rank metric '{entry}'. Use format 'metric:asc' or 'metric:desc'"
                )
            metric, direction = entry.split(":", 1)
            metric = metric.strip()
            direction = direction.strip().lower()
            if direction not in ("asc", "desc"):
                raise ValueError(
                    f"Invalid direction '{direction}' for metric '{metric}'. "
                    "Allowed values: asc, desc"
                )
            if not metric:
                raise ValueError("Rank metric name cannot be empty")
            if metric in seen:
                continue
            seen.add(metric)
            specs.append(MetricDirection(metric=metric, higher_better=(direction == "desc")))

        if not specs:
            raise ValueError("No valid rank metrics provided")
        return specs

    def infer_metric_directions_from_rules(self, rules: List) -> List[MetricDirection]:
        inferred = []
        seen = set()
        for rule in rules:
            if rule.field in seen:
                continue
            if rule.operator in (">", ">=", "=="):
                inferred.append(MetricDirection(metric=rule.field, higher_better=True))
                seen.add(rule.field)
            elif rule.operator in ("<", "<="):
                inferred.append(MetricDirection(metric=rule.field, higher_better=False))
                seen.add(rule.field)
        return inferred

    def rank(self, screening_result: dict, metric_directions: List[MetricDirection], eligible_only: bool = True) -> dict:
        if not metric_directions:
            raise ValueError("At least one ranking metric is required")

        items = screening_result.get("items", [])
        rows = []
        for item in items:
            if item.get("status") != "SUCCESS":
                continue
            if eligible_only and not item.get("eligible"):
                continue
            row = {
                "symbol": item.get("symbol"),
                "eligible": item.get("eligible", False),
                "passed_rules": item.get("passed_rules", 0),
                "total_rules": item.get("total_rules", 0),
            }
            metrics = item.get("metrics", {})
            for md in metric_directions:
                row[md.metric] = metrics.get(md.metric)
            rows.append(row)

        if not rows:
            source = "eligible" if eligible_only else "successful"
            raise ValueError(f"No {source} symbols available for ranking")

        df = pd.DataFrame(rows)
        score_columns = []
        omitted_metrics = []

        for md in metric_directions:
            metric = md.metric
            score_col = f"score_{metric}"
            values = pd.to_numeric(df[metric], errors="coerce")
            valid = values.dropna()

            if len(valid) < 2:
                omitted_metrics.append(metric)
                continue

            percentile = values.rank(pct=True, ascending=md.higher_better) * 100
            df[score_col] = percentile.round(2)
            score_columns.append(score_col)

        if not score_columns:
            raise ValueError(
                "No ranking metrics had enough data across symbols to compute scores"
            )

        df["composite_score"] = df[score_columns].mean(axis=1, skipna=True)
        df = df[df["composite_score"].notna()].copy()

        if df.empty:
            raise ValueError("Composite score could not be computed for any symbol")

        df["rank"] = df["composite_score"].rank(method="min", ascending=False).astype(int)
        df = df.sort_values(by=["rank", "symbol"], ascending=[True, True])

        records = []
        for _, row in df.iterrows():
            record = {
                "symbol": row["symbol"],
                "rank": int(row["rank"]),
                "composite_score": round(float(row["composite_score"]), 2),
                "eligible": bool(row["eligible"]),
                "passed_rules": int(row["passed_rules"]),
                "total_rules": int(row["total_rules"]),
            }
            for md in metric_directions:
                metric = md.metric
                value = row.get(metric)
                score_key = f"score_{metric}"
                score_value = row.get(score_key)
                record[metric] = (None if pd.isna(value) else round(float(value), 6))
                record[score_key] = (None if pd.isna(score_value) else round(float(score_value), 2))
            records.append(record)

        return {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ranked_count": len(records),
            "eligible_only": eligible_only,
            "metric_directions": [f"{md.metric}:{md.label}" for md in metric_directions],
            "omitted_metrics": omitted_metrics,
            "items": records,
        }

    def save_scorecard_csv(self, scorecard_result: dict, output_dir: str = None) -> str:
        if output_dir is None:
            output_dir = config.output_dir
        os.makedirs(output_dir, exist_ok=True)

        dt = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(output_dir, f"portfolio_scorecard_{dt}.csv")

        items = scorecard_result.get("items", [])
        if not items:
            raise ValueError("Scorecard result has no rows to write")

        fieldnames = list(items[0].keys())
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in items:
                writer.writerow(row)

        return out_path
