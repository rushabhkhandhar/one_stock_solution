"""
Rule-Based Stock Screener Engine
================================
Evaluates user-defined numeric rules against live computed metrics for a
watchlist of symbols.
"""
from __future__ import annotations

import csv
import datetime
import math
import os
import re
import time
from dataclasses import dataclass
from typing import List

from config import config
from data.ingestion import DataIngestion
from data.preprocessing import DataPreprocessor
from quant.forensics import BeneishMScore
from quant.piotroski import PiotroskiFScore
from quant.ratios import FinancialRatios


@dataclass
class ScreeningRule:
    field: str
    operator: str
    threshold: float

    @property
    def expression(self) -> str:
        return f"{self.field}{self.operator}{self.threshold}"


class RuleBasedStockScreener:
    """Run strict rule evaluation using live computed symbol metrics."""

    _RULE_PATTERN = re.compile(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(<=|>=|==|!=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$"
    )

    def __init__(self):
        self.ingestion = DataIngestion()
        self.preprocessor = DataPreprocessor()
        self.ratios_calc = FinancialRatios()
        self.fscore_model = PiotroskiFScore()
        self.mscore_model = BeneishMScore()

    @staticmethod
    def _normalize_symbols(symbols: List[str]) -> List[str]:
        cleaned = []
        seen = set()
        for raw in symbols:
            symbol = raw.strip().upper()
            if not symbol:
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            cleaned.append(symbol)
        return cleaned

    def parse_rules(self, rule_text: str) -> List[ScreeningRule]:
        if rule_text is None:
            raise ValueError("Rule text cannot be empty")

        fragments = []
        for line in rule_text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            for token in stripped.split(","):
                expr = token.strip()
                if expr:
                    fragments.append(expr)

        if not fragments:
            raise ValueError("No valid rules found")

        rules = []
        for expr in fragments:
            match = self._RULE_PATTERN.match(expr)
            if not match:
                raise ValueError(
                    f"Invalid rule expression: '{expr}'. "
                    "Expected format like 'pe_ratio<=25'"
                )
            field, operator, threshold_text = match.groups()
            rules.append(
                ScreeningRule(
                    field=field,
                    operator=operator,
                    threshold=float(threshold_text),
                )
            )
        return rules

    def load_rules_file(self, file_path: str) -> List[ScreeningRule]:
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Rules file not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
        return self.parse_rules(text)

    @staticmethod
    def _evaluate(value: float, operator: str, threshold: float) -> bool:
        if operator == ">":
            return value > threshold
        if operator == ">=":
            return value >= threshold
        if operator == "<":
            return value < threshold
        if operator == "<=":
            return value <= threshold
        if operator == "==":
            return value == threshold
        if operator == "!=":
            return value != threshold
        raise ValueError(f"Unsupported operator: {operator}")

    def _compute_metrics(self, symbol: str) -> dict:
        data = self.ingestion.load_company(symbol, consolidated=config.consolidated)
        data = self.preprocessor.clean(data)
        data = self.preprocessor.compute_derived(data)

        ratios = self.ratios_calc.calculate(data)
        fscore = self.fscore_model.calculate(data)
        mscore = self.mscore_model.calculate(data)

        metrics = {}
        for key, value in ratios.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                if math.isfinite(float(value)):
                    metrics[key] = float(value)

        if fscore.get("available") and fscore.get("f_score") is not None:
            metrics["f_score"] = float(fscore["f_score"])
        if mscore.get("available") and mscore.get("m_score") is not None:
            metrics["m_score"] = float(mscore["m_score"])

        return metrics

    def run(self, symbols: List[str], rules: List[ScreeningRule]) -> dict:
        normalized = self._normalize_symbols(symbols)
        if not normalized:
            raise ValueError("Screener requires at least one valid symbol")
        if not rules:
            raise ValueError("Screener requires at least one rule")

        items = []
        for idx, symbol in enumerate(normalized, start=1):
            started_at = time.time()
            print(f"\n[{idx}/{len(normalized)}] Screening {symbol}")
            try:
                metrics = self._compute_metrics(symbol)
                rule_checks = []

                for rule in rules:
                    value = metrics.get(rule.field)
                    if value is None:
                        rule_checks.append({
                            "rule": rule.expression,
                            "field": rule.field,
                            "operator": rule.operator,
                            "threshold": rule.threshold,
                            "value": None,
                            "passed": False,
                            "reason": "missing_metric",
                        })
                        continue

                    passed = self._evaluate(value, rule.operator, rule.threshold)
                    rule_checks.append({
                        "rule": rule.expression,
                        "field": rule.field,
                        "operator": rule.operator,
                        "threshold": rule.threshold,
                        "value": round(float(value), 6),
                        "passed": passed,
                        "reason": "",
                    })

                passed_rules = sum(1 for rc in rule_checks if rc["passed"])
                total_rules = len(rule_checks)
                eligible = passed_rules == total_rules
                elapsed = round(time.time() - started_at, 2)

                items.append({
                    "symbol": symbol,
                    "status": "SUCCESS",
                    "eligible": eligible,
                    "passed_rules": passed_rules,
                    "total_rules": total_rules,
                    "pass_rate": round((passed_rules / total_rules) * 100, 2),
                    "elapsed_sec": elapsed,
                    "metrics": metrics,
                    "rule_checks": rule_checks,
                    "error": "",
                })
                print(
                    f"  ✔ {passed_rules}/{total_rules} rules passed "
                    f"({('ELIGIBLE' if eligible else 'NOT ELIGIBLE')})"
                )
            except Exception as exc:
                elapsed = round(time.time() - started_at, 2)
                items.append({
                    "symbol": symbol,
                    "status": "FAILED",
                    "eligible": False,
                    "passed_rules": 0,
                    "total_rules": len(rules),
                    "pass_rate": 0.0,
                    "elapsed_sec": elapsed,
                    "metrics": {},
                    "rule_checks": [],
                    "error": str(exc),
                })
                print(f"  ✗ Screening failed: {exc}")

        success_count = sum(1 for i in items if i["status"] == "SUCCESS")
        failure_count = len(items) - success_count
        eligible_count = sum(1 for i in items if i["status"] == "SUCCESS" and i["eligible"])

        return {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(items),
            "success_count": success_count,
            "failure_count": failure_count,
            "eligible_count": eligible_count,
            "rule_expressions": [r.expression for r in rules],
            "items": items,
        }

    def save_results_csv(self, screening_result: dict, output_dir: str = None) -> str:
        if output_dir is None:
            output_dir = config.output_dir

        os.makedirs(output_dir, exist_ok=True)
        dt = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(output_dir, f"screener_results_{dt}.csv")

        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "symbol",
                    "status",
                    "eligible",
                    "passed_rules",
                    "total_rules",
                    "pass_rate",
                    "elapsed_sec",
                    "error",
                ],
            )
            writer.writeheader()
            for item in screening_result.get("items", []):
                writer.writerow({
                    "symbol": item.get("symbol", ""),
                    "status": item.get("status", ""),
                    "eligible": item.get("eligible", False),
                    "passed_rules": item.get("passed_rules", 0),
                    "total_rules": item.get("total_rules", 0),
                    "pass_rate": item.get("pass_rate", 0.0),
                    "elapsed_sec": item.get("elapsed_sec", 0.0),
                    "error": item.get("error", ""),
                })

        return out_path
