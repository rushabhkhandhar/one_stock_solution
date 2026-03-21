"""
Batch Watchlist Runner
======================
Runs the full orchestrator pipeline for multiple symbols and writes a
run summary CSV for portfolio-level tracking.
"""
from __future__ import annotations

import csv
import datetime
import os
import time

from agents.orchestrator import Orchestrator
from config import config


class BatchWatchlistRunner:
    """Execute full research pipeline across a user-provided watchlist."""

    @staticmethod
    def _normalize_symbols(symbols: list[str]) -> list[str]:
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

    def parse_symbols_csv(self, symbols_csv: str) -> list[str]:
        if symbols_csv is None:
            raise ValueError("--symbols cannot be empty")

        symbols = [s for s in symbols_csv.split(",")]
        normalized = self._normalize_symbols(symbols)

        if not normalized:
            raise ValueError("No valid symbols found in --symbols input")
        return normalized

    def load_watchlist_file(self, file_path: str) -> list[str]:
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Watchlist file not found: {file_path}")

        symbols = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                value = line.strip()
                if not value:
                    continue
                if value.startswith("#"):
                    continue
                symbols.append(value)

        normalized = self._normalize_symbols(symbols)
        if not normalized:
            raise ValueError("Watchlist file has no valid symbols")
        return normalized

    def run(self, symbols: list[str]) -> dict:
        normalized = self._normalize_symbols(symbols)
        if not normalized:
            raise ValueError("Batch run requires at least one valid symbol")

        orchestrator = Orchestrator()
        items = []

        for idx, symbol in enumerate(normalized, start=1):
            started_at = time.time()
            print(f"\n[{idx}/{len(normalized)}] {symbol}")
            try:
                report_path = orchestrator.analyze(symbol)
                elapsed = round(time.time() - started_at, 2)
                items.append({
                    "symbol": symbol,
                    "status": "SUCCESS",
                    "elapsed_sec": elapsed,
                    "report_path": report_path,
                    "error": "",
                })
                print(f"  ✔ Completed in {elapsed:.2f}s")
            except Exception as exc:
                elapsed = round(time.time() - started_at, 2)
                items.append({
                    "symbol": symbol,
                    "status": "FAILED",
                    "elapsed_sec": elapsed,
                    "report_path": "",
                    "error": str(exc),
                })
                print(f"  ✗ Failed in {elapsed:.2f}s: {exc}")

        success_count = sum(1 for item in items if item["status"] == "SUCCESS")
        failure_count = len(items) - success_count

        return {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(items),
            "success_count": success_count,
            "failure_count": failure_count,
            "items": items,
        }

    def save_summary_csv(self, batch_result: dict, output_dir: str | None = None) -> str:
        if output_dir is None:
            output_dir = config.output_dir

        os.makedirs(output_dir, exist_ok=True)
        dt = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(output_dir, f"watchlist_batch_summary_{dt}.csv")

        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["symbol", "status", "elapsed_sec", "report_path", "error"],
            )
            writer.writeheader()
            for item in batch_result.get("items", []):
                writer.writerow(item)

        return out_path
