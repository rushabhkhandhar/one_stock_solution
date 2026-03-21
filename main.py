"""
═══════════════════════════════════════════════════════════
  Advanced Equity Research System
  ───────────────────────────────
    Run single-stock or batch watchlist research.

  Usage:
      python main.py TCS
      python main.py RELIANCE
      python main.py "HDFC BANK"
            python main.py --symbols "RELIANCE,TCS,HDFCBANK"
            python main.py --watchlist-file watchlist.txt
═══════════════════════════════════════════════════════════
"""
import sys
import os
import argparse
from typing import Optional

# ── Auto-activate .venv if running from system Python ─────
_venv_python = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '.venv', 'bin', 'python')
if (
    os.path.exists(_venv_python)
    and os.path.abspath(sys.executable) != os.path.abspath(_venv_python)
    and 'VIRTUAL_ENV' not in os.environ
):
    os.execv(_venv_python, [_venv_python] + sys.argv)

from agents.orchestrator import Orchestrator
from agents.batch_runner import BatchWatchlistRunner


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Advanced Equity Research System (single-stock and batch watchlist mode)"
    )
    parser.add_argument(
        "symbol",
        nargs="?",
        help="Single stock symbol or name (for single-stock mode)",
    )
    parser.add_argument(
        "--watchlist-file",
        dest="watchlist_file",
        help="Path to a newline-separated watchlist file",
    )
    parser.add_argument(
        "--symbols",
        dest="symbols",
        help="Comma-separated symbols for batch mode (example: RELIANCE,TCS,HDFCBANK)",
    )
    parser.add_argument(
        "--no-print-report",
        action="store_true",
        help="Do not print full markdown reports to terminal",
    )
    return parser


def _run_single(stock_name: str, print_report: bool = True) -> int:
    print(f"\n🔍  Starting equity research for: {stock_name}")
    print("─" * 60)

    orchestrator = Orchestrator()
    filepath = orchestrator.analyze(stock_name)

    if print_report:
        print("\n" + "─" * 60)
        print("  FULL REPORT")
        print("─" * 60 + "\n")
        with open(filepath, 'r', encoding='utf-8') as f:
            print(f.read())
    return 0


def _run_batch(*, watchlist_file: Optional[str], symbols_csv: Optional[str]) -> int:
    runner = BatchWatchlistRunner()

    if watchlist_file:
        symbols = runner.load_watchlist_file(watchlist_file)
        source_label = f"file: {watchlist_file}"
    elif symbols_csv:
        symbols = runner.parse_symbols_csv(symbols_csv)
        source_label = "--symbols"
    else:
        print("  ✗ Batch mode requires --watchlist-file or --symbols.")
        return 2

    print("\n📚  Starting batch watchlist research")
    print(f"  Source: {source_label}")
    print(f"  Symbols: {len(symbols)}")
    print("─" * 60)

    result = runner.run(symbols)
    summary_path = runner.save_summary_csv(result)

    print("\n" + "═" * 60)
    print("  BATCH SUMMARY")
    print("═" * 60)
    print(f"  Total   : {result['total']}")
    print(f"  Success : {result['success_count']}")
    print(f"  Failed  : {result['failure_count']}")
    print(f"  Summary : {summary_path}")
    print("═" * 60 + "\n")

    if result['failure_count'] > 0:
        return 1
    return 0


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.watchlist_file and args.symbols:
        parser.error("Use either --watchlist-file or --symbols, not both.")

    batch_mode = bool(args.watchlist_file or args.symbols)

    if batch_mode:
        if args.symbol:
            parser.error("Do not pass positional symbol in batch mode.")
        exit_code = _run_batch(
            watchlist_file=args.watchlist_file,
            symbols_csv=args.symbols,
        )
        sys.exit(exit_code)

    stock_name = args.symbol
    if not stock_name:
        stock_name = input("\n  Enter stock name / symbol: ").strip()

    if not stock_name:
        print("  ✗ Please provide a stock name.")
        sys.exit(2)

    exit_code = _run_single(
        stock_name=stock_name,
        print_report=not args.no_print_report,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
