"""
═══════════════════════════════════════════════════════════
  Advanced Equity Research System
  ───────────────────────────────
    Run single-stock research, batch watchlist research, or
    rule-based screening with portfolio ranking.

  Usage:
      python main.py TCS
      python main.py RELIANCE
      python main.py "HDFC BANK"
            python main.py --symbols "RELIANCE,TCS,HDFCBANK"
            python main.py --watchlist-file watchlist.txt
            python main.py --symbols "RELIANCE,TCS" --screen-rules "pe_ratio<=25,roe>=12,f_score>=6"
            python main.py --watchlist-file watchlist.txt --screen-rules-file rules.txt --rank-metrics "roe:desc,pe_ratio:asc,f_score:desc"
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
from agents.screener_engine import RuleBasedStockScreener
from agents.portfolio_scorecard import PortfolioRankingScorecard


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Advanced Equity Research System "
            "(single-stock, batch watchlist, and screener modes)"
        )
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
    parser.add_argument(
        "--screen-rules",
        dest="screen_rules",
        help="Inline screener rules (example: pe_ratio<=25,roe>=12,f_score>=6)",
    )
    parser.add_argument(
        "--screen-rules-file",
        dest="screen_rules_file",
        help="Path to screener rules file (one rule per line or comma-separated)",
    )
    parser.add_argument(
        "--rank-metrics",
        dest="rank_metrics",
        help=(
            "Portfolio ranking metrics with directions "
            "(example: roe:desc,pe_ratio:asc,f_score:desc)"
        ),
    )
    parser.add_argument(
        "--rank-all",
        action="store_true",
        help="Rank all successfully screened symbols (default ranks only eligible symbols)",
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
    insights = runner.generate_watchlist_insights(result)

    print("\n" + "═" * 60)
    print("  BATCH SUMMARY")
    print("═" * 60)
    print(f"  Total   : {result['total']}")
    print(f"  Success : {result['success_count']}")
    print(f"  Failed  : {result['failure_count']}")
    print(f"  Summary : {summary_path}")
    print(f"  Drift   : {insights['drift_report_path']}")
    print(f"  Alerts  : {insights['alerts_path']}")
    print(f"  Compare : {insights['rerun_comparison_path']}")
    print("═" * 60 + "\n")

    if result['failure_count'] > 0:
        return 1
    return 0


def _resolve_batch_symbols(
    runner: BatchWatchlistRunner,
    watchlist_file: Optional[str],
    symbols_csv: Optional[str],
) -> list:
    if watchlist_file:
        return runner.load_watchlist_file(watchlist_file)
    if symbols_csv:
        return runner.parse_symbols_csv(symbols_csv)
    raise ValueError("A symbols source is required (--symbols or --watchlist-file)")


def _run_screening(
    *,
    watchlist_file: Optional[str],
    symbols_csv: Optional[str],
    screen_rules: Optional[str],
    screen_rules_file: Optional[str],
    rank_metrics: Optional[str],
    rank_all: bool,
) -> int:
    batch_runner = BatchWatchlistRunner()
    screener = RuleBasedStockScreener()
    scorecard = PortfolioRankingScorecard()

    symbols = _resolve_batch_symbols(batch_runner, watchlist_file, symbols_csv)

    if screen_rules_file:
        rules = screener.load_rules_file(screen_rules_file)
        rule_source = f"file: {screen_rules_file}"
    elif screen_rules:
        rules = screener.parse_rules(screen_rules)
        rule_source = "--screen-rules"
    else:
        raise ValueError("Screener mode requires --screen-rules or --screen-rules-file")

    print("\n🧮  Starting rule-based stock screener")
    print(f"  Symbols: {len(symbols)}")
    print(f"  Rules  : {len(rules)} ({rule_source})")
    print("─" * 60)

    screening_result = screener.run(symbols, rules)
    screener_csv = screener.save_results_csv(screening_result)

    print("\n" + "═" * 60)
    print("  SCREENER SUMMARY")
    print("═" * 60)
    print(f"  Total    : {screening_result['total']}")
    print(f"  Success  : {screening_result['success_count']}")
    print(f"  Failed   : {screening_result['failure_count']}")
    print(f"  Eligible : {screening_result['eligible_count']}")
    print(f"  CSV      : {screener_csv}")

    if rank_metrics:
        metric_directions = scorecard.parse_metric_directions(rank_metrics)
    else:
        metric_directions = scorecard.infer_metric_directions_from_rules(rules)
        if not metric_directions:
            raise ValueError(
                "No rank metrics were provided and ranking could not be inferred from rules. "
                "Use --rank-metrics explicitly."
            )

    scorecard_result = scorecard.rank(
        screening_result=screening_result,
        metric_directions=metric_directions,
        eligible_only=not rank_all,
    )
    scorecard_csv = scorecard.save_scorecard_csv(scorecard_result)

    print(f"  Ranked   : {scorecard_result['ranked_count']}")
    print(f"  Scorecard: {scorecard_csv}")
    if scorecard_result.get('omitted_metrics'):
        print(f"  Omitted metrics (insufficient data): {', '.join(scorecard_result['omitted_metrics'])}")
    print("═" * 60 + "\n")

    if screening_result['failure_count'] > 0:
        return 1
    return 0


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.watchlist_file and args.symbols:
        parser.error("Use either --watchlist-file or --symbols, not both.")

    if args.screen_rules and args.screen_rules_file:
        parser.error("Use either --screen-rules or --screen-rules-file, not both.")

    screen_mode = bool(args.screen_rules or args.screen_rules_file)
    batch_mode = bool(args.watchlist_file or args.symbols)

    if args.rank_metrics and not screen_mode:
        parser.error("--rank-metrics can only be used with screener mode")

    if args.rank_all and not screen_mode:
        parser.error("--rank-all can only be used with screener mode")

    if screen_mode:
        if args.symbol:
            parser.error("Do not pass positional symbol in screener mode.")
        if not batch_mode:
            parser.error("Screener mode requires --symbols or --watchlist-file.")
        try:
            exit_code = _run_screening(
                watchlist_file=args.watchlist_file,
                symbols_csv=args.symbols,
                screen_rules=args.screen_rules,
                screen_rules_file=args.screen_rules_file,
                rank_metrics=args.rank_metrics,
                rank_all=args.rank_all,
            )
        except Exception as exc:
            print(f"  ✗ Screener run failed: {exc}")
            sys.exit(2)
        sys.exit(exit_code)

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
