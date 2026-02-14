"""
═══════════════════════════════════════════════════════════
  Advanced Equity Research System
  ───────────────────────────────
  Just provide a stock name — the system does the rest.

  Usage:
      python main.py TCS
      python main.py RELIANCE
      python main.py "HDFC BANK"
═══════════════════════════════════════════════════════════
"""
import sys
from agents.orchestrator import Orchestrator


def main():
    # ── Get stock name from CLI or interactive prompt ──
    if len(sys.argv) > 1:
        stock_name = " ".join(sys.argv[1:])
    else:
        stock_name = input("\n  Enter stock name / symbol: ").strip()

    if not stock_name:
        print("  ✗ Please provide a stock name.")
        sys.exit(1)

    print(f"\n🔍  Starting equity research for: {stock_name}")
    print("─" * 60)

    # ── Run the full pipeline ──
    orchestrator = Orchestrator()
    filepath = orchestrator.analyze(stock_name)

    # ── Print report to terminal ──
    print("\n" + "─" * 60)
    print("  FULL REPORT")
    print("─" * 60 + "\n")
    with open(filepath, 'r', encoding='utf-8') as f:
        print(f.read())


if __name__ == "__main__":
    main()
