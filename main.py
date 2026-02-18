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
import os

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
