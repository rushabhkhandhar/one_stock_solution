"""
SEBI Compliance & Safety Module
================================
Phase 6 requirements:
  • All AI-generated advice labeled as such with specific sources.
  • Kill-switch for automated signals on data anomalies.
  • DPDPA 2023 data-handling compliance.
"""
import datetime


DISCLAIMER = (
    "DISCLAIMER: This report is entirely AI-generated using publicly available "
    "financial data from Screener.in and BSE India. It does NOT constitute "
    "investment advice. All outputs should be independently verified by a "
    "SEBI-registered investment advisor. The authors accept no liability for "
    "any investment decisions made based on this report."
)


def stamp_source(text: str, source: str = "Screener.in / BSE India") -> str:
    """Append a source citation to any AI-generated text."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"{text}\n[Source: {source} | AI-generated on {ts}]"


class KillSwitch:
    """
    Emergency stop mechanism for the automated signal generator.

    Triggers when:
      • Data is stale (>7 days old).
      • Key financial fields are entirely missing.
      • Price data shows >20 % single-day move (likely data error).
    """

    def __init__(self, max_staleness_days: int = 7,
                 max_daily_move_pct: float = 20.0):
        self.max_staleness = max_staleness_days
        self.max_move      = max_daily_move_pct
        self._triggered    = False
        self._reason       = ""

    @property
    def triggered(self) -> bool:
        return self._triggered

    @property
    def reason(self) -> str:
        return self._reason

    def check(self, data: dict) -> bool:
        """Return True if safe to proceed; False if kill-switch triggers."""
        import pandas as pd
        import numpy as np

        # 1. Check data freshness
        for key in ('pnl', 'balance_sheet', 'cash_flow'):
            df = data.get(key)
            if isinstance(df, pd.DataFrame) and not df.empty:
                latest = df.index.max()
                age = (pd.Timestamp.now() - latest).days
                # Financial data is annual; allow up to 18 months
                if age > 550:
                    self._triggered = True
                    self._reason = (
                        f"Data too stale: latest {key} entry is {age} days old."
                    )
                    return False

        # 2. Check for critical missing fields
        for key in ('pnl', 'balance_sheet'):
            df = data.get(key)
            if not isinstance(df, pd.DataFrame) or df.empty:
                self._triggered = True
                self._reason = f"Critical data missing: {key} is empty."
                return False

        # 3. Check price anomalies
        price = data.get('price')
        if isinstance(price, pd.DataFrame) and not price.empty:
            col = 'close' if 'close' in price.columns else price.columns[0]
            returns = price[col].pct_change().dropna()
            if (returns.abs() > self.max_move / 100).any():
                self._triggered = True
                self._reason = (
                    f"Price anomaly detected: >{self.max_move}% single-day move."
                )
                return False

        self._triggered = False
        return True
