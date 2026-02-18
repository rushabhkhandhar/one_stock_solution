"""
Data Preprocessing & Field Mapping
-----------------------------------
Handles:
  • Canonical field-name resolution (scraper strips spaces / +)
  • Data cleaning (NaN, dtype enforcement)
  • Derived metric computation (shares outstanding, ROE, D/E, etc.)
"""
import pandas as pd
import numpy as np


# ======================================================================
# Helper utilities
# ======================================================================

def _normalize(s: str) -> str:
    """Normalize a column name by stripping \xa0 and spaces, lowercasing."""
    return s.replace('\xa0', '').replace(' ', '').lower()


def find_column(df: pd.DataFrame, possible_names: list) -> pd.Series:
    """
    Look up a column by trying several possible names.
    Handles \xa0 (non-breaking space) in scraper column headers.
    Returns a NaN-filled series if nothing is found.
    """
    for name in possible_names:
        # exact match
        if name in df.columns:
            return df[name]
        # normalized match (strips \xa0, spaces, case)
        norm = _normalize(name)
        for col in df.columns:
            if _normalize(col) == norm:
                return df[col]
    return pd.Series(dtype=float, index=df.index,
                     name=possible_names[0] if possible_names else 'unknown')


def get_value(series: pd.Series, idx: int = -1):
    """Safely get a positional value from a series; returns np.nan on failure."""
    try:
        val = series.iloc[idx]
        return val if pd.notna(val) else np.nan
    except (IndexError, KeyError, TypeError):
        return np.nan


# ======================================================================
# DataPreprocessor
# ======================================================================

class DataPreprocessor:
    """
    Canonical field-name mapping.

    The scraper strips spaces and '+' from HTML headers:
        "Operating Profit"  → "OperatingProfit"
        "Cash from Operating Activity +"  → "CashfromOperatingActivity"
    Addon (API) data strips only spaces:
        "Fixed assets purchased" → "Fixedassetspurchased"
    """

    FIELD_MAP = {
        # ── P&L ──────────────────────────────────────────────
        'sales':             ['Sales'],
        'expenses':          ['Expenses'],
        'operating_profit':  ['OperatingProfit'],
        'opm':               ['OPM%'],
        'other_income':      ['OtherIncome'],
        'interest':          ['Interest'],
        'depreciation':      ['Depreciation'],
        'pbt':               ['Profitbeforetax'],
        'tax_pct':           ['Tax%'],
        'tax':               ['Tax', 'Taxpaid', 'TaxPaid'],
        'net_profit':        ['NetProfit'],
        'eps':               ['EPSinRs', 'EPS'],
        'dividend_payout':   ['DividendPayout%'],
        # ── Balance Sheet ────────────────────────────────────
        'equity_capital':    ['EquityCapital'],
        'reserves':          ['Reserves'],
        'borrowings':        ['Borrowings'],
        'other_liabilities': ['OtherLiabilities'],
        'total_liabilities': ['TotalLiabilities'],
        'fixed_assets':      ['FixedAssets'],
        'cwip':              ['CWIP'],
        'investments':       ['Investments'],
        'cash_equivalents':  ['CashEquivalents'],
        'other_assets':      ['OtherAssets'],
        'total_assets':      ['TotalAssets'],
        # ── Cash Flow ────────────────────────────────────────
        'operating_cf':      ['CashfromOperatingActivity'],
        'investing_cf':      ['CashfromInvestingActivity'],
        'financing_cf':      ['CashfromFinancingActivity'],
        'net_cf':            ['NetCashFlow'],
        # ── Ratios ───────────────────────────────────────────
        'debtor_days':       ['DebtorDays'],
        'inventory_days':    ['InventoryDays'],
        'days_payable':      ['DaysPayable'],
        'cash_conversion':   ['CashConversionCycle'],
        'working_capital_days': ['WorkingCapitalDays'],
        'roce':              ['ROCE%'],
        # ── Shareholding ─────────────────────────────────────
        'promoters':         ['Promoters'],
        'fiis':              ['FIIs', 'FII'],
        'diis':              ['DIIs', 'DII'],
        'government':        ['Government', 'Gov'],
        'public':            ['Public'],
    }

    def get(self, df: pd.DataFrame, canonical_name: str) -> pd.Series:
        """Retrieve a column by its canonical (friendly) name."""
        names = self.FIELD_MAP.get(canonical_name, [canonical_name])
        return find_column(df, names)

    # ------------------------------------------------------------------
    # Cleaning
    # ------------------------------------------------------------------
    def clean(self, data: dict) -> dict:
        """Enforce numeric dtypes across all financial DataFrames."""
        for key in ('pnl', 'balance_sheet', 'cash_flow', 'ratios',
                    'quarterly', 'shareholding'):
            if key in data and isinstance(data[key], pd.DataFrame):
                df = data[key]
                for col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                data[key] = df
        return data

    # ------------------------------------------------------------------
    # Derived metrics
    # ------------------------------------------------------------------
    def compute_derived(self, data: dict) -> dict:
        """Add commonly used derived series to *data*."""
        pnl = data.get('pnl', pd.DataFrame())
        bs  = data.get('balance_sheet', pd.DataFrame())

        if pnl.empty:
            return data

        sales       = self.get(pnl, 'sales')
        net_profit  = self.get(pnl, 'net_profit')
        eps         = self.get(pnl, 'eps')

        # Shares outstanding (₹ Cr of net-profit / ₹ per-share EPS → Cr shares)
        with np.errstate(divide='ignore', invalid='ignore'):
            shares = net_profit / eps
        data['shares_outstanding'] = shares

        # PAT margin (decimal)
        with np.errstate(divide='ignore', invalid='ignore'):
            data['pat_margin'] = net_profit / sales

        if not bs.empty:
            equity_capital = self.get(bs, 'equity_capital')
            reserves       = self.get(bs, 'reserves')
            borrowings     = self.get(bs, 'borrowings')
            equity = equity_capital.add(reserves, fill_value=0)

            # ROE
            common = equity.index.intersection(net_profit.index)
            if len(common) > 0:
                with np.errstate(divide='ignore', invalid='ignore'):
                    data['roe'] = net_profit.loc[common] / equity.loc[common]

            # Debt / Equity
            common = equity.index.intersection(borrowings.index)
            if len(common) > 0:
                with np.errstate(divide='ignore', invalid='ignore'):
                    data['debt_to_equity'] = borrowings.loc[common] / equity.loc[common]

        return data


# Module-level convenience instance
pp = DataPreprocessor()
