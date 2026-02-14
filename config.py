"""
Central Configuration for the Equity Research System.
All tunable parameters, API keys, and thresholds in one place.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MarketDefaults:
    """Default market parameters for Indian equity markets."""
    risk_free_rate: float = 0.071       # 10Y India G-Sec yield (~7.1%)
    market_risk_premium: float = 0.06   # Historical equity risk premium India
    default_beta: float = 1.0           # Default beta (market)
    terminal_growth_rate: float = 0.04  # Long-term nominal GDP growth (~4%)
    tax_rate: float = 0.25              # Corporate tax rate (new regime)
    projection_years: int = 10          # DCF projection horizon
    currency_unit: str = "Cr"           # All figures in ₹ Crores


@dataclass
class Thresholds:
    """Scoring thresholds for quantitative models."""
    # Beneish M-Score
    mscore_manipulation: float = -1.78   # Above → likely manipulator
    mscore_safe: float = -2.22           # Below → unlikely manipulator
    # Piotroski F-Score
    fscore_strong: int = 8               # 8-9: Strong
    fscore_moderate: int = 5             # 5-7: Moderate
    # Valuation
    pe_expensive: float = 50.0           # P/E above this is expensive
    debt_equity_high: float = 1.5        # D/E above this is risky


@dataclass
class APIKeys:
    """API configuration for external services (Phase 2+)."""
    mistral_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    qdrant_url: str = "localhost"
    qdrant_port: int = 6333


@dataclass
class Config:
    """Master configuration object."""
    market: MarketDefaults = field(default_factory=MarketDefaults)
    thresholds: Thresholds = field(default_factory=Thresholds)
    api: APIKeys = field(default_factory=APIKeys)
    output_dir: str = "./output"
    consolidated: bool = True

    def __post_init__(self):
        os.makedirs(self.output_dir, exist_ok=True)
        self.api.mistral_api_key = os.getenv("MISTRAL_API_KEY")
        self.api.openai_api_key = os.getenv("OPENAI_API_KEY")


# Singleton config instance
config = Config()
