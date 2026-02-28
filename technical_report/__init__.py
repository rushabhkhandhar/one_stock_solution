"""
Technical Report Module
========================
Standalone, institutional-grade technical & quantitative analysis pipeline.
Generates a self-contained PDF report with high-fidelity visualisations.

Data acquisition re-uses ``data.realtime_feeds.RealtimeFeeds`` — no
separate data loader needed.

Modules:
    risk_metrics     – Sharpe, Sortino, Calmar, MDD, VaR, CVaR
    volatility_model – GARCH wrapper, rolling vol, ATR bands, regimes
    price_levels     – Algorithmic S/R (K-Means), Fibonacci retracement
    indicators       – VPVR, VWAP, RSI/MACD divergence, Bollinger %B
    visualizations   – mplfinance candlestick + multi-panel charts
    report_builder   – Assembles metrics + plots into Markdown / PDF
    pipeline         – Single entry-point orchestrator

Usage:
    python -m technical_report.pipeline RELIANCE
    python -m technical_report.pipeline TCS --days 500 --no-pdf
"""

from technical_report.pipeline import run_technical_report  # noqa: F401
