"""
Technical Report Pipeline — Single Entry-Point
=================================================
Orchestrates the full technical analysis → chart → PDF workflow.

Usage (standalone):
    python -m technical_report.pipeline RELIANCE
    python -m technical_report.pipeline AXISCADES --days 500

Usage (from code):
    from technical_report.pipeline import run_technical_report
    result = run_technical_report("RELIANCE", days=365)

Reuses existing infrastructure:
  • data.realtime_feeds.RealtimeFeeds   — OHLCV data via yfinance
  • quant.technicals.TechnicalAnalyzer  — baseline technical indicators
  • predictive.arima_ets.HybridPredictor — GARCH engine (via volatility_model)
  • reports.pdf_exporter                — Markdown → PDF
"""
from __future__ import annotations

import argparse
import os
import sys
import traceback
import datetime
from typing import Optional

import pandas as pd


def _fetch_ohlcv(symbol: str, days: int = 365) -> pd.DataFrame:
    """Fetch OHLCV data using existing RealtimeFeeds."""
    try:
        from data.realtime_feeds import RealtimeFeeds
        feeds = RealtimeFeeds()
        df = feeds.stock_history(symbol, days=days)
        if df is not None and not df.empty:
            # Normalise columns to lowercase
            df.columns = [c.lower().strip() for c in df.columns]
            # Ensure DatetimeIndex
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            return df
    except Exception as e:
        print(f"[pipeline] RealtimeFeeds failed: {e}")

    # Fallback: direct yfinance call
    try:
        import yfinance as yf
        suffixes = [".NS", ".BO"]
        for sfx in suffixes:
            ticker = yf.Ticker(f"{symbol}{sfx}")
            df = ticker.history(period=f"{days}d")
            if df is not None and not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]
                # Drop extra yfinance columns if present
                for col in ["dividends", "stock splits", "capital gains"]:
                    if col in df.columns:
                        df.drop(columns=[col], inplace=True)
                return df
    except Exception as e:
        print(f"[pipeline] yfinance fallback failed: {e}")

    return pd.DataFrame()


def _fetch_benchmark_close(days: int = 365) -> pd.Series | None:
    """Fetch Nifty 50 benchmark close prices for Information Ratio."""
    try:
        from data.realtime_feeds import RealtimeFeeds
        feeds = RealtimeFeeds()
        nifty = feeds.nifty50_history(days=days)
        if nifty is not None and not nifty.empty:
            nifty.columns = [c.lower().strip() for c in nifty.columns]
            return nifty["close"]
    except Exception:
        pass

    try:
        import yfinance as yf
        nifty = yf.Ticker("^NSEI").history(period=f"{days}d")
        if nifty is not None and not nifty.empty:
            nifty.columns = [c.lower().strip() for c in nifty.columns]
            return nifty["close"]
    except Exception:
        pass

    return None


def run_technical_report(
    symbol: str,
    days: int = 365,
    output_dir: str = "./output",
    generate_pdf: bool = True,
) -> dict:
    """
    Execute the full technical analysis pipeline.

    Parameters
    ----------
    symbol      : Stock symbol (e.g. "RELIANCE", "TCS")
    days        : Number of trading days of OHLCV history
    output_dir  : Root output directory
    generate_pdf: Whether to export Markdown → PDF

    Returns
    -------
    dict with keys:
      - success   : bool
      - symbol    : str
      - md_path   : path to Markdown report
      - pdf_path  : path to PDF (if generated)
      - chart_paths: {name: filepath}
      - metrics   : dict of all computed metrics
      - error     : error message (if failed)
    """
    result = {
        "success": False,
        "symbol": symbol.upper(),
        "md_path": "",
        "pdf_path": "",
        "chart_paths": {},
        "metrics": {},
        "error": "",
    }

    print(f"\n{'='*60}")
    print(f"  Technical Analysis Report — {symbol.upper()}")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # ── 1. Fetch data ────────────────────────────────────────
    print("\n[1/9] Fetching OHLCV data ...")
    df = _fetch_ohlcv(symbol, days)
    if df.empty:
        result["error"] = "No OHLCV data retrieved"
        print(f"  ✗ {result['error']}")
        return result
    print(f"  ✓ {len(df)} bars retrieved ({df.index[0].date()} → {df.index[-1].date()})")

    close = df["close"].astype(float)
    high = df["high"].astype(float) if "high" in df.columns else close
    low = df["low"].astype(float) if "low" in df.columns else close
    volume = df["volume"].astype(float) if "volume" in df.columns else None

    # Benchmark close prices for Information Ratio
    benchmark_close = _fetch_benchmark_close(days)

    # ── 2. Compute Risk Metrics ──────────────────────────────
    print("\n[2/9] Computing risk & performance metrics ...")
    try:
        from technical_report.risk_metrics import RiskMetricsEngine
        risk_engine = RiskMetricsEngine()
        risk_metrics = risk_engine.compute_all(
            close, benchmark_close=benchmark_close
        )
        print(f"  ✓ Sharpe: {risk_metrics.get('ratios', {}).get('sharpe_ratio', 'N/A')}")
    except Exception as e:
        risk_metrics = {"available": False, "error": str(e)}
        print(f"  ⚠ Risk metrics failed: {e}")

    # ── 3. Compute Volatility Model ──────────────────────────
    print("\n[3/9] Computing volatility analytics ...")
    try:
        from technical_report.volatility_model import VolatilityModel
        vol_model = VolatilityModel()
        vol_data = vol_model.compute_all(df)
        regime = vol_data.get("regime", {}).get("regime", "N/A")
        print(f"  ✓ Regime: {regime}")
    except Exception as e:
        vol_data = {"available": False, "error": str(e)}
        print(f"  ⚠ Volatility model failed: {e}")

    # ── 4. Compute Price Levels & Indicators ─────────────────
    print("\n[4/9] Computing S/R levels & momentum indicators ...")
    try:
        from technical_report.price_levels import PriceLevelDetector
        pl_engine = PriceLevelDetector()
        price_levels = pl_engine.compute_all(df)
        n_levels = len(price_levels.get("merged", {}).get("levels", []))
        print(f"  ✓ {n_levels} S/R levels detected")
    except Exception as e:
        price_levels = {"available": False, "error": str(e)}
        print(f"  ⚠ Price levels failed: {e}")

    try:
        from technical_report.indicators import VolumeProfiler, MomentumAnalyzer
        vpvr_data = VolumeProfiler.vpvr(close, volume, high, low)
        vwap_data = VolumeProfiler.vwap(close, high, low, volume) if volume is not None else {"available": False}
        rsi_data = MomentumAnalyzer.rsi_divergence(close)
        macd_data = MomentumAnalyzer.macd_analysis(close)
        boll_data = MomentumAnalyzer.bollinger_extended(close)
        print(f"  ✓ VPVR POC: ₹{vpvr_data.get('poc_price', 'N/A')}")
        print(f"  ✓ RSI: {rsi_data.get('current_rsi', 'N/A')} | MACD: {macd_data.get('crossover', 'N/A')}")
    except Exception as e:
        vpvr_data = {"available": False}
        vwap_data = {"available": False}
        rsi_data = {"available": False}
        macd_data = {"available": False}
        boll_data = {"available": False}
        print(f"  ⚠ Indicators failed: {e}")

    # ── 5. Advanced Statistical Features ─────────────────────
    print("\n[5/9] Computing advanced statistics (HMM, Hurst, ACF) ...")
    hmm_data = {"available": False}
    hurst_data = {"available": False}
    acf_data = {"available": False}
    try:
        from technical_report.advanced_stats import (
            hmm_regime_detection, hurst_exponent, acf_pacf,
        )
        hmm_data = hmm_regime_detection(close)
        hurst_data = hurst_exponent(close)
        acf_data = acf_pacf(close)
        if hmm_data.get("available"):
            print(f"  ✓ HMM regime: {hmm_data.get('current_regime_label', 'N/A')}")
        else:
            print(f"  ⚠ HMM: {hmm_data.get('reason', 'unavailable')}")
        print(f"  ✓ Hurst: {hurst_data.get('hurst', 'N/A')} ({hurst_data.get('interpretation', '')})")
        sig_lags = acf_data.get("significant_acf_lags", [])
        print(f"  ✓ ACF significant lags: {sig_lags[:5]}")
    except Exception as e:
        print(f"  ⚠ Advanced stats failed: {e}")

    # ── 6. Relative & Benchmark Analysis ─────────────────────
    print("\n[6/9] Computing relative analysis (Beta, Mansfield RS) ...")
    beta_data = {"available": False}
    mrs_data = {"available": False}
    try:
        from technical_report.relative_analysis import (
            rolling_beta_correlation, mansfield_relative_strength,
        )
        if benchmark_close is not None:
            beta_data = rolling_beta_correlation(close, benchmark_close)
            mrs_data = mansfield_relative_strength(close, benchmark_close)
            if beta_data.get("available"):
                print(f"  ✓ Beta: {beta_data.get('current_beta', 'N/A')} | "
                      f"Corr: {beta_data.get('current_correlation', 'N/A')}")
            if mrs_data.get("available"):
                print(f"  ✓ Mansfield RS: {mrs_data.get('current_mrs', 'N/A')} "
                      f"({mrs_data.get('interpretation', '')})")
        else:
            print("  ⚠ Benchmark data unavailable — skipping relative analysis")
    except Exception as e:
        print(f"  ⚠ Relative analysis failed: {e}")

    # ── 7. Trend Indicators ──────────────────────────────────
    print("\n[7/9] Computing trend indicators (ADX, Supertrend, Patterns) ...")
    adx_data = {"available": False}
    supertrend_data = {"available": False}
    pattern_data = {"available": False}
    try:
        from technical_report.trend_indicators import (
            adx_dmi, supertrend, candlestick_patterns,
        )
        adx_data = adx_dmi(high, low, close)
        supertrend_data = supertrend(high, low, close)
        open_col = df["open"].astype(float) if "open" in df.columns else close
        pattern_data = candlestick_patterns(open_col, high, low, close)
        if adx_data.get("available"):
            print(f"  ✓ ADX: {adx_data.get('current_adx', 'N/A')} "
                  f"({adx_data.get('trend_strength', '')})")
        if supertrend_data.get("available"):
            print(f"  ✓ Supertrend: {supertrend_data.get('current_direction', 'N/A')}")
        print(f"  ✓ Candlestick patterns: {pattern_data.get('count', 0)} found")
    except Exception as e:
        print(f"  ⚠ Trend indicators failed: {e}")

    # ── 8. Seasonality ───────────────────────────────────────
    print("\n[8/9] Computing seasonality (Monthly heatmap, Day-of-Week) ...")
    monthly_data = {"available": False}
    dow_data = {"available": False}
    try:
        from technical_report.seasonality import (
            monthly_return_heatmap, day_of_week_effect,
        )
        monthly_data = monthly_return_heatmap(close)
        dow_data = day_of_week_effect(close)
        if monthly_data.get("available"):
            print(f"  ✓ Best month: {monthly_data.get('best_month')} "
                  f"({monthly_data.get('best_month_avg_pct')}%)")
        if dow_data.get("available"):
            print(f"  ✓ Best day: {dow_data.get('best_day')} "
                  f"({dow_data.get('best_day_avg_pct')}%)")
    except Exception as e:
        print(f"  ⚠ Seasonality failed: {e}")

    # ── 9. Generate Charts ───────────────────────────────────
    print("\n[9/9] Generating charts ...")
    chart_paths = {}
    try:
        from technical_report.visualizations import TechnicalVisualizer
        viz = TechnicalVisualizer(symbol, output_dir)
        chart_paths = viz.generate_all(
            df=df,
            risk_metrics=risk_metrics,
            vol_model=vol_data,
            vpvr_data=vpvr_data,
            vwap_data=vwap_data,
            price_levels=price_levels,
            rsi_data=rsi_data,
            macd_data=macd_data,
            boll_data=boll_data,
            acf_data=acf_data,
            beta_data=beta_data,
            mrs_data=mrs_data,
            adx_data=adx_data,
            supertrend_data=supertrend_data,
            monthly_data=monthly_data,
            dow_data=dow_data,
        )
        print(f"  ✓ {len(chart_paths)} charts generated:")
        for name, path in chart_paths.items():
            print(f"    • {name}: {path}")
    except Exception as e:
        print(f"  ⚠ Chart generation failed: {e}")
        traceback.print_exc()

    # ── 10. Build Report ─────────────────────────────────────
    print("\n[10] Building report ...")
    try:
        from technical_report.report_builder import TechnicalReportBuilder
        builder = TechnicalReportBuilder(symbol, output_dir)
        md_path = builder.build(
            price_df=df,
            risk_metrics=risk_metrics,
            vol_model=vol_data,
            price_levels=price_levels,
            vpvr_data=vpvr_data,
            vwap_data=vwap_data,
            rsi_data=rsi_data,
            macd_data=macd_data,
            boll_data=boll_data,
            chart_paths=chart_paths,
            hmm_data=hmm_data,
            hurst_data=hurst_data,
            acf_data=acf_data,
            beta_data=beta_data,
            mrs_data=mrs_data,
            adx_data=adx_data,
            supertrend_data=supertrend_data,
            pattern_data=pattern_data,
            monthly_data=monthly_data,
            dow_data=dow_data,
        )
        result["md_path"] = md_path
        print(f"  ✓ Markdown: {md_path}")

        if generate_pdf:
            pdf_path = builder.build_pdf(md_path)
            result["pdf_path"] = pdf_path
            if pdf_path:
                print(f"  ✓ PDF: {pdf_path}")
            else:
                print("  ⚠ PDF generation failed (Markdown still available)")
    except Exception as e:
        print(f"  ✗ Report build failed: {e}")
        traceback.print_exc()
        result["error"] = str(e)
        return result

    # ── Assemble result ──────────────────────────────────────
    result["success"] = True
    result["chart_paths"] = chart_paths
    result["metrics"] = {
        "risk": {k: v for k, v in risk_metrics.items() if not k.startswith("_")},
        "volatility": {k: v for k, v in vol_data.items() if not k.startswith("_")},
        "price_levels": {k: v for k, v in price_levels.items() if not k.startswith("_")},
        "vpvr": {k: v for k, v in vpvr_data.items() if not k.startswith("_")},
        "vwap": {k: v for k, v in vwap_data.items() if not k.startswith("_")},
        "rsi": {k: v for k, v in rsi_data.items() if not k.startswith("_")},
        "macd": {k: v for k, v in macd_data.items() if not k.startswith("_")},
        "bollinger": {k: v for k, v in boll_data.items() if not k.startswith("_")},
        "hmm": {k: v for k, v in hmm_data.items() if not k.startswith("_")},
        "hurst": {k: v for k, v in hurst_data.items() if not k.startswith("_")},
        "acf": {k: v for k, v in acf_data.items() if not k.startswith("_")},
        "beta": {k: v for k, v in beta_data.items() if not k.startswith("_")},
        "mrs": {k: v for k, v in mrs_data.items() if not k.startswith("_")},
        "adx": {k: v for k, v in adx_data.items() if not k.startswith("_")},
        "supertrend": {k: v for k, v in supertrend_data.items() if not k.startswith("_")},
        "patterns": {k: v for k, v in pattern_data.items() if not k.startswith("_")},
        "monthly": {k: v for k, v in monthly_data.items() if not k.startswith("_")},
        "dow": {k: v for k, v in dow_data.items() if not k.startswith("_")},
    }

    print(f"\n{'='*60}")
    print(f"  Report complete for {symbol.upper()}")
    print(f"{'='*60}\n")

    return result


# ── CLI entry-point ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Standalone Technical Analysis Report Generator"
    )
    parser.add_argument(
        "symbol", type=str,
        help="Stock symbol (e.g. RELIANCE, TCS, AXISCADES)"
    )
    parser.add_argument(
        "--days", type=int, default=365,
        help="Number of trading days of history (default: 365)"
    )
    parser.add_argument(
        "--output", type=str, default="./output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "--no-pdf", action="store_true",
        help="Skip PDF generation (Markdown only)"
    )

    args = parser.parse_args()

    result = run_technical_report(
        symbol=args.symbol,
        days=args.days,
        output_dir=args.output,
        generate_pdf=not args.no_pdf,
    )

    if not result["success"]:
        print(f"\n✗ Failed: {result['error']}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
