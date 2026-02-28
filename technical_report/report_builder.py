"""
Technical Report Builder
==========================
Assembles metrics from all analysis modules plus chart image files into
a structured **Markdown** document, then delegates to the existing
``reports.pdf_exporter`` to produce a PDF.

Output artefacts (per symbol):
  • ``{SYMBOL}_Technical_Report.md``
  • ``{SYMBOL}_Technical_Report.pdf``
  • ``output/{SYMBOL}/plots/*.png`` (chart images)
"""
from __future__ import annotations

import os
import datetime
from typing import Optional


def _fmt(value, decimals: int = 2, pct: bool = False, prefix: str = "") -> str:
    """Format a numeric value for the report table."""
    if value is None:
        return "N/A"
    try:
        v = float(value)
        s = f"{v:,.{decimals}f}"
        if pct:
            s += "%"
        return f"{prefix}{s}"
    except (ValueError, TypeError):
        return str(value)


def _row(label: str, value, decimals: int = 2, pct: bool = False,
         prefix: str = "") -> str:
    """Generate a Markdown table row."""
    return f"| {label} | {_fmt(value, decimals, pct, prefix)} |"


class TechnicalReportBuilder:
    """Compose the final technical analysis report in Markdown."""

    def __init__(self, symbol: str, output_dir: str = "./output"):
        self.symbol = symbol.upper()
        self.output_dir = output_dir
        self.report_dir = os.path.join(output_dir, self.symbol)
        os.makedirs(self.report_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def build(
        self,
        price_df,
        risk_metrics: dict,
        vol_model: dict,
        price_levels: dict,
        vpvr_data: dict,
        vwap_data: dict,
        rsi_data: dict,
        macd_data: dict,
        boll_data: dict,
        chart_paths: dict[str, str] | None = None,
    ) -> str:
        """Generate the full Markdown report. Returns path to .md file."""
        sections = [
            self._header(price_df),
            self._risk_section(risk_metrics),
            self._volatility_section(vol_model),
            self._price_levels_section(price_levels),
            self._volume_section(vpvr_data, vwap_data),
            self._momentum_section(rsi_data, macd_data, boll_data),
            self._charts_section(chart_paths or {}),
            self._footer(),
        ]

        md_text = "\n\n".join(s for s in sections if s)

        md_path = os.path.join(
            self.report_dir,
            f"{self.symbol}_Technical_Report.md"
        )
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_text)

        return md_path

    def build_pdf(self, md_path: str) -> str:
        """Convert the Markdown report to PDF using existing exporter."""
        try:
            from reports.pdf_exporter import export_markdown_to_pdf
            pdf_path = export_markdown_to_pdf(
                md_path, self.symbol, self.report_dir
            )
            return pdf_path
        except Exception as e:
            print(f"[report_builder] PDF export failed: {e}")
            return ""

    # ------------------------------------------------------------------
    # Section generators
    # ------------------------------------------------------------------
    def _header(self, price_df) -> str:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        last_close = float(price_df["close"].iloc[-1]) if price_df is not None else 0
        last_date = str(price_df.index[-1].date()) if price_df is not None else "N/A"
        n_bars = len(price_df) if price_df is not None else 0

        return f"""# {self.symbol} — Technical Analysis Report

> **Generated:** {now} | **Last Close:** ₹{last_close:,.2f} ({last_date}) | **Data Points:** {n_bars}

---

*This is an automated, standalone technical analysis report.
It is independent of fundamental data and should be used alongside
fundamental research for investment decisions.*

---"""

    # ── Risk & Performance ──
    def _risk_section(self, r: dict) -> str:
        if not r.get("available", True):
            return "## 1. Risk & Performance Metrics\n\n*Insufficient data.*"

        perf = r.get("performance", {})
        ratios = r.get("ratios", {})
        dd = r.get("drawdown", {})
        var = r.get("var", {})

        lines = [
            "## 1. Risk & Performance Metrics",
            "",
            "### Return Distribution",
            "| Metric | Value |",
            "| :--- | ---: |",
            _row("CAGR", perf.get("cagr_pct"), pct=True),
            _row("Annualised Volatility", perf.get("annualised_volatility_pct"), pct=True),
            _row("Skewness", perf.get("skewness")),
            _row("Kurtosis", perf.get("kurtosis")),
            "",
            "### Risk-Adjusted Ratios",
            "| Ratio | Value |",
            "| :--- | ---: |",
            _row("Sharpe Ratio", ratios.get("sharpe_ratio")),
            _row("Sortino Ratio", ratios.get("sortino_ratio")),
            _row("Calmar Ratio", ratios.get("calmar_ratio")),
            _row("Information Ratio", ratios.get("information_ratio")),
            _row("Omega Ratio", ratios.get("omega_ratio")),
            _row("Tail Ratio", ratios.get("tail_ratio")),
            _row("Gain-to-Pain", ratios.get("gain_to_pain_ratio")),
            "",
            "### Drawdown Analysis",
            "| Metric | Value |",
            "| :--- | ---: |",
            _row("Max Drawdown", dd.get("max_drawdown_pct"), pct=True),
            f"| Peak Date | {dd.get('peak_date', 'N/A')} |",
            f"| Trough Date | {dd.get('trough_date', 'N/A')} |",
            f"| Recovery Date | {dd.get('recovery_date', 'N/A')} |",
            "",
            "### Value at Risk (95% CI)",
            "| Method | Value |",
            "| :--- | ---: |",
            _row("Historical VaR", var.get("historical_var_pct"), pct=True),
            _row("Parametric VaR (Gaussian)", var.get("parametric_var_pct"), pct=True),
            _row("Cornish-Fisher VaR", var.get("cornish_fisher_var_pct"), pct=True),
            _row("CVaR / Expected Shortfall", var.get("historical_cvar_pct"), pct=True),
        ]
        return "\n".join(lines)

    # ── Volatility ──
    def _volatility_section(self, v: dict) -> str:
        if not v.get("available", True):
            return "## 2. Volatility Analysis\n\n*Insufficient data.*"

        rv = v.get("rolling_vol", {})
        rv_windows = rv.get("windows", {})
        regime = v.get("regime", {})
        atr = v.get("atr_bands", {})
        est = v.get("estimators", {})
        garch = v.get("garch", {})

        lines = [
            "## 2. Volatility Analysis",
            "",
            "### Rolling Annualised Volatility",
            "| Window | Value |",
            "| :--- | ---: |",
            _row("10-day", rv_windows.get("10d", {}).get("current_pct"), pct=True),
            _row("21-day", rv_windows.get("21d", {}).get("current_pct"), pct=True),
            _row("63-day", rv_windows.get("63d", {}).get("current_pct"), pct=True),
            _row("126-day", rv_windows.get("126d", {}).get("current_pct"), pct=True),
            _row("252-day", rv_windows.get("252d", {}).get("current_pct"), pct=True),
            "",
            "### Volatility Regime",
            f"| Current Regime | **{regime.get('regime', 'N/A')}** |",
            f"| Vol Percentile (252d) | {_fmt(regime.get('percentile'), 1, True)} |",
            "",
            "### ATR Bands",
            "| Metric | Value |",
            "| :--- | ---: |",
            _row("ATR (14-day)", atr.get("current_atr"), prefix="₹"),
            _row("ATR as % of Price", atr.get("current_atr_pct"), pct=True),
            "",
            "### Range Estimators",
            "| Estimator | Ann. Vol |",
            "| :--- | ---: |",
            _row("Parkinson", est.get("parkinson_vol_pct"), pct=True),
            _row("Garman-Klass", est.get("garman_klass_vol_pct"), pct=True),
        ]

        if garch.get("available"):
            lines += [
                "",
                "### GARCH Analysis",
                f"| Model | {garch.get('model', 'N/A')} |",
                _row("Annualised Cond. Vol", garch.get("annualised_vol_pct"), pct=True),
                f"| Vol Regime | {garch.get('vol_regime', 'N/A')} |",
            ]

        return "\n".join(lines)

    # ── Price Levels ──
    def _price_levels_section(self, pl: dict) -> str:
        if not pl.get("available", True):
            return "## 3. Support & Resistance Levels\n\n*Insufficient data.*"

        merged = pl.get("merged", {})
        levels = merged.get("levels", [])
        fib = pl.get("fibonacci", {})

        lines = [
            "## 3. Support & Resistance Levels",
            "",
            "### Algorithmically-Derived Levels (K-Means + Pivot + Fibonacci)",
            "| Price (₹) | Type | Source | Strength |",
            "| ---: | :--- | :--- | :--- |",
        ]
        for lvl in levels[:12]:
            lines.append(
                f"| {lvl.get('price', 0):,.2f} | "
                f"{lvl.get('type', '').upper()} | "
                f"{lvl.get('source', '')} | "
                f"{'★' * lvl.get('strength', 1)} |"
            )

        if fib.get("available", False):
            lines += [
                "",
                "### Fibonacci Retracement",
                "| Level | Price (₹) |",
                "| :--- | ---: |",
            ]
            for name in ["0.0%", "23.6%", "38.2%", "50.0%", "61.8%", "78.6%", "100.0%"]:
                key = name
                val = fib.get("levels", {}).get(key)
                if val is not None:
                    lines.append(f"| {name} | ₹{val:,.2f} |")

        return "\n".join(lines)

    # ── Volume Analysis ──
    def _volume_section(self, vpvr: dict, vwap: dict) -> str:
        lines = ["## 4. Volume Analysis"]

        if vpvr.get("available"):
            lines += [
                "",
                "### Volume Profile (VPVR)",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Point of Control (POC)", vpvr.get("poc_price"), prefix="₹"),
                _row("Value Area High", vpvr.get("value_area_high"), prefix="₹"),
                _row("Value Area Low", vpvr.get("value_area_low"), prefix="₹"),
            ]
        else:
            lines.append("\n*VPVR data unavailable.*")

        if vwap.get("available"):
            lines += [
                "",
                "### VWAP",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Current VWAP", vwap.get("current_vwap"), prefix="₹"),
                _row("Price", vwap.get("current_price"), prefix="₹"),
                _row("Deviation from VWAP", vwap.get("deviation_pct"), pct=True),
                f"| Position | **{vwap.get('position', 'N/A')}** |",
            ]

        return "\n".join(lines)

    # ── Momentum ──
    def _momentum_section(self, rsi: dict, macd: dict, boll: dict) -> str:
        lines = ["## 5. Momentum & Oscillators"]

        if rsi.get("available"):
            lines += [
                "",
                "### RSI (14-period)",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Current RSI", rsi.get("current_rsi")),
                f"| Divergence | **{rsi.get('divergence', 'NONE')}** |",
                f"| Signal | {rsi.get('signal', '—')} |",
                f"| Extreme | {rsi.get('extreme', '—')} |",
            ]

        if macd.get("available"):
            lines += [
                "",
                "### MACD (12, 26, 9)",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("MACD Line", macd.get("macd"), 4),
                _row("Signal Line", macd.get("signal"), 4),
                _row("Histogram", macd.get("histogram"), 4),
                f"| Crossover | **{macd.get('crossover', 'N/A')}** |",
                f"| Histogram Momentum | **{macd.get('histogram_trend', 'N/A')}** *(absolute magnitude trend)* |",
            ]

        if boll.get("available"):
            lines += [
                "",
                "### Bollinger Bands (20, 2σ)",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Upper Band", boll.get("current_upper"), prefix="₹"),
                _row("SMA (20)", boll.get("current_sma"), prefix="₹"),
                _row("Lower Band", boll.get("current_lower"), prefix="₹"),
                _row("%B", boll.get("pct_b"), 4),
                _row("Bandwidth", boll.get("bandwidth_pct"), pct=True),
                f"| Squeeze | **{'YES' if boll.get('squeeze') else 'NO'}** |",
            ]

        return "\n".join(lines)

    # ── Charts ──
    def _charts_section(self, chart_paths: dict[str, str]) -> str:
        if not chart_paths:
            return "## 6. Charts\n\n*No charts generated.*"

        lines = ["## 6. Charts", ""]
        chart_labels = {
            "candlestick": "Candlestick with S/R & VWAP",
            "volume_profile": "Volume Profile (VPVR)",
            "momentum": "Momentum Dashboard (RSI / MACD / Bollinger %B)",
            "risk_panel": "Risk & Volatility Panel",
            "summary": "Summary Dashboard",
        }
        for key, path in chart_paths.items():
            label = chart_labels.get(key, key)
            rel_path = os.path.relpath(path, self.report_dir)
            lines.append(f"### {label}\n")
            lines.append(f"![{label}]({rel_path})\n")

        return "\n".join(lines)

    # ── Footer ──
    def _footer(self) -> str:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""---

## Disclaimer

> This report is generated by an automated technical analysis engine.
> It does **not** constitute financial advice. Past performance is not
> indicative of future results. Always combine technical analysis with
> fundamental research and risk management.

*Report generated at {now} IST.*
"""
