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
        # ── new data dicts ──
        hmm_data: dict | None = None,
        hurst_data: dict | None = None,
        acf_data: dict | None = None,
        beta_data: dict | None = None,
        mrs_data: dict | None = None,
        adx_data: dict | None = None,
        supertrend_data: dict | None = None,
        pattern_data: dict | None = None,
        monthly_data: dict | None = None,
        dow_data: dict | None = None,
    ) -> str:
        """Generate the full Markdown report. Returns path to .md file."""
        sections = [
            self._header(price_df),
            self._risk_section(risk_metrics),
            self._volatility_section(vol_model),
            self._price_levels_section(price_levels),
            self._volume_section(vpvr_data, vwap_data),
            self._momentum_section(rsi_data, macd_data, boll_data),
            self._advanced_stats_section(
                hmm_data or {}, hurst_data or {}, acf_data or {}),
            self._relative_section(beta_data or {}, mrs_data or {}),
            self._trend_indicators_section(
                adx_data or {}, supertrend_data or {}, pattern_data or {}),
            self._seasonality_section(monthly_data or {}, dow_data or {}),
            self._signal_confluence_section(
                hmm_data or {}, rsi_data, macd_data,
                adx_data or {}, supertrend_data or {},
                mrs_data or {}, hurst_data or {},
            ),
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

    # ── Advanced Statistics ──
    def _advanced_stats_section(self, hmm: dict, hurst: dict, acf: dict) -> str:
        lines = ["## 6. Advanced Statistical Analysis"]

        if hmm.get("available"):
            lines += [
                "",
                "### HMM Regime Detection",
                f"| Current Regime | **{hmm.get('current_regime_label', 'N/A')}** |",
                "",
                "| Regime | Avg Daily Return | Avg 5d Volatility |",
                "| :--- | ---: | ---: |",
            ]
            for r in hmm.get("regimes", []):
                lines.append(
                    f"| {r.get('label', '?')} | "
                    f"{_fmt(r.get('avg_daily_return_pct'), 3, True)} | "
                    f"{_fmt(r.get('avg_5d_vol_pct'), 3, True)} |"
                )
        else:
            lines.append("\n*HMM regime detection unavailable.*")

        if hurst.get("available"):
            lines += [
                "",
                "### Hurst Exponent",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Hurst Exponent", hurst.get("hurst"), 4),
                f"| Interpretation | **{hurst.get('interpretation', 'N/A')}** |",
                f"| Implication | {hurst.get('implication', 'N/A')} |",
            ]

        if acf.get("available"):
            sig_lags = acf.get("significant_acf_lags", [])
            persistence = acf.get("momentum_persistence", "N/A")
            lines += [
                "",
                "### ACF / PACF Analysis",
                f"| Momentum Persistence | **{persistence}** |",
                f"| Significant ACF Lags | {', '.join(str(l) for l in sig_lags[:8]) or 'None'} |",
            ]

        return "\n".join(lines)

    # ── Relative & Benchmark Analysis ──
    def _relative_section(self, beta: dict, mrs: dict) -> str:
        lines = ["## 7. Relative & Benchmark Analysis"]

        if beta.get("available"):
            lines += [
                "",
                "### Rolling Beta & Correlation vs Nifty 50",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Current Beta", beta.get("current_beta")),
                _row("Average Beta", beta.get("avg_beta")),
                _row("Current Correlation", beta.get("current_correlation")),
                _row("Average Correlation", beta.get("avg_correlation")),
                f"| Window | {beta.get('window', 63)} trading days |",
            ]
        else:
            lines.append("\n*Benchmark data unavailable for relative analysis.*")

        if mrs.get("available"):
            lines += [
                "",
                "### Mansfield Relative Strength",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Current MRS", mrs.get("current_mrs"), 4),
                f"| Interpretation | **{mrs.get('interpretation', 'N/A')}** |",
            ]

        return "\n".join(lines)

    # ── Trend Indicators ──
    def _trend_indicators_section(
        self, adx: dict, st: dict, patterns: dict
    ) -> str:
        lines = ["## 8. Trend Quality & Advanced Indicators"]

        if adx.get("available"):
            lines += [
                "",
                "### ADX & Directional Movement Index",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("ADX", adx.get("current_adx")),
                _row("+DI", adx.get("current_plus_di")),
                _row("-DI", adx.get("current_minus_di")),
                f"| Trend Strength | **{adx.get('trend_strength', 'N/A')}** |",
                f"| Directional Bias | **{adx.get('directional_bias', 'N/A')}** |",
            ]

        if st.get("available"):
            lines += [
                "",
                "### Supertrend",
                "| Metric | Value |",
                "| :--- | ---: |",
                _row("Supertrend", st.get("current_supertrend"), prefix="₹"),
                f"| Direction | **{st.get('current_direction', 'N/A')}** |",
            ]

        if patterns.get("available", False) and patterns.get("count", 0) > 0:
            lines += [
                "",
                "### Candlestick Patterns (last 5 bars)",
                "| Pattern | Date | Signal | Strength |",
                "| :--- | :--- | :--- | :--- |",
            ]
            for p in patterns.get("patterns_found", [])[:10]:
                lines.append(
                    f"| {p.get('pattern', '')} | {p.get('date', '')} | "
                    f"{p.get('signal', '')} | {p.get('strength', '')} |"
                )
        elif not patterns.get("available", True):
            lines.append("\n*Candlestick pattern recognition unavailable (TA-Lib required).*")
        else:
            lines.append("\n*No significant candlestick patterns detected in recent bars.*")

        return "\n".join(lines)

    # ── Seasonality ──
    def _seasonality_section(self, monthly: dict, dow: dict) -> str:
        lines = ["## 9. Seasonality & Cycle Analysis"]

        if monthly.get("available"):
            lines += [
                "",
                "### Monthly Return Profile",
                f"| Best Month | **{monthly.get('best_month', 'N/A')}** "
                f"(avg {_fmt(monthly.get('best_month_avg_pct'), 2, True)}) |",
                f"| Worst Month | **{monthly.get('worst_month', 'N/A')}** "
                f"(avg {_fmt(monthly.get('worst_month_avg_pct'), 2, True)}) |",
            ]
        else:
            lines.append("\n*Insufficient data for monthly seasonality.*")

        if dow.get("available"):
            lines += [
                "",
                "### Day-of-Week Effect",
                "| Day | Avg Return | Std Dev | Count |",
                "| :--- | ---: | ---: | ---: |",
            ]
            for day_name, stats in dow.get("day_returns", {}).items():
                cnt = stats.get('count', 0)
                cnt_str = str(int(cnt)) if cnt == int(cnt) else str(cnt)
                lines.append(
                    f"| {day_name} | "
                    f"{_fmt(stats.get('avg_return_pct'), 4, True)} | "
                    f"{_fmt(stats.get('std_pct'), 4, True)} | "
                    f"{cnt_str} |"
                )
            lines += [
                "",
                f"| Best Day | **{dow.get('best_day', 'N/A')}** "
                f"(avg {_fmt(dow.get('best_day_avg_pct'), 4, True)}) |",
                f"| Worst Day | **{dow.get('worst_day', 'N/A')}** "
                f"(avg {_fmt(dow.get('worst_day_avg_pct'), 4, True)}) |",
            ]

        return "\n".join(lines)

    # ── Signal Confluence ──
    def _signal_confluence_section(
        self, hmm: dict, rsi: dict, macd: dict,
        adx: dict, st: dict, mrs: dict, hurst: dict,
    ) -> str:
        """Synthesise all directional signals into a confluence table
        and flag any significant divergences between indicators."""
        lines = ["## 10. Signal Confluence & Divergence Analysis"]

        # ── Collect individual signal votes ──
        signals: list[tuple[str, str, str]] = []  # (source, bias, value)

        # HMM regime
        if hmm.get("available"):
            lbl = hmm.get("current_regime_label", "")
            if "bull" in lbl.lower():
                signals.append(("HMM Regime", "BULLISH", lbl))
            elif "bear" in lbl.lower():
                signals.append(("HMM Regime", "BEARISH", lbl))
            else:
                signals.append(("HMM Regime", "NEUTRAL", lbl))

        # RSI
        if rsi and rsi.get("available"):
            cur_rsi = rsi.get("current_rsi")
            if cur_rsi is not None:
                if cur_rsi >= 70:
                    signals.append(("RSI (14)", "BEARISH", f"{cur_rsi:.1f} (overbought)"))
                elif cur_rsi <= 30:
                    signals.append(("RSI (14)", "BULLISH", f"{cur_rsi:.1f} (oversold)"))
                elif cur_rsi >= 50:
                    signals.append(("RSI (14)", "BULLISH", f"{cur_rsi:.1f}"))
                else:
                    signals.append(("RSI (14)", "BEARISH", f"{cur_rsi:.1f}"))

        # MACD crossover
        if macd and macd.get("available"):
            xo = macd.get("crossover", "").upper()
            if "BULLISH" in xo:
                signals.append(("MACD Crossover", "BULLISH", xo))
            elif "BEARISH" in xo:
                signals.append(("MACD Crossover", "BEARISH", xo))
            else:
                signals.append(("MACD Crossover", "NEUTRAL", xo or "N/A"))

        # ADX / DMI directional bias
        if adx.get("available"):
            bias = adx.get("directional_bias", "").upper()
            adx_val = adx.get("current_adx", 0)
            strength = adx.get("trend_strength", "")
            if "BULLISH" in bias:
                signals.append(("DMI Directional", "BULLISH",
                                f"+DI > -DI | ADX {adx_val:.1f} ({strength})"))
            elif "BEARISH" in bias:
                signals.append(("DMI Directional", "BEARISH",
                                f"-DI > +DI | ADX {adx_val:.1f} ({strength})"))
            else:
                signals.append(("DMI Directional", "NEUTRAL",
                                f"ADX {adx_val:.1f} ({strength})"))

        # Supertrend
        if st.get("available"):
            direction = st.get("current_direction", "").upper()
            st_val = st.get("current_supertrend", 0)
            if "BULLISH" in direction:
                signals.append(("Supertrend", "BULLISH", f"₹{st_val:,.2f}"))
            elif "BEARISH" in direction:
                signals.append(("Supertrend", "BEARISH", f"₹{st_val:,.2f}"))

        # Mansfield RS
        if mrs.get("available"):
            interp = mrs.get("interpretation", "").upper()
            mrs_val = mrs.get("current_mrs", 0)
            if "OUTPERFORMING" in interp:
                signals.append(("Mansfield RS", "BULLISH",
                                f"{mrs_val:.2f} ({interp})"))
            elif "UNDERPERFORMING" in interp:
                signals.append(("Mansfield RS", "BEARISH",
                                f"{mrs_val:.2f} ({interp})"))

        if not signals:
            lines.append("\n*Insufficient indicator data for confluence analysis.*")
            return "\n".join(lines)

        # ── Build confluence table ──
        lines += [
            "",
            "### Indicator Signal Table",
            "| Indicator | Bias | Detail |",
            "| :--- | :---: | :--- |",
        ]
        bull_count = 0
        bear_count = 0
        for src, bias, detail in signals:
            emoji = "🟢" if bias == "BULLISH" else ("🔴" if bias == "BEARISH" else "⚪")
            lines.append(f"| {src} | {emoji} **{bias}** | {detail} |")
            if bias == "BULLISH":
                bull_count += 1
            elif bias == "BEARISH":
                bear_count += 1

        total = len(signals)
        lines += [
            "",
            "### Confluence Summary",
            "| Metric | Value |",
            "| :--- | ---: |",
            f"| Bullish Signals | {bull_count} / {total} |",
            f"| Bearish Signals | {bear_count} / {total} |",
            f"| Neutral Signals | {total - bull_count - bear_count} / {total} |",
        ]

        # Net bias
        if bull_count > bear_count + 1:
            net = "NET BULLISH"
        elif bear_count > bull_count + 1:
            net = "NET BEARISH"
        elif bull_count == bear_count:
            net = "MIXED / NO CLEAR BIAS"
        else:
            net = "MARGINALLY " + ("BULLISH" if bull_count > bear_count else "BEARISH")
        lines.append(f"| **Overall Bias** | **{net}** |")

        # ── Divergence warnings ──
        divergences: list[str] = []

        # Check Supertrend vs majority
        st_sig = next((b for s, b, _ in signals if s == "Supertrend"), None)
        majority = "BULLISH" if bull_count > bear_count else (
            "BEARISH" if bear_count > bull_count else None)
        if st_sig and majority and st_sig != majority:
            others = ", ".join(
                f"{s} ({b})" for s, b, _ in signals
                if s != "Supertrend" and b == majority
            )
            divergences.append(
                f"**Supertrend ({st_sig})** diverges from the majority bias "
                f"({majority}): {others}. The Supertrend may be reacting to a "
                f"short-term bounce within a larger downtrend — wait for "
                f"confirmation before acting on the Supertrend alone."
            )

        # Check HMM vs Supertrend specifically
        hmm_sig = next((b for s, b, _ in signals if s == "HMM Regime"), None)
        if hmm_sig and st_sig and hmm_sig != st_sig and hmm_sig != "NEUTRAL":
            divergences.append(
                f"**HMM Regime ({hmm_sig})** conflicts with **Supertrend ({st_sig})**. "
                f"The HMM captures the broader statistical regime while Supertrend "
                f"tracks short-term ATR-based trend — regime context should take "
                f"precedence for position sizing."
            )

        # Hurst implication
        if hurst.get("available"):
            h_val = hurst.get("hurst", 0.5)
            interp = hurst.get("interpretation", "")
            if h_val and h_val > 0.55:
                divergences.append(
                    f"Hurst exponent ({h_val:.4f}) indicates **{interp}** — "
                    f"the current directional trend is statistically likely to "
                    f"persist. Signals aligned with the dominant regime carry "
                    f"higher conviction."
                )
            elif h_val and h_val < 0.45:
                divergences.append(
                    f"Hurst exponent ({h_val:.4f}) indicates **{interp}** — "
                    f"the price is statistically likely to revert. Breakout "
                    f"signals (Supertrend, MACD) may generate more false positives."
                )

        if divergences:
            lines += ["", "### Divergence Alerts"]
            for i, d in enumerate(divergences, 1):
                lines.append(f"\n{i}. {d}")
        else:
            lines += ["", "*No significant signal divergences detected — indicators are aligned.*"]

        return "\n".join(lines)

    # ── Charts ──
    def _charts_section(self, chart_paths: dict[str, str]) -> str:
        if not chart_paths:
            return "## 11. Charts\n\n*No charts generated.*"

        lines = ["## 11. Charts", ""]
        chart_labels = {
            "candlestick": "Candlestick with S/R & VWAP",
            "volume_profile": "Volume Profile (VPVR)",
            "momentum": "Momentum Dashboard (RSI / MACD / Bollinger %B)",
            "risk_panel": "Risk & Volatility Panel",
            "summary": "Summary Dashboard",
            "acf_pacf": "ACF / PACF Correlogram",
            "rolling_beta": "Rolling Beta & Correlation vs Nifty 50",
            "mansfield_rs": "Mansfield Relative Strength",
            "adx_dmi": "ADX & Directional Movement Index",
            "supertrend": "Supertrend Overlay",
            "monthly_heatmap": "Monthly Return Heatmap",
            "day_of_week": "Day-of-Week Effect",
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
