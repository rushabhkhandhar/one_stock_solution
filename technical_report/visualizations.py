"""
Visualization Engine — Plotly (high-fidelity static PNGs)
============================================================
Generates institutional-grade charts exported as static PNG via kaleido.

Charts produced:
  1. **Candlestick Overview** — OHLCV + DMAs + S/R price labels + VWAP
  2. **Volume Profile (VPVR)** — horizontal volume histogram alongside price
  3. **Momentum Dashboard** — RSI, MACD histogram, Bollinger %B (stacked)
  4. **Risk / Volatility Panel** — drawdown, rolling Sharpe, vol regime
  5. **Summary Dashboard** — 2x2 composite

All charts are saved as PNG to ``output/{symbol}/plots/``.
"""
from __future__ import annotations

import os
import warnings
from typing import Optional

import numpy as np
import pandas as pd

import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore", category=UserWarning)

# ---------- Export settings ----------
_WIDTH = 1600
_HEIGHT = 1000
_SCALE = 2           # 2x for retina-grade PNGs

# ---------- colour palette (dark institutional) ----------
_P = {
    "bg":         "#0e1117",
    "paper":      "#0e1117",
    "panel":      "#1a1d23",
    "text":       "#d0d0d0",
    "grid":       "#2a2d35",
    "green":      "#26a69a",
    "red":        "#ef5350",
    "blue":       "#42a5f5",
    "orange":     "#ffa726",
    "purple":     "#ab47bc",
    "yellow":     "#ffee58",
    "cyan":       "#26c6da",
    "grey":       "#78909c",
    "white":      "#ffffff",
    "support":    "#26a69a",
    "resistance": "#ef5350",
    "vwap":       "#ffa726",
    "poc":        "#ab47bc",
}

# ---------- shared layout helpers ----------

def _dark_layout(**overrides) -> dict:
    base = dict(
        template="plotly_dark",
        paper_bgcolor=_P["bg"],
        plot_bgcolor=_P["panel"],
        font=dict(family="Arial, sans-serif", size=12, color=_P["text"]),
        margin=dict(l=60, r=120, t=50, b=60),
        xaxis=dict(
            gridcolor=_P["grid"], gridwidth=0.3,
            showline=True, linecolor=_P["grid"],
            tickformat="%b '%y",
        ),
        yaxis=dict(
            gridcolor=_P["grid"], gridwidth=0.3,
            showline=True, linecolor=_P["grid"],
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0.5)", bordercolor=_P["grid"],
            font=dict(size=10, color=_P["text"]),
        ),
    )
    base.update(overrides)
    return base


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


class TechnicalVisualizer:
    """Generates all technical analysis charts using Plotly."""

    def __init__(self, symbol: str, output_dir: str = "./output"):
        self.symbol = symbol.upper()
        self.plot_dir = os.path.join(output_dir, self.symbol, "plots")
        _ensure_dir(self.plot_dir)

    def _save(self, fig: go.Figure, name: str,
              width: int = _WIDTH, height: int = _HEIGHT) -> str:
        path = os.path.join(self.plot_dir, f"{self.symbol}_{name}.png")
        fig.write_image(path, width=width, height=height, scale=_SCALE)
        return path

    # ==================================================================
    # 1. Candlestick  (OHLCV + DMAs + S/R labels + VWAP)
    # ==================================================================
    def candlestick_chart(
        self,
        df: pd.DataFrame,
        support_levels: list[float] | None = None,
        resistance_levels: list[float] | None = None,
        vwap_series: pd.Series | None = None,
        title_suffix: str = "",
        lookback: int = 120,
        support_labels: list[str] | None = None,
        resist_labels: list[str] | None = None,
    ) -> str:
        chart_df = df.tail(lookback).copy()

        # Normalise column names
        col_map = {"open": "Open", "high": "High", "low": "Low",
                    "close": "Close", "volume": "Volume"}
        chart_df = chart_df.rename(columns=col_map)
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c not in chart_df.columns:
                return ""
        if not isinstance(chart_df.index, pd.DatetimeIndex):
            chart_df.index = pd.to_datetime(chart_df.index)

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.78, 0.22], vertical_spacing=0.02,
        )

        # --- Candles ---
        fig.add_trace(go.Candlestick(
            x=chart_df.index, open=chart_df["Open"], high=chart_df["High"],
            low=chart_df["Low"], close=chart_df["Close"],
            increasing_line_color=_P["green"], decreasing_line_color=_P["red"],
            increasing_fillcolor=_P["green"], decreasing_fillcolor=_P["red"],
            name="OHLC", showlegend=False,
        ), row=1, col=1)

        # --- 50 & 200 DMA ---
        if len(chart_df) >= 50:
            dma50 = chart_df["Close"].rolling(50).mean()
            fig.add_trace(go.Scatter(
                x=chart_df.index, y=dma50, mode="lines",
                line=dict(color=_P["blue"], width=1.3, dash="dash"),
                name="DMA 50",
            ), row=1, col=1)
        if len(chart_df) >= 200:
            dma200 = chart_df["Close"].rolling(200).mean()
            fig.add_trace(go.Scatter(
                x=chart_df.index, y=dma200, mode="lines",
                line=dict(color=_P["orange"], width=1.3),
                name="DMA 200",
            ), row=1, col=1)

        # --- VWAP ---
        if vwap_series is not None:
            vw = vwap_series.reindex(chart_df.index)
            if vw.notna().sum() > 10:
                fig.add_trace(go.Scatter(
                    x=vw.index, y=vw, mode="lines",
                    line=dict(color=_P["vwap"], width=1.4, dash="dashdot"),
                    name="VWAP",
                ), row=1, col=1)

        # --- S/R horizontal lines with clear price labels ---
        s_labels = support_labels or []
        r_labels = resist_labels or []

        if support_levels:
            for i, lvl in enumerate(support_levels[:6]):
                src = s_labels[i] if i < len(s_labels) else ""
                tag = f"S  {lvl:,.0f}" + (f"  ({src})" if src else "")
                fig.add_hline(
                    y=lvl, line_color=_P["support"], line_width=1,
                    line_dash="dot", row=1, col=1,
                    annotation_text=tag,
                    annotation_position="right",
                    annotation_font=dict(size=11, color=_P["support"]),
                    annotation_bgcolor="rgba(14,17,23,0.85)",
                    annotation_bordercolor=_P["support"],
                    annotation_borderwidth=1,
                )
        if resistance_levels:
            for i, lvl in enumerate(resistance_levels[:6]):
                src = r_labels[i] if i < len(r_labels) else ""
                tag = f"R  {lvl:,.0f}" + (f"  ({src})" if src else "")
                fig.add_hline(
                    y=lvl, line_color=_P["resistance"], line_width=1,
                    line_dash="dot", row=1, col=1,
                    annotation_text=tag,
                    annotation_position="right",
                    annotation_font=dict(size=11, color=_P["resistance"]),
                    annotation_bgcolor="rgba(14,17,23,0.85)",
                    annotation_bordercolor=_P["resistance"],
                    annotation_borderwidth=1,
                )

        # --- Volume bars ---
        vol_colors = [_P["green"] if chart_df["Close"].iloc[j] >= chart_df["Open"].iloc[j]
                      else _P["red"] for j in range(len(chart_df))]
        fig.add_trace(go.Bar(
            x=chart_df.index, y=chart_df["Volume"],
            marker_color=vol_colors, opacity=0.55,
            name="Volume", showlegend=False,
        ), row=2, col=1)

        title = f"{self.symbol} — Candlestick Chart"
        if title_suffix:
            title += f" | {title_suffix}"

        fig.update_layout(
            **_dark_layout(title=dict(text=title, x=0.5, font=dict(size=15))),
            xaxis_rangeslider_visible=False,
            height=_HEIGHT,
        )
        fig.update_yaxes(title_text="Price", row=1, col=1,
                         tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"])
        fig.update_yaxes(title_text="Volume", row=2, col=1,
                         tickprefix="", tickformat=".2s",
                         gridcolor=_P["grid"])
        fig.update_xaxes(gridcolor=_P["grid"], row=1, col=1)
        fig.update_xaxes(gridcolor=_P["grid"], row=2, col=1,
                         tickformat="%b '%y")

        return self._save(fig, "candlestick")

    # ==================================================================
    # 2. Volume Profile (VPVR)
    # ==================================================================
    def volume_profile_chart(
        self,
        df: pd.DataFrame,
        vpvr_data: dict,
        lookback: int = 120,
    ) -> str:
        if not vpvr_data.get("available"):
            return ""

        chart_df = df.tail(lookback)
        close = chart_df["close"] if "close" in chart_df.columns else chart_df.get("Close")
        if close is None:
            return ""

        fig = make_subplots(
            rows=1, cols=2, column_widths=[0.72, 0.28],
            shared_yaxes=True, horizontal_spacing=0.01,
        )

        # Price line
        fig.add_trace(go.Scatter(
            x=close.index, y=close.values, mode="lines",
            line=dict(color=_P["blue"], width=1.4), name="Close",
        ), row=1, col=1)

        poc = vpvr_data["poc_price"]
        va_h = vpvr_data["value_area_high"]
        va_l = vpvr_data["value_area_low"]

        # Value Area shading
        fig.add_hrect(y0=va_l, y1=va_h, fillcolor=_P["blue"],
                      opacity=0.08, line_width=0, row=1, col=1)
        fig.add_hline(y=poc, line_color=_P["poc"], line_width=1.5,
                      line_dash="solid", row=1, col=1,
                      annotation_text=f"POC  {poc:,.0f}",
                      annotation_position="top left",
                      annotation_font=dict(size=11, color=_P["poc"]))

        # Horizontal volume bars
        profile = vpvr_data["profile"]
        y_pos = [p["price_mid"] for p in profile]
        vols = [p["volume"] for p in profile]
        bar_colors = [_P["green"] if p["price_mid"] <= poc else _P["red"] for p in profile]
        bar_h = (max(y_pos) - min(y_pos)) / len(y_pos) * 0.85 if len(y_pos) > 1 else 1

        fig.add_trace(go.Bar(
            y=y_pos, x=vols, orientation="h",
            marker_color=bar_colors, opacity=0.7,
            name="VPVR", width=bar_h, showlegend=False,
        ), row=1, col=2)

        fig.add_hline(y=poc, line_color=_P["poc"], line_width=1.5,
                      line_dash="solid", row=1, col=2)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Volume Profile (VPVR)", x=0.5,
                           font=dict(size=15)),
                height=_HEIGHT,
            ),
        )
        fig.update_yaxes(title_text="Price", tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"], row=1, col=1)
        fig.update_yaxes(gridcolor=_P["grid"], row=1, col=2)
        fig.update_xaxes(title_text="", tickformat="%b '%y",
                         gridcolor=_P["grid"], row=1, col=1)
        fig.update_xaxes(title_text="Volume", tickformat=".2s",
                         gridcolor=_P["grid"], row=1, col=2, autorange="reversed")

        return self._save(fig, "volume_profile")

    # ==================================================================
    # 3. Momentum Dashboard (RSI / MACD / Bollinger %B)
    # ==================================================================
    def momentum_dashboard(
        self,
        close: pd.Series,
        rsi_data: dict,
        macd_data: dict,
        boll_data: dict,
        lookback: int = 120,
    ) -> str:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            row_heights=[0.35, 0.35, 0.30],
            vertical_spacing=0.06,
            subplot_titles=["RSI (14)", "MACD (12, 26, 9)", "Bollinger %B"],
        )

        recent = slice(-lookback, None)

        # ---- Panel 1: RSI ----
        if rsi_data.get("available"):
            rsi_s = rsi_data["_rsi_series"].iloc[recent]
            fig.add_trace(go.Scatter(
                x=rsi_s.index, y=rsi_s.values, mode="lines",
                line=dict(color=_P["cyan"], width=1.5), name="RSI",
                showlegend=False,
            ), row=1, col=1)
            # Reference bands
            fig.add_hline(y=70, line_color=_P["red"], line_width=0.7,
                          line_dash="dash", row=1, col=1,
                          annotation_text="Overbought (70)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["red"]))
            fig.add_hline(y=30, line_color=_P["green"], line_width=0.7,
                          line_dash="dash", row=1, col=1,
                          annotation_text="Oversold (30)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["green"]))
            fig.add_hrect(y0=70, y1=100, fillcolor=_P["red"],
                          opacity=0.06, line_width=0, row=1, col=1)
            fig.add_hrect(y0=0, y1=30, fillcolor=_P["green"],
                          opacity=0.06, line_width=0, row=1, col=1)
            # Latest value annotation
            last_rsi = rsi_s.iloc[-1]
            fig.add_annotation(
                x=rsi_s.index[-1], y=last_rsi,
                text=f"  {last_rsi:.1f}",
                showarrow=False, font=dict(size=13, color=_P["cyan"],
                                           family="Arial Black"),
                xanchor="left", row=1, col=1,
            )

        # ---- Panel 2: MACD ----
        if macd_data.get("available"):
            macd_l = macd_data["_macd_line"].iloc[recent]
            sig_l = macd_data["_signal_line"].iloc[recent]
            hist = macd_data["_histogram"].iloc[recent]

            fig.add_trace(go.Scatter(
                x=macd_l.index, y=macd_l.values, mode="lines",
                line=dict(color=_P["blue"], width=1.3), name="MACD",
            ), row=2, col=1)
            fig.add_trace(go.Scatter(
                x=sig_l.index, y=sig_l.values, mode="lines",
                line=dict(color=_P["orange"], width=1.3), name="Signal",
            ), row=2, col=1)

            hist_colors = [_P["green"] if v >= 0 else _P["red"] for v in hist.values]
            fig.add_trace(go.Bar(
                x=hist.index, y=hist.values,
                marker_color=hist_colors, opacity=0.5,
                name="Histogram", showlegend=False,
            ), row=2, col=1)
            fig.add_hline(y=0, line_color=_P["grey"], line_width=0.5, row=2, col=1)

        # ---- Panel 3: Bollinger %B ----
        if boll_data.get("available"):
            pct_b = boll_data["_pct_b"].iloc[recent]
            fig.add_trace(go.Scatter(
                x=pct_b.index, y=pct_b.values, mode="lines",
                line=dict(color=_P["purple"], width=1.3), name="%B",
                showlegend=False,
            ), row=3, col=1)
            fig.add_hline(y=1.0, line_color=_P["red"], line_width=0.7,
                          line_dash="dash", row=3, col=1,
                          annotation_text="Upper Band (1.0)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["red"]))
            fig.add_hline(y=0.0, line_color=_P["green"], line_width=0.7,
                          line_dash="dash", row=3, col=1,
                          annotation_text="Lower Band (0.0)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["green"]))
            fig.add_hline(y=0.5, line_color=_P["grey"], line_width=0.4,
                          line_dash="dot", row=3, col=1)
            # Last value
            last_b = pct_b.iloc[-1]
            fig.add_annotation(
                x=pct_b.index[-1], y=last_b,
                text=f"  {last_b:.2f}",
                showarrow=False, font=dict(size=12, color=_P["purple"],
                                           family="Arial Black"),
                xanchor="left", row=3, col=1,
            )

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Momentum Dashboard", x=0.5,
                           font=dict(size=15)),
                height=1100,
            ),
        )
        fig.update_yaxes(title_text="RSI", range=[0, 100],
                         gridcolor=_P["grid"], row=1, col=1,
                         tickprefix="", tickformat=".0f")
        fig.update_yaxes(title_text="MACD",
                         gridcolor=_P["grid"], row=2, col=1,
                         tickprefix="", tickformat=".1f")
        fig.update_yaxes(title_text="%B",
                         gridcolor=_P["grid"], row=3, col=1,
                         tickprefix="", tickformat=".2f")
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"],
                         row=3, col=1)

        return self._save(fig, "momentum_dashboard", height=1100)

    # ==================================================================
    # 4. Risk & Volatility Panel
    # ==================================================================
    def risk_panel(
        self,
        close: pd.Series,
        risk_metrics: dict,
        vol_model: dict,
        lookback: int = 252,
    ) -> str:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            row_heights=[0.35, 0.32, 0.33],
            vertical_spacing=0.06,
            subplot_titles=["Drawdown", "Rolling Sharpe (63d)",
                            "Annualised Volatility"],
        )
        recent = slice(-lookback, None)

        # ---- Panel 1: Drawdown ----
        dd = risk_metrics.get("drawdown", {})
        if "_dd_series" in dd:
            dd_s = dd["_dd_series"].iloc[recent] * 100
            fig.add_trace(go.Scatter(
                x=dd_s.index, y=dd_s.values, fill="tozeroy",
                fillcolor="rgba(239,83,80,0.25)",
                line=dict(color=_P["red"], width=1), name="Drawdown",
                showlegend=False,
            ), row=1, col=1)
            # Max drawdown annotation
            worst_idx = dd_s.idxmin()
            worst_val = dd_s.min()
            fig.add_annotation(
                x=worst_idx, y=worst_val,
                text=f"Max DD: {worst_val:.1f}%",
                showarrow=True, arrowhead=2, arrowcolor=_P["red"],
                font=dict(size=11, color=_P["red"]),
                bgcolor="rgba(14,17,23,0.85)",
                bordercolor=_P["red"],
                row=1, col=1,
            )

        # ---- Panel 2: Rolling Sharpe ----
        rolling = risk_metrics.get("rolling", {})
        if "_rolling_sharpe" in rolling:
            rs = rolling["_rolling_sharpe"].iloc[recent]
            fig.add_trace(go.Scatter(
                x=rs.index, y=rs.values, mode="lines",
                line=dict(color=_P["cyan"], width=1.3), name="Sharpe 63d",
                showlegend=False,
            ), row=2, col=1)
            fig.add_hline(y=0, line_color=_P["grey"], line_width=0.5, row=2, col=1)
            fig.add_hline(y=1, line_color=_P["green"], line_width=0.5,
                          line_dash="dash", row=2, col=1,
                          annotation_text="Good (1.0)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["green"]))
            fig.add_hline(y=-1, line_color=_P["red"], line_width=0.5,
                          line_dash="dash", row=2, col=1,
                          annotation_text="Poor (-1.0)",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["red"]))

        # ---- Panel 3: Rolling Volatility ----
        rv = vol_model.get("rolling_vol", {})
        rv_windows = rv.get("windows", {})
        for key, color, label in [("21d", _P["orange"], "21d Vol"),
                                  ("63d", _P["blue"], "63d Vol")]:
            rv_data = rv_windows.get(key, {})
            if "_series" in rv_data:
                s = rv_data["_series"].iloc[recent]
                fig.add_trace(go.Scatter(
                    x=s.index, y=s.values, mode="lines",
                    line=dict(color=color, width=1.2), name=label,
                ), row=3, col=1)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Risk & Volatility Panel",
                           x=0.5, font=dict(size=15)),
                height=1100,
            ),
        )
        fig.update_yaxes(title_text="Drawdown (%)",
                         gridcolor=_P["grid"], row=1, col=1,
                         tickprefix="", tickformat=".1f")
        fig.update_yaxes(title_text="Sharpe",
                         gridcolor=_P["grid"], row=2, col=1,
                         tickprefix="", tickformat=".2f")
        fig.update_yaxes(title_text="Ann. Vol (%)",
                         gridcolor=_P["grid"], row=3, col=1,
                         tickprefix="", tickformat=".1f")
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"],
                         row=3, col=1)

        return self._save(fig, "risk_panel", height=1100)

    # ==================================================================
    # 5. Summary Dashboard (2x2 composite)
    # ==================================================================
    def summary_dashboard(
        self,
        df: pd.DataFrame,
        risk_metrics: dict,
        vol_model: dict,
        vpvr_data: dict,
        price_levels: dict,
        lookback: int = 120,
    ) -> str:
        chart_df = df.tail(lookback)
        close = chart_df["close"] if "close" in chart_df.columns else chart_df.get("Close")
        if close is None:
            return ""

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Price + S/R Levels", "Volume Profile (mini)",
                "Drawdown", "Rolling Volatility (21d)",
            ],
            vertical_spacing=0.10, horizontal_spacing=0.08,
        )

        # ---- TL: Price with S/R ----
        fig.add_trace(go.Scatter(
            x=close.index, y=close.values, mode="lines",
            line=dict(color=_P["blue"], width=1.4), name="Close",
            showlegend=False,
        ), row=1, col=1)

        merged = price_levels.get("merged", {})
        if "levels" in merged:
            for lvl in merged["levels"][:10]:
                p = lvl["price"]
                is_sup = lvl.get("type", "").upper() == "SUPPORT"
                c = _P["support"] if is_sup else _P["resistance"]
                tag = ("S" if is_sup else "R") + f"  {p:,.0f}"
                fig.add_hline(
                    y=p, line_color=c, line_width=0.8, line_dash="dot",
                    row=1, col=1,
                    annotation_text=tag,
                    annotation_position="right",
                    annotation_font=dict(size=9, color=c),
                    annotation_bgcolor="rgba(14,17,23,0.8)",
                    annotation_bordercolor=c,
                )

        # ---- TR: Volume Profile (mini) ----
        if vpvr_data.get("available"):
            profile = vpvr_data["profile"]
            y_pos = [p["price_mid"] for p in profile]
            vols = [p["volume"] for p in profile]
            cols = [_P["green"] if p["price_mid"] <= vpvr_data["poc_price"]
                    else _P["red"] for p in profile]
            bar_h = (max(y_pos) - min(y_pos)) / len(y_pos) * 0.85 if len(y_pos) > 1 else 1
            fig.add_trace(go.Bar(
                y=y_pos, x=vols, orientation="h",
                marker_color=cols, opacity=0.7,
                name="VPVR", width=bar_h, showlegend=False,
            ), row=1, col=2)
            fig.add_hline(y=vpvr_data["poc_price"], line_color=_P["poc"],
                          line_width=1.2, row=1, col=2,
                          annotation_text=f"POC  {vpvr_data['poc_price']:,.0f}",
                          annotation_font=dict(size=9, color=_P["poc"]))

        # ---- BL: Drawdown ----
        dd = risk_metrics.get("drawdown", {})
        if "_dd_series" in dd:
            dd_s = dd["_dd_series"].tail(lookback) * 100
            fig.add_trace(go.Scatter(
                x=dd_s.index, y=dd_s.values, fill="tozeroy",
                fillcolor="rgba(239,83,80,0.3)",
                line=dict(color=_P["red"], width=0.8),
                name="DD", showlegend=False,
            ), row=2, col=1)

        # ---- BR: Rolling Volatility ----
        rv = vol_model.get("rolling_vol", {})
        rv_windows = rv.get("windows", {})
        rv21_data = rv_windows.get("21d", {})
        if "_series" in rv21_data:
            rv21 = rv21_data["_series"].tail(lookback)
            fig.add_trace(go.Scatter(
                x=rv21.index, y=rv21.values, mode="lines",
                line=dict(color=_P["orange"], width=1.2),
                name="21d Vol", showlegend=False,
            ), row=2, col=2)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Summary Dashboard",
                           x=0.5, font=dict(size=15)),
                height=1100,
            ),
        )
        fig.update_yaxes(tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"], row=1, col=1)
        fig.update_yaxes(tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"], row=1, col=2)
        fig.update_yaxes(title_text="DD %", tickprefix="",
                         gridcolor=_P["grid"], row=2, col=1)
        fig.update_yaxes(title_text="Vol %", tickprefix="",
                         gridcolor=_P["grid"], row=2, col=2)
        for r, c in [(1, 1), (2, 1), (2, 2)]:
            fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"],
                             row=r, col=c)
        fig.update_xaxes(gridcolor=_P["grid"], row=1, col=2)

        return self._save(fig, "summary_dashboard", height=1100)

    # ==================================================================
    # 6. ACF / PACF Correlogram
    # ==================================================================
    def acf_pacf_chart(self, acf_data: dict) -> str:
        if not acf_data.get("available"):
            return ""

        acf_vals = acf_data.get("_acf")
        pacf_vals = acf_data.get("_pacf")
        if acf_vals is None or pacf_vals is None:
            return ""

        n_lags = len(acf_vals) - 1  # skip lag-0
        lags = list(range(1, n_lags + 1))
        acf_v = list(acf_vals[1:])
        pacf_v = list(pacf_vals[1:])

        # Approximate 95 % confidence band
        n_obs = acf_data.get("_n_obs", 250)
        ci = 1.96 / (n_obs ** 0.5)

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.10,
            subplot_titles=["Autocorrelation (ACF)", "Partial Autocorrelation (PACF)"],
        )

        for row, vals, label in [(1, acf_v, "ACF"), (2, pacf_v, "PACF")]:
            colors = [_P["blue"] if abs(v) > ci else _P["grey"] for v in vals]
            fig.add_trace(go.Bar(
                x=lags, y=vals, marker_color=colors,
                name=label, showlegend=False,
            ), row=row, col=1)
            fig.add_hline(y=ci, line_color=_P["red"], line_dash="dash",
                          line_width=0.8, row=row, col=1,
                          annotation_text="95% CI",
                          annotation_position="right",
                          annotation_font=dict(size=9, color=_P["red"]))
            fig.add_hline(y=-ci, line_color=_P["red"], line_dash="dash",
                          line_width=0.8, row=row, col=1)
            fig.add_hline(y=0, line_color=_P["grey"], line_width=0.4,
                          row=row, col=1)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — ACF / PACF Correlogram",
                           x=0.5, font=dict(size=15)),
                height=800,
            ),
        )
        fig.update_xaxes(title_text="Lag", row=2, col=1, dtick=5,
                         gridcolor=_P["grid"])
        fig.update_yaxes(title_text="ACF", gridcolor=_P["grid"], row=1, col=1)
        fig.update_yaxes(title_text="PACF", gridcolor=_P["grid"], row=2, col=1)

        return self._save(fig, "acf_pacf", height=800)

    # ==================================================================
    # 7. Rolling Beta & Correlation vs Benchmark
    # ==================================================================
    def rolling_beta_chart(self, beta_data: dict) -> str:
        if not beta_data.get("available"):
            return ""

        beta_s = beta_data.get("_beta_series")
        corr_s = beta_data.get("_correlation_series")
        if beta_s is None or corr_s is None:
            return ""

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=["Rolling Beta vs Nifty 50", "Rolling Correlation"],
        )

        fig.add_trace(go.Scatter(
            x=beta_s.index, y=beta_s.values, mode="lines",
            line=dict(color=_P["blue"], width=1.4), name="Beta",
            showlegend=False,
        ), row=1, col=1)
        fig.add_hline(y=1.0, line_color=_P["grey"], line_dash="dash",
                      line_width=0.7, row=1, col=1,
                      annotation_text="β = 1.0",
                      annotation_position="right",
                      annotation_font=dict(size=9, color=_P["grey"]))
        last_beta = beta_s.dropna().iloc[-1] if len(beta_s.dropna()) else None
        if last_beta is not None:
            fig.add_annotation(
                x=beta_s.dropna().index[-1], y=last_beta,
                text=f"  β = {last_beta:.2f}",
                showarrow=False, font=dict(size=12, color=_P["blue"],
                                           family="Arial Black"),
                xanchor="left", row=1, col=1)

        fig.add_trace(go.Scatter(
            x=corr_s.index, y=corr_s.values, mode="lines",
            line=dict(color=_P["cyan"], width=1.4), name="Correlation",
            showlegend=False,
        ), row=2, col=1)
        fig.add_hline(y=0, line_color=_P["grey"], line_width=0.4, row=2, col=1)
        last_corr = corr_s.dropna().iloc[-1] if len(corr_s.dropna()) else None
        if last_corr is not None:
            fig.add_annotation(
                x=corr_s.dropna().index[-1], y=last_corr,
                text=f"  r = {last_corr:.2f}",
                showarrow=False, font=dict(size=12, color=_P["cyan"],
                                           family="Arial Black"),
                xanchor="left", row=2, col=1)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Rolling Beta & Correlation vs Nifty 50",
                           x=0.5, font=dict(size=15)),
                height=800,
            ),
        )
        fig.update_yaxes(title_text="Beta", gridcolor=_P["grid"], row=1, col=1)
        fig.update_yaxes(title_text="Correlation", gridcolor=_P["grid"],
                         range=[-1, 1], row=2, col=1)
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"], row=2, col=1)

        return self._save(fig, "rolling_beta", height=800)

    # ==================================================================
    # 8. Mansfield Relative Strength
    # ==================================================================
    def mansfield_rs_chart(self, mrs_data: dict, close: pd.Series) -> str:
        if not mrs_data.get("available"):
            return ""

        mrs_s = mrs_data.get("_mrs_series")
        if mrs_s is None:
            return ""

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.55, 0.45], vertical_spacing=0.06,
            subplot_titles=["Price", "Mansfield Relative Strength"],
        )

        # Price panel
        fig.add_trace(go.Scatter(
            x=close.index, y=close.values, mode="lines",
            line=dict(color=_P["blue"], width=1.3), name="Close",
            showlegend=False,
        ), row=1, col=1)

        # MRS panel
        fig.add_trace(go.Scatter(
            x=mrs_s.index, y=mrs_s.values, mode="lines",
            line=dict(color=_P["orange"], width=1.5), name="MRS",
            showlegend=False,
        ), row=2, col=1)
        fig.add_hline(y=0, line_color=_P["grey"], line_dash="dash",
                      line_width=0.7, row=2, col=1,
                      annotation_text="Zero line (benchmark)",
                      annotation_position="right",
                      annotation_font=dict(size=9, color=_P["grey"]))

        # Shade positive vs negative
        fig.add_trace(go.Scatter(
            x=mrs_s.index, y=mrs_s.clip(lower=0).values,
            fill="tozeroy", fillcolor="rgba(38,166,154,0.15)",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=mrs_s.index, y=mrs_s.clip(upper=0).values,
            fill="tozeroy", fillcolor="rgba(239,83,80,0.15)",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ), row=2, col=1)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Mansfield Relative Strength vs Nifty 50",
                           x=0.5, font=dict(size=15)),
                height=800,
            ),
        )
        fig.update_yaxes(title_text="Price", tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"], row=1, col=1)
        fig.update_yaxes(title_text="MRS", gridcolor=_P["grid"], row=2, col=1)
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"], row=2, col=1)

        return self._save(fig, "mansfield_rs", height=800)

    # ==================================================================
    # 9. ADX / DMI Panel
    # ==================================================================
    def adx_dmi_chart(self, adx_data: dict, lookback: int = 180) -> str:
        if not adx_data.get("available"):
            return ""

        adx_s = adx_data.get("_adx_series")
        pdi = adx_data.get("_plus_di_series")
        mdi = adx_data.get("_minus_di_series")
        if adx_s is None or pdi is None or mdi is None:
            return ""

        recent = slice(-lookback, None)
        adx_s = adx_s.iloc[recent]
        pdi = pdi.iloc[recent]
        mdi = mdi.iloc[recent]

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=adx_s.index, y=adx_s.values, mode="lines",
            line=dict(color=_P["yellow"], width=2), name="ADX",
        ))
        fig.add_trace(go.Scatter(
            x=pdi.index, y=pdi.values, mode="lines",
            line=dict(color=_P["green"], width=1.3), name="+DI",
        ))
        fig.add_trace(go.Scatter(
            x=mdi.index, y=mdi.values, mode="lines",
            line=dict(color=_P["red"], width=1.3), name="-DI",
        ))

        # Trend strength thresholds
        fig.add_hline(y=25, line_color=_P["grey"], line_dash="dash",
                      line_width=0.7,
                      annotation_text="Strong trend (25)",
                      annotation_position="right",
                      annotation_font=dict(size=9, color=_P["grey"]))
        fig.add_hline(y=40, line_color=_P["grey"], line_dash="dot",
                      line_width=0.5,
                      annotation_text="Very strong (40)",
                      annotation_position="right",
                      annotation_font=dict(size=9, color=_P["grey"]))

        # Last ADX annotation
        last_adx = adx_s.dropna().iloc[-1] if len(adx_s.dropna()) else None
        if last_adx is not None:
            fig.add_annotation(
                x=adx_s.dropna().index[-1], y=last_adx,
                text=f"  ADX: {last_adx:.1f}",
                showarrow=False,
                font=dict(size=12, color=_P["yellow"], family="Arial Black"),
                xanchor="left")

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — ADX & DMI",
                           x=0.5, font=dict(size=15)),
                height=600,
            ),
        )
        fig.update_yaxes(title_text="Value", gridcolor=_P["grid"],
                         range=[0, max(70, float(adx_s.max()) + 5)])
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"])

        return self._save(fig, "adx_dmi", height=600)

    # ==================================================================
    # 10. Supertrend Overlay
    # ==================================================================
    def supertrend_chart(
        self, df: pd.DataFrame, st_data: dict, lookback: int = 180
    ) -> str:
        if not st_data.get("available"):
            return ""

        st_series = st_data.get("_supertrend_series")
        dir_series = st_data.get("_direction_series")
        if st_series is None or dir_series is None:
            return ""

        chart_df = df.tail(lookback).copy()
        close = chart_df["close"] if "close" in chart_df.columns else chart_df.get("Close")
        if close is None:
            return ""

        st_s = st_series.reindex(chart_df.index).dropna()
        dir_s = dir_series.reindex(chart_df.index).dropna()

        fig = go.Figure()

        # Price line
        fig.add_trace(go.Scatter(
            x=close.index, y=close.values, mode="lines",
            line=dict(color=_P["blue"], width=1.5), name="Close",
        ))

        # Supertrend — green when bullish, red when bearish
        bullish_mask = dir_s == 1
        bearish_mask = dir_s == -1

        common_idx = st_s.index.intersection(dir_s.index)
        st_aligned = st_s.loc[common_idx]
        dir_aligned = dir_s.loc[common_idx]

        bull_st = st_aligned.where(dir_aligned == 1)
        bear_st = st_aligned.where(dir_aligned == -1)

        fig.add_trace(go.Scatter(
            x=bull_st.index, y=bull_st.values, mode="lines",
            line=dict(color=_P["green"], width=1.8),
            name="Supertrend (Bull)", connectgaps=False,
        ))
        fig.add_trace(go.Scatter(
            x=bear_st.index, y=bear_st.values, mode="lines",
            line=dict(color=_P["red"], width=1.8),
            name="Supertrend (Bear)", connectgaps=False,
        ))

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Supertrend Overlay",
                           x=0.5, font=dict(size=15)),
                height=700,
            ),
        )
        fig.update_yaxes(title_text="Price", tickprefix="₹", tickformat=",.0f",
                         gridcolor=_P["grid"])
        fig.update_xaxes(tickformat="%b '%y", gridcolor=_P["grid"])

        return self._save(fig, "supertrend", height=700)

    # ==================================================================
    # 11. Monthly Return Heatmap
    # ==================================================================
    def monthly_heatmap_chart(self, season_data: dict) -> str:
        if not season_data.get("available"):
            return ""

        heatmap_df = season_data.get("_heatmap_df")
        if heatmap_df is None or heatmap_df.empty:
            return ""

        # Fill NaN with 0 for display
        hm = heatmap_df.fillna(float("nan"))

        # Colour scale: red for negative, green for positive
        fig = go.Figure(data=go.Heatmap(
            z=hm.values,
            x=hm.columns.tolist(),
            y=[str(y) for y in hm.index.tolist()],
            colorscale=[
                [0.0, "#ef5350"],
                [0.5, "#1a1d23"],
                [1.0, "#26a69a"],
            ],
            zmid=0,
            text=[[f"{v:.1f}%" if not np.isnan(v) else ""
                   for v in row] for row in hm.values],
            texttemplate="%{text}",
            textfont=dict(size=11, color=_P["text"]),
            hovertemplate="Year: %{y}<br>Month: %{x}<br>Return: %{z:.2f}%<extra></extra>",
            colorbar=dict(
                title="Return %",
                ticksuffix="%",
                len=0.75,
            ),
        ))

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Monthly Return Heatmap",
                           x=0.5, font=dict(size=15)),
                height=max(400, 120 + len(hm) * 40),
            ),
        )
        fig.update_xaxes(side="top", tickangle=0)
        fig.update_yaxes(autorange="reversed")

        return self._save(fig, "monthly_heatmap",
                          height=max(400, 120 + len(hm) * 40))

    # ==================================================================
    # 12. Day-of-Week Effect
    # ==================================================================
    def day_of_week_chart(self, dow_data: dict) -> str:
        if not dow_data.get("available"):
            return ""

        grouped = dow_data.get("_grouped_df")
        if grouped is None or grouped.empty:
            return ""

        days = grouped.index.tolist()
        avgs = grouped["avg_return_pct"].values
        stds = grouped["std_pct"].values

        colors = [_P["green"] if v >= 0 else _P["red"] for v in avgs]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=days, y=avgs,
            marker_color=colors, opacity=0.85,
            error_y=dict(type="data", array=stds, visible=True,
                         color=_P["grey"], thickness=1.2),
            name="Avg Return",
        ))
        fig.add_hline(y=0, line_color=_P["grey"], line_width=0.5)

        fig.update_layout(
            **_dark_layout(
                title=dict(text=f"{self.symbol} — Day-of-Week Effect",
                           x=0.5, font=dict(size=15)),
                height=500,
            ),
        )
        fig.update_yaxes(title_text="Avg Daily Return (%)",
                         gridcolor=_P["grid"], ticksuffix="%")
        fig.update_xaxes(gridcolor=_P["grid"])

        return self._save(fig, "day_of_week", height=500)

    # ==================================================================
    # Generate ALL charts
    # ==================================================================
    def generate_all(
        self,
        df: pd.DataFrame,
        risk_metrics: dict,
        vol_model: dict,
        vpvr_data: dict,
        vwap_data: dict,
        price_levels: dict,
        rsi_data: dict,
        macd_data: dict,
        boll_data: dict,
        # ── new data dicts ──
        acf_data: dict | None = None,
        beta_data: dict | None = None,
        mrs_data: dict | None = None,
        adx_data: dict | None = None,
        supertrend_data: dict | None = None,
        monthly_data: dict | None = None,
        dow_data: dict | None = None,
    ) -> dict[str, str]:
        close = df["close"] if "close" in df.columns else df.get("Close")
        if close is None:
            return {}

        merged = price_levels.get("merged", {})
        all_levels = merged.get("levels", [])
        support_lvls = [l for l in all_levels if l.get("type", "").upper() == "SUPPORT"]
        resist_lvls = [l for l in all_levels if l.get("type", "").upper() == "RESISTANCE"]
        support_prices = [l["price"] for l in support_lvls[:6]]
        resist_prices = [l["price"] for l in resist_lvls[:6]]

        vwap_series = vwap_data.get("_vwap_series") if vwap_data.get("available") else None

        paths: dict[str, str] = {}

        for name, fn in [
            ("candlestick", lambda: self.candlestick_chart(
                df, support_prices, resist_prices, vwap_series,
                support_labels=[l.get("source", "") for l in support_lvls[:6]],
                resist_labels=[l.get("source", "") for l in resist_lvls[:6]],
            )),
            ("volume_profile", lambda: self.volume_profile_chart(df, vpvr_data)),
            ("momentum", lambda: self.momentum_dashboard(
                close, rsi_data, macd_data, boll_data)),
            ("risk_panel", lambda: self.risk_panel(close, risk_metrics, vol_model)),
            ("summary", lambda: self.summary_dashboard(
                df, risk_metrics, vol_model, vpvr_data, price_levels)),
            # ── NEW charts ──
            ("acf_pacf", lambda: self.acf_pacf_chart(acf_data or {})),
            ("rolling_beta", lambda: self.rolling_beta_chart(beta_data or {})),
            ("mansfield_rs", lambda: self.mansfield_rs_chart(mrs_data or {}, close)),
            ("adx_dmi", lambda: self.adx_dmi_chart(adx_data or {})),
            ("supertrend", lambda: self.supertrend_chart(df, supertrend_data or {})),
            ("monthly_heatmap", lambda: self.monthly_heatmap_chart(monthly_data or {})),
            ("day_of_week", lambda: self.day_of_week_chart(dow_data or {})),
        ]:
            try:
                p = fn()
                if p:
                    paths[name] = p
            except Exception as e:
                print(f"[visualizations] {name} error: {e}")

        return paths
