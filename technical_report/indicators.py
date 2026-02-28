"""
Volume & Momentum Indicators
===============================
New indicators not already in ``quant/technicals.py``:

  Volume:
    • VPVR (Volume Profile Visible Range) — horizontal volume histogram
    • VWAP (Volume Weighted Average Price) — intraday anchor

  Momentum (wraps existing + adds divergence flagging):
    • MACD histogram with divergence detection
    • RSI with bullish/bearish divergence
    • Bollinger %B and Bandwidth

Existing indicators (RSI, MACD, OBV, ATR, Bollinger Bands) are
computed by ``quant.technicals.TechnicalAnalyzer`` — this module
only adds what's missing.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


class VolumeProfiler:
    """Volume Profile Visible Range (VPVR) and VWAP calculator."""

    # ------------------------------------------------------------------
    # VPVR — Volume at Price
    # ------------------------------------------------------------------
    @staticmethod
    def vpvr(
        close: pd.Series,
        volume: pd.Series,
        high: pd.Series | None = None,
        low: pd.Series | None = None,
        n_bins: int = 50,
        lookback: int | None = None,
    ) -> dict:
        """Compute Volume Profile (VPVR).

        For each price bin, sums the volume traded at that level.
        Identifies:
          • Point of Control (POC) — price with highest volume
          • Value Area High / Low (70% of total volume)
          • High Volume Nodes (HVN) and Low Volume Nodes (LVN)

        Parameters
        ----------
        close, volume : pd.Series
        high, low     : optional (for typical price calculation)
        n_bins        : number of price bins
        lookback      : limit to last N bars (None = all data)
        """
        if volume is None or len(volume) < 30:
            return {"available": False, "reason": "Insufficient volume data"}

        if lookback:
            close = close.tail(lookback)
            volume = volume.tail(lookback)
            if high is not None:
                high = high.tail(lookback)
            if low is not None:
                low = low.tail(lookback)

        # Typical Price = (H + L + C) / 3 if available, else just Close
        if high is not None and low is not None:
            typical = (high + low + close) / 3
        else:
            typical = close

        price_min = float(typical.min())
        price_max = float(typical.max())

        if price_max <= price_min:
            return {"available": False, "reason": "No price range"}

        bin_edges = np.linspace(price_min, price_max, n_bins + 1)
        bin_volumes = np.zeros(n_bins)

        # Assign volume to price bins
        for i in range(len(typical)):
            p = float(typical.iloc[i])
            v = float(volume.iloc[i])
            idx = int(np.clip(
                np.searchsorted(bin_edges, p, side="right") - 1,
                0, n_bins - 1
            ))
            bin_volumes[idx] += v

        # Point of Control (highest volume bin)
        poc_idx = int(np.argmax(bin_volumes))
        poc_price = (bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2

        # Value Area (70% of total volume centered on POC)
        total_vol = bin_volumes.sum()
        target_vol = total_vol * 0.70
        accumulated = bin_volumes[poc_idx]
        va_lo_idx = poc_idx
        va_hi_idx = poc_idx

        while accumulated < target_vol:
            expand_lo = bin_volumes[va_lo_idx - 1] if va_lo_idx > 0 else 0
            expand_hi = bin_volumes[va_hi_idx + 1] if va_hi_idx < n_bins - 1 else 0

            if expand_hi >= expand_lo and va_hi_idx < n_bins - 1:
                va_hi_idx += 1
                accumulated += expand_hi
            elif va_lo_idx > 0:
                va_lo_idx -= 1
                accumulated += expand_lo
            else:
                break

        va_low_price = float(bin_edges[va_lo_idx])
        va_high_price = float(bin_edges[va_hi_idx + 1])

        # Build profile data for plotting
        profile = []
        avg_vol = float(bin_volumes.mean())
        for i in range(n_bins):
            mid = (bin_edges[i] + bin_edges[i + 1]) / 2
            vol = float(bin_volumes[i])
            node_type = "HVN" if vol > avg_vol * 1.5 else ("LVN" if vol < avg_vol * 0.5 else "NORMAL")
            profile.append({
                "price_low": round(float(bin_edges[i]), 2),
                "price_high": round(float(bin_edges[i + 1]), 2),
                "price_mid": round(float(mid), 2),
                "volume": round(vol, 0),
                "node_type": node_type,
            })

        return {
            "available": True,
            "poc_price": round(float(poc_price), 2),
            "value_area_high": round(va_high_price, 2),
            "value_area_low": round(va_low_price, 2),
            "n_bins": n_bins,
            "profile": profile,
            # For plotting
            "_bin_edges": bin_edges,
            "_bin_volumes": bin_volumes,
            "_poc_idx": poc_idx,
        }

    # ------------------------------------------------------------------
    # VWAP
    # ------------------------------------------------------------------
    @staticmethod
    def vwap(
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        volume: pd.Series,
    ) -> dict:
        """Compute cumulative VWAP and standard deviation bands.

        VWAP = Σ(Typical Price × Volume) / Σ(Volume)
        """
        if volume is None or len(volume) < 10:
            return {"available": False}

        typical = (high + low + close) / 3
        cum_tp_vol = (typical * volume).cumsum()
        cum_vol = volume.cumsum()
        vwap_series = cum_tp_vol / cum_vol

        # VWAP standard deviation bands
        vwap_sq_diff = ((typical - vwap_series) ** 2 * volume).cumsum() / cum_vol
        vwap_std = np.sqrt(vwap_sq_diff)

        upper1 = vwap_series + vwap_std
        lower1 = vwap_series - vwap_std
        upper2 = vwap_series + 2 * vwap_std
        lower2 = vwap_series - 2 * vwap_std

        current_vwap = float(vwap_series.iloc[-1])
        latest_price = float(close.iloc[-1])
        deviation_pct = round((latest_price / current_vwap - 1) * 100, 2)

        return {
            "available": True,
            "current_vwap": round(current_vwap, 2),
            "current_price": round(latest_price, 2),
            "deviation_pct": deviation_pct,
            "position": "ABOVE" if latest_price > current_vwap else "BELOW",
            "_vwap_series": vwap_series,
            "_upper1": upper1,
            "_lower1": lower1,
            "_upper2": upper2,
            "_lower2": lower2,
        }


class MomentumAnalyzer:
    """Extended momentum analytics with divergence detection."""

    # ------------------------------------------------------------------
    # RSI Divergence
    # ------------------------------------------------------------------
    @staticmethod
    def rsi_divergence(close: pd.Series, period: int = 14,
                       lookback: int = 50) -> dict:
        """Detect bullish/bearish divergence between price and RSI."""
        if len(close) < period + lookback:
            return {"available": False}

        # Compute RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.dropna()

        # Look at recent window
        recent_close = close.tail(lookback)
        recent_rsi = rsi.tail(lookback)

        if len(recent_close) < 20 or len(recent_rsi) < 20:
            return {"available": False}

        # Identify swing lows in price and RSI
        half = len(recent_close) // 2
        price_low1 = float(recent_close.iloc[:half].min())
        price_low2 = float(recent_close.iloc[half:].min())
        low1_idx = recent_close.iloc[:half].idxmin()
        low2_idx = recent_close.iloc[half:].idxmin()
        rsi_at_low1 = float(recent_rsi.loc[low1_idx]) if low1_idx in recent_rsi.index else 50.0
        rsi_at_low2 = float(recent_rsi.loc[low2_idx]) if low2_idx in recent_rsi.index else 50.0

        # Identify swing highs
        price_high1 = float(recent_close.iloc[:half].max())
        price_high2 = float(recent_close.iloc[half:].max())
        high1_idx = recent_close.iloc[:half].idxmax()
        high2_idx = recent_close.iloc[half:].idxmax()
        rsi_at_high1 = float(recent_rsi.loc[high1_idx]) if high1_idx in recent_rsi.index else 50.0
        rsi_at_high2 = float(recent_rsi.loc[high2_idx]) if high2_idx in recent_rsi.index else 50.0

        current_rsi = round(float(rsi.iloc[-1]), 2)
        divergence = "NONE"
        signal = ""

        # Bullish divergence: price makes lower low, RSI makes higher low
        if price_low2 < price_low1 and rsi_at_low2 > rsi_at_low1:
            divergence = "BULLISH"
            signal = "Price lower low + RSI higher low → potential reversal UP"

        # Bearish divergence: price makes higher high, RSI makes lower high
        if price_high2 > price_high1 and rsi_at_high2 < rsi_at_high1:
            divergence = "BEARISH"
            signal = "Price higher high + RSI lower high → potential reversal DOWN"

        # Overbought / oversold flags
        extreme = ""
        if current_rsi > 70:
            extreme = "OVERBOUGHT"
        elif current_rsi < 30:
            extreme = "OVERSOLD"

        return {
            "available": True,
            "current_rsi": current_rsi,
            "divergence": divergence,
            "signal": signal,
            "extreme": extreme,
            "_rsi_series": rsi,
        }

    # ------------------------------------------------------------------
    # MACD Divergence
    # ------------------------------------------------------------------
    @staticmethod
    def macd_analysis(close: pd.Series) -> dict:
        """MACD with histogram and divergence detection."""
        if len(close) < 35:
            return {"available": False}

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line

        current_macd = round(float(macd_line.iloc[-1]), 4)
        current_signal = round(float(signal_line.iloc[-1]), 4)
        current_hist = round(float(histogram.iloc[-1]), 4)

        # Crossover
        crossover = "NONE"
        if len(histogram) >= 3:
            if histogram.iloc[-1] > 0 and histogram.iloc[-3] < 0:
                crossover = "BULLISH_CROSSOVER"
            elif histogram.iloc[-1] < 0 and histogram.iloc[-3] > 0:
                crossover = "BEARISH_CROSSOVER"
            elif histogram.iloc[-1] > 0:
                crossover = "BULLISH"
            else:
                crossover = "BEARISH"

        # Histogram momentum — measures rate of change of the histogram.
        # "STRENGTHENING" = histogram moving away from zero (momentum building)
        # "WEAKENING"     = histogram moving toward zero (momentum fading)
        # We look at absolute magnitude to avoid directional confusion.
        if len(histogram) >= 5:
            recent_hist = histogram.tail(5).values
            abs_now = abs(recent_hist[-1])
            abs_prev = abs(recent_hist[-3])
            if abs_now > abs_prev:
                hist_trend = "STRENGTHENING"   # momentum building
            elif abs_now < abs_prev:
                hist_trend = "FADING"          # momentum losing steam
            else:
                hist_trend = "FLAT"
        else:
            hist_trend = "N/A"

        return {
            "available": True,
            "macd": current_macd,
            "signal": current_signal,
            "histogram": current_hist,
            "crossover": crossover,
            "histogram_trend": hist_trend,
            "_macd_line": macd_line,
            "_signal_line": signal_line,
            "_histogram": histogram,
        }

    # ------------------------------------------------------------------
    # Bollinger %B and Bandwidth
    # ------------------------------------------------------------------
    @staticmethod
    def bollinger_extended(close: pd.Series, period: int = 20,
                           std_dev: float = 2.0) -> dict:
        """Bollinger Bands with %B and Bandwidth metrics."""
        if len(close) < period + 5:
            return {"available": False}

        sma = close.rolling(period).mean()
        std = close.rolling(period).std()
        upper = sma + std_dev * std
        lower = sma - std_dev * std

        # %B = (Price - Lower) / (Upper - Lower)
        bandwidth = ((upper - lower) / sma * 100).dropna()
        pct_b = ((close - lower) / (upper - lower)).dropna()

        current_pct_b = round(float(pct_b.iloc[-1]), 4)
        current_bw = round(float(bandwidth.iloc[-1]), 2)

        # Squeeze detection (bandwidth at historic low)
        if len(bandwidth) > 50:
            bw_percentile = float(
                (bandwidth < current_bw).sum() / len(bandwidth) * 100
            )
            squeeze = bw_percentile < 10
        else:
            bw_percentile = None
            squeeze = False

        return {
            "available": True,
            "pct_b": current_pct_b,
            "bandwidth_pct": current_bw,
            "bandwidth_percentile": round(bw_percentile, 1) if bw_percentile else None,
            "squeeze": squeeze,
            "current_upper": round(float(upper.iloc[-1]), 2),
            "current_lower": round(float(lower.iloc[-1]), 2),
            "current_sma": round(float(sma.iloc[-1]), 2),
            "_upper": upper,
            "_lower": lower,
            "_sma": sma,
            "_pct_b": pct_b,
            "_bandwidth": bandwidth,
        }
