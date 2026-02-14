"""
Technical & Volume Analysis
============================
Computes key technical indicators from price/volume data:

  Trend:
    • 50-DMA / 200-DMA and Golden/Death Cross
    • Price position relative to moving averages

  Momentum:
    • RSI (14-day)
    • MACD (12, 26, 9)
    • Rate of Change (ROC)

  Volume:
    • OBV (On-Balance Volume)
    • Volume trend (20-day average vs 50-day average)
    • Volume-price divergence detection
    • Delivery volume analysis (if available)

  Volatility:
    • ATR (14-day Average True Range)
    • Bollinger Band width

All from yfinance — no API keys needed.
"""
import numpy as np
import pandas as pd


class TechnicalAnalyzer:
    """Compute technical indicators for Indian equities."""

    def analyze(self, price_df: pd.DataFrame,
                symbol: str = '') -> dict:
        """
        Full technical analysis from OHLCV DataFrame.

        Parameters
        ----------
        price_df : DataFrame with columns [open, high, low, close, volume]
        symbol   : str (for display only)

        Returns
        -------
        dict with trend, momentum, volume, volatility sub-dicts + overall signal
        """
        if price_df is None or price_df.empty or len(price_df) < 30:
            return {'available': False,
                    'reason': 'Insufficient price data (need ≥ 30 bars)'}

        close = price_df['close'].astype(float)
        high = price_df['high'].astype(float)
        low = price_df['low'].astype(float)
        volume = price_df['volume'].astype(float)

        result = {'available': True, 'symbol': symbol}

        # ── Trend Analysis ───────────────────────────────────
        result['trend'] = self._trend_analysis(close)

        # ── Momentum Indicators ──────────────────────────────
        result['momentum'] = self._momentum_analysis(close)

        # ── Volume Analysis ──────────────────────────────────
        result['volume_analysis'] = self._volume_analysis(close, volume)

        # ── Volatility ───────────────────────────────────────
        result['volatility'] = self._volatility_analysis(close, high, low)

        # ── Overall Signal ───────────────────────────────────
        result['overall_signal'] = self._composite_signal(result)

        return result

    # ==================================================================
    # Trend
    # ==================================================================
    def _trend_analysis(self, close: pd.Series) -> dict:
        n = len(close)
        latest = float(close.iloc[-1])

        trend = {'available': True}

        # 50-DMA
        if n >= 50:
            dma50 = close.rolling(50).mean()
            trend['dma_50'] = round(float(dma50.iloc[-1]), 2)
            trend['above_50dma'] = latest > trend['dma_50']
        else:
            dma50 = None

        # 200-DMA
        if n >= 200:
            dma200 = close.rolling(200).mean()
            trend['dma_200'] = round(float(dma200.iloc[-1]), 2)
            trend['above_200dma'] = latest > trend['dma_200']
        else:
            dma200 = None

        # Golden/Death Cross
        if dma50 is not None and dma200 is not None:
            recent_50 = dma50.tail(5)
            recent_200 = dma200.tail(5)
            if len(recent_50.dropna()) >= 5 and len(recent_200.dropna()) >= 5:
                cross_now = float(recent_50.iloc[-1]) > float(recent_200.iloc[-1])
                cross_prev = float(recent_50.iloc[-5]) > float(recent_200.iloc[-5])
                if cross_now and not cross_prev:
                    trend['cross'] = 'GOLDEN_CROSS'
                    trend['cross_signal'] = '🟢 Golden Cross (50-DMA crossed above 200-DMA)'
                elif not cross_now and cross_prev:
                    trend['cross'] = 'DEATH_CROSS'
                    trend['cross_signal'] = '🔴 Death Cross (50-DMA crossed below 200-DMA)'
                else:
                    trend['cross'] = 'NONE'
                    if cross_now:
                        trend['cross_signal'] = '50-DMA above 200-DMA (bullish structure)'
                    else:
                        trend['cross_signal'] = '50-DMA below 200-DMA (bearish structure)'

        # Price vs moving averages
        if dma50 is not None:
            pct_from_50 = round((latest / float(dma50.iloc[-1]) - 1) * 100, 2)
            trend['pct_from_50dma'] = pct_from_50
        if dma200 is not None:
            pct_from_200 = round((latest / float(dma200.iloc[-1]) - 1) * 100, 2)
            trend['pct_from_200dma'] = pct_from_200

        # 20-day trend slope
        if n >= 20:
            recent = close.tail(20).values
            x = np.arange(len(recent))
            slope, _ = np.polyfit(x, recent, 1)
            trend['short_term_slope'] = round(float(slope), 4)
            trend['short_term_direction'] = (
                'UP' if slope > 0.2 else ('DOWN' if slope < -0.2 else 'SIDEWAYS'))

        return trend

    # ==================================================================
    # Momentum
    # ==================================================================
    def _momentum_analysis(self, close: pd.Series) -> dict:
        n = len(close)
        momentum = {'available': True}

        # RSI (14-day)
        if n >= 15:
            delta = close.diff()
            gain = delta.where(delta > 0, 0.0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
            rs = gain / loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))
            rsi_val = float(rsi.iloc[-1])
            momentum['rsi'] = round(rsi_val, 2)
            if rsi_val > 70:
                momentum['rsi_signal'] = 'OVERBOUGHT'
            elif rsi_val < 30:
                momentum['rsi_signal'] = 'OVERSOLD'
            else:
                momentum['rsi_signal'] = 'NEUTRAL'

        # MACD (12, 26, 9)
        if n >= 35:
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            histogram = macd_line - signal_line

            momentum['macd'] = round(float(macd_line.iloc[-1]), 4)
            momentum['macd_signal'] = round(float(signal_line.iloc[-1]), 4)
            momentum['macd_histogram'] = round(float(histogram.iloc[-1]), 4)

            # Crossover detection
            if len(histogram) >= 3:
                if histogram.iloc[-1] > 0 and histogram.iloc[-3] < 0:
                    momentum['macd_crossover'] = 'BULLISH_CROSSOVER'
                elif histogram.iloc[-1] < 0 and histogram.iloc[-3] > 0:
                    momentum['macd_crossover'] = 'BEARISH_CROSSOVER'
                elif histogram.iloc[-1] > 0:
                    momentum['macd_crossover'] = 'BULLISH'
                else:
                    momentum['macd_crossover'] = 'BEARISH'

        # Rate of Change (20-day)
        if n >= 21:
            roc = ((close.iloc[-1] / close.iloc[-21]) - 1) * 100
            momentum['roc_20d'] = round(float(roc), 2)

        # 52-week high/low proximity
        if n >= 252:
            high_52w = float(close.tail(252).max())
            low_52w = float(close.tail(252).min())
            latest = float(close.iloc[-1])
            momentum['high_52w'] = round(high_52w, 2)
            momentum['low_52w'] = round(low_52w, 2)
            momentum['pct_from_52w_high'] = round(
                (latest / high_52w - 1) * 100, 2)
            momentum['pct_from_52w_low'] = round(
                (latest / low_52w - 1) * 100, 2)

        return momentum

    # ==================================================================
    # Volume
    # ==================================================================
    def _volume_analysis(self, close: pd.Series,
                         volume: pd.Series) -> dict:
        n = len(close)
        vol = {'available': True}

        if n < 20:
            vol['available'] = False
            return vol

        latest_vol = float(volume.iloc[-1])
        avg_20 = float(volume.tail(20).mean())
        vol['latest_volume'] = int(latest_vol)
        vol['avg_volume_20d'] = int(avg_20)

        if n >= 50:
            avg_50 = float(volume.tail(50).mean())
            vol['avg_volume_50d'] = int(avg_50)
            vol['volume_trend'] = 'EXPANDING' if avg_20 > avg_50 * 1.15 else \
                                  ('CONTRACTING' if avg_20 < avg_50 * 0.85 else 'STABLE')
        else:
            vol['volume_trend'] = 'N/A'

        # Relative volume (today vs 20-day avg)
        if avg_20 > 0:
            vol['relative_volume'] = round(latest_vol / avg_20, 2)

        # OBV (On-Balance Volume)
        price_change = close.diff()
        obv = pd.Series(0, index=close.index, dtype=float)
        obv[price_change > 0] = volume[price_change > 0]
        obv[price_change < 0] = -volume[price_change < 0]
        obv = obv.cumsum()
        vol['obv_latest'] = int(obv.iloc[-1])

        # OBV trend (20-day slope)
        if n >= 20:
            obv_recent = obv.tail(20).values
            x = np.arange(len(obv_recent))
            obv_slope, _ = np.polyfit(x, obv_recent, 1)
            vol['obv_trend'] = 'ACCUMULATION' if obv_slope > 0 else 'DISTRIBUTION'

        # Volume-Price Divergence
        if n >= 20:
            price_slope_sign = 1 if close.iloc[-1] > close.iloc[-20] else -1
            obv_slope_sign = 1 if obv_slope > 0 else -1
            if price_slope_sign > 0 and obv_slope_sign < 0:
                vol['divergence'] = 'BEARISH_DIVERGENCE'
                vol['divergence_signal'] = (
                    '⚠️ Price rising but volume declining — '
                    'bearish divergence, rally may weaken')
            elif price_slope_sign < 0 and obv_slope_sign > 0:
                vol['divergence'] = 'BULLISH_DIVERGENCE'
                vol['divergence_signal'] = (
                    '🟢 Price falling but accumulation happening — '
                    'bullish divergence, bottom may be near')
            else:
                vol['divergence'] = 'NONE'
                vol['divergence_signal'] = 'Price and volume confirm the trend'

        return vol

    # ==================================================================
    # Volatility
    # ==================================================================
    def _volatility_analysis(self, close: pd.Series,
                             high: pd.Series, low: pd.Series) -> dict:
        n = len(close)
        volatility = {'available': True}

        if n < 14:
            volatility['available'] = False
            return volatility

        # ATR (14-day)
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        atr_val = float(atr.iloc[-1])
        volatility['atr_14'] = round(atr_val, 2)
        volatility['atr_pct'] = round(atr_val / float(close.iloc[-1]) * 100, 2)

        # Bollinger Bands (20, 2)
        if n >= 20:
            sma20 = close.rolling(20).mean()
            std20 = close.rolling(20).std()
            upper = sma20 + 2 * std20
            lower = sma20 - 2 * std20
            bb_width = (float(upper.iloc[-1]) - float(lower.iloc[-1])) / float(sma20.iloc[-1])
            volatility['bb_upper'] = round(float(upper.iloc[-1]), 2)
            volatility['bb_lower'] = round(float(lower.iloc[-1]), 2)
            volatility['bb_width_pct'] = round(bb_width * 100, 2)

            latest = float(close.iloc[-1])
            if latest > float(upper.iloc[-1]):
                volatility['bb_position'] = 'ABOVE_UPPER'
            elif latest < float(lower.iloc[-1]):
                volatility['bb_position'] = 'BELOW_LOWER'
            else:
                volatility['bb_position'] = 'WITHIN_BANDS'

        # Historical volatility (20-day annualized)
        if n >= 21:
            returns = close.pct_change().dropna()
            hv = float(returns.tail(20).std() * np.sqrt(252) * 100)
            volatility['hist_volatility_20d'] = round(hv, 2)

        return volatility

    # ==================================================================
    # Composite Signal
    # ==================================================================
    def _composite_signal(self, result: dict) -> dict:
        """Combine all technical signals into one composite view."""
        bull_signals = 0
        bear_signals = 0
        total_signals = 0

        trend = result.get('trend', {})
        mom = result.get('momentum', {})
        vol = result.get('volume_analysis', {})

        # Trend signals
        if trend.get('above_50dma'):
            bull_signals += 1
        else:
            bear_signals += 1
        total_signals += 1

        if trend.get('above_200dma'):
            bull_signals += 1
        else:
            bear_signals += 1
        total_signals += 1

        cross = trend.get('cross', 'NONE')
        if cross == 'GOLDEN_CROSS':
            bull_signals += 2
            total_signals += 2
        elif cross == 'DEATH_CROSS':
            bear_signals += 2
            total_signals += 2

        # Momentum signals
        rsi_sig = mom.get('rsi_signal')
        if rsi_sig == 'OVERBOUGHT':
            bear_signals += 1
            total_signals += 1
        elif rsi_sig == 'OVERSOLD':
            bull_signals += 1
            total_signals += 1

        macd_cross = mom.get('macd_crossover')
        if macd_cross in ('BULLISH', 'BULLISH_CROSSOVER'):
            bull_signals += 1
            total_signals += 1
        elif macd_cross in ('BEARISH', 'BEARISH_CROSSOVER'):
            bear_signals += 1
            total_signals += 1

        # Volume signals
        obv_trend = vol.get('obv_trend')
        if obv_trend == 'ACCUMULATION':
            bull_signals += 1
        elif obv_trend == 'DISTRIBUTION':
            bear_signals += 1
        total_signals += 1

        div = vol.get('divergence')
        if div == 'BULLISH_DIVERGENCE':
            bull_signals += 1
            total_signals += 1
        elif div == 'BEARISH_DIVERGENCE':
            bear_signals += 1
            total_signals += 1

        # Composite
        if total_signals == 0:
            return {'signal': 'NEUTRAL', 'confidence': 'LOW',
                    'bull_count': 0, 'bear_count': 0, 'total': 0}

        bull_pct = bull_signals / total_signals
        bear_pct = bear_signals / total_signals

        if bull_pct >= 0.7:
            signal = 'STRONG_BULLISH'
        elif bull_pct >= 0.5:
            signal = 'MILDLY_BULLISH'
        elif bear_pct >= 0.7:
            signal = 'STRONG_BEARISH'
        elif bear_pct >= 0.5:
            signal = 'MILDLY_BEARISH'
        else:
            signal = 'NEUTRAL'

        confidence = 'HIGH' if max(bull_pct, bear_pct) >= 0.75 else \
                     ('MEDIUM' if max(bull_pct, bear_pct) >= 0.55 else 'LOW')

        return {
            'signal': signal,
            'confidence': confidence,
            'bull_count': bull_signals,
            'bear_count': bear_signals,
            'total': total_signals,
        }
