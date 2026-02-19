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

        # Normalise column names to lowercase
        df = price_df.copy()
        df.columns = [c.lower().strip() for c in df.columns]

        if 'close' not in df.columns:
            return {'available': False, 'reason': "Missing 'close' column"}

        close = df['close'].astype(float)

        # high / low may be absent in screener.in weekly data — estimate
        if 'high' in df.columns:
            high = df['high'].astype(float)
        else:
            high = close  # best available proxy

        if 'low' in df.columns:
            low = df['low'].astype(float)
        else:
            low = close

        has_ohlc = ('high' in df.columns and 'low' in df.columns)

        volume = df['volume'].astype(float) if 'volume' in df.columns else pd.Series(
            0, index=close.index, dtype=float)

        delivery = (df['delivery_pct'].astype(float)
                    if 'delivery_pct' in df.columns else None)

        result = {'available': True, 'symbol': symbol}

        # ── Trend Analysis ───────────────────────────────────
        result['trend'] = self._trend_analysis(close)

        # ── Momentum Indicators ──────────────────────────────
        result['momentum'] = self._momentum_analysis(close)

        # ── Volume Analysis ──────────────────────────────────
        result['volume_analysis'] = self._volume_analysis(close, volume)

        # ── Delivery Volume Analysis ────────────────────────
        if delivery is not None:
            result['delivery_analysis'] = self._delivery_analysis(
                close, volume, delivery)
        else:
            result['delivery_analysis'] = {
                'available': False,
                'reason': 'Delivery % data not available in price feed'}

        # ── Volatility ───────────────────────────────────────
        result['volatility'] = self._volatility_analysis(close, high, low, has_ohlc)

        # ── Support / Resistance Levels ──────────────────────
        result['support_resistance'] = self._support_resistance(close, high, low, has_ohlc)

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
    # Delivery Volume Analysis
    # ==================================================================
    def _delivery_analysis(self, close: pd.Series,
                           volume: pd.Series,
                           delivery_pct: pd.Series) -> dict:
        """Analyse delivery volume % for smart-money signals."""
        # Drop NaN delivery rows
        valid = delivery_pct.dropna()
        if len(valid) < 20:
            return {'available': False,
                    'reason': 'Insufficient delivery data (need ≥ 20 bars)'}

        da = {'available': True}
        latest_del = float(valid.iloc[-1])
        da['latest_delivery_pct'] = round(latest_del, 1)

        # Averages
        avg_20 = float(valid.tail(20).mean())
        da['avg_delivery_20d'] = round(avg_20, 1)

        if len(valid) >= 50:
            avg_50 = float(valid.tail(50).mean())
            da['avg_delivery_50d'] = round(avg_50, 1)

        if len(valid) >= 200:
            avg_200 = float(valid.tail(200).mean())
            da['avg_delivery_200d'] = round(avg_200, 1)

        # Delivery trend: is 20d avg above or below 50d avg?
        if len(valid) >= 50:
            da['delivery_trend'] = ('RISING' if avg_20 > avg_50 * 1.05
                                    else ('FALLING' if avg_20 < avg_50 * 0.95
                                          else 'STABLE'))
        else:
            da['delivery_trend'] = 'N/A'

        # Smart money signal:
        # High delivery % + rising price = institutional accumulation
        # High delivery % + falling price = institutional distribution
        # Low delivery % + rising price = speculative rally (weak)
        if len(close) >= 20 and len(valid) >= 20:
            price_chg_20d = (close.iloc[-1] / close.iloc[-20] - 1) * 100
            high_delivery = avg_20 > 50  # >50% is considered high
            low_delivery = avg_20 < 30   # <30% is considered low

            if high_delivery and price_chg_20d > 2:
                da['smart_money_signal'] = 'ACCUMULATION'
                da['smart_money_detail'] = (
                    f'🟢 High delivery ({avg_20:.1f}%) with price rising '
                    f'{price_chg_20d:+.1f}% — institutional accumulation likely')
            elif high_delivery and price_chg_20d < -2:
                da['smart_money_signal'] = 'DISTRIBUTION'
                da['smart_money_detail'] = (
                    f'🔴 High delivery ({avg_20:.1f}%) with price falling '
                    f'{price_chg_20d:+.1f}% — institutional distribution likely')
            elif low_delivery and price_chg_20d > 5:
                da['smart_money_signal'] = 'SPECULATIVE_RALLY'
                da['smart_money_detail'] = (
                    f'⚠️ Low delivery ({avg_20:.1f}%) with price surge '
                    f'{price_chg_20d:+.1f}% — speculative/trader-driven, '
                    f'rally may lack conviction')
            elif low_delivery and price_chg_20d < -5:
                da['smart_money_signal'] = 'PANIC_SELLING'
                da['smart_money_detail'] = (
                    f'⚠️ Low delivery ({avg_20:.1f}%) with sharp fall '
                    f'{price_chg_20d:+.1f}% — panic/stop-loss driven '
                    f'selling, not institutional exit')
            else:
                da['smart_money_signal'] = 'NEUTRAL'
                da['smart_money_detail'] = (
                    f'Delivery {avg_20:.1f}% with price change '
                    f'{price_chg_20d:+.1f}% — no clear smart money signal')

            da['price_change_20d'] = round(price_chg_20d, 2)

        # Delivery spike detection (latest vs 20d avg)
        if avg_20 > 0:
            da['relative_delivery'] = round(latest_del / avg_20, 2)
            if latest_del > avg_20 * 1.5:
                da['delivery_spike'] = True
                da['delivery_spike_detail'] = (
                    f'Delivery spike: {latest_del:.1f}% vs '
                    f'{avg_20:.1f}% avg — unusual institutional activity')
            else:
                da['delivery_spike'] = False

        return da

    # ==================================================================
    # Volatility
    # ==================================================================
    def _volatility_analysis(self, close: pd.Series,
                             high: pd.Series, low: pd.Series,
                             has_ohlc: bool = True) -> dict:
        n = len(close)
        volatility = {'available': True}
        if not has_ohlc:
            volatility['note'] = 'ATR estimated from close-only data (no high/low)'

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

        # Delivery volume signals
        delivery = result.get('delivery_analysis', {})
        if delivery.get('available'):
            smart = delivery.get('smart_money_signal')
            if smart == 'ACCUMULATION':
                bull_signals += 1
                total_signals += 1
            elif smart == 'DISTRIBUTION':
                bear_signals += 1
                total_signals += 1
            elif smart == 'SPECULATIVE_RALLY':
                bear_signals += 1    # speculative rallies are bearish warning
                total_signals += 1

        # Composite
        if total_signals == 0:
            return {'signal': 'NEUTRAL', 'confidence': 'LOW',
                    'bull_count': 0, 'bear_count': 0, 'total': 0}

        bull_pct = bull_signals / total_signals
        bear_pct = bear_signals / total_signals

        # Thresholds from natural thirds of the signal distribution
        if bull_pct >= 2/3:
            signal = 'STRONG_BULLISH'
        elif bull_pct > bear_pct:
            signal = 'MILDLY_BULLISH'
        elif bear_pct >= 2/3:
            signal = 'STRONG_BEARISH'
        elif bear_pct > bull_pct:
            signal = 'MILDLY_BEARISH'
        else:
            signal = 'NEUTRAL'

        confidence = 'HIGH' if max(bull_pct, bear_pct) >= 2/3 else \
                     ('MEDIUM' if max(bull_pct, bear_pct) > 0.5 else 'LOW')

        return {
            'signal': signal,
            'confidence': confidence,
            'bull_count': bull_signals,
            'bear_count': bear_signals,
            'total': total_signals,
        }

    # ==================================================================
    # Support / Resistance Levels (Pivot-Based)
    # ==================================================================
    def _support_resistance(self, close: pd.Series,
                            high: pd.Series, low: pd.Series,
                            has_ohlc: bool = True) -> dict:
        """Compute pivot-based support and resistance levels.

        Uses three methods, all from real price history:
          1. Classic Pivot Points (daily: P, S1/S2/S3, R1/R2/R3)
          2. Fibonacci Retracement levels from 52-week range
          3. Volume-weighted price clusters (congestion zones)

        Parameters
        ----------
        close, high, low : pd.Series of price data
        has_ohlc : whether high/low are real (not proxied from close)
        """
        n = len(close)
        if n < 30:
            return {'available': False,
                    'reason': 'Need ≥30 price bars for S/R levels'}

        latest = float(close.iloc[-1])
        result = {'available': True, 'current_price': round(latest, 2)}

        # ── 1. Classic Pivot Points ──────────────────────────
        # Use the last complete trading period (yesterday's H/L/C)
        h_val = float(high.iloc[-2]) if len(high) >= 2 else float(high.iloc[-1])
        l_val = float(low.iloc[-2]) if len(low) >= 2 else float(low.iloc[-1])
        c_val = float(close.iloc[-2]) if len(close) >= 2 else float(close.iloc[-1])

        pivot = (h_val + l_val + c_val) / 3
        r1 = 2 * pivot - l_val
        s1 = 2 * pivot - h_val
        r2 = pivot + (h_val - l_val)
        s2 = pivot - (h_val - l_val)
        r3 = h_val + 2 * (pivot - l_val)
        s3 = l_val - 2 * (h_val - pivot)

        result['pivot_points'] = {
            'pivot': round(pivot, 2),
            'r1': round(r1, 2),
            'r2': round(r2, 2),
            'r3': round(r3, 2),
            's1': round(s1, 2),
            's2': round(s2, 2),
            's3': round(s3, 2),
        }

        # Current position relative to pivot
        if latest > r1:
            result['pivot_zone'] = 'ABOVE_R1'
        elif latest > pivot:
            result['pivot_zone'] = 'ABOVE_PIVOT'
        elif latest > s1:
            result['pivot_zone'] = 'BELOW_PIVOT'
        else:
            result['pivot_zone'] = 'BELOW_S1'

        # ── 2. Fibonacci Retracement (52-week range) ─────────
        lookback = min(252, n)
        period_high = float(high.tail(lookback).max())
        period_low = float(low.tail(lookback).min())
        fib_range = period_high - period_low

        if fib_range > 0:
            fib_levels = {}
            for level, ratio in [('0.0%', 0.0), ('23.6%', 0.236),
                                  ('38.2%', 0.382), ('50.0%', 0.5),
                                  ('61.8%', 0.618), ('78.6%', 0.786),
                                  ('100.0%', 1.0)]:
                fib_levels[level] = round(period_high - fib_range * ratio, 2)

            result['fibonacci'] = {
                'period_high': round(period_high, 2),
                'period_low': round(period_low, 2),
                'levels': fib_levels,
            }

            # Find nearest Fibonacci support and resistance
            fib_supports = [v for v in fib_levels.values() if v < latest]
            fib_resistances = [v for v in fib_levels.values() if v > latest]
            if fib_supports:
                result['fibonacci']['nearest_support'] = max(fib_supports)
            if fib_resistances:
                result['fibonacci']['nearest_resistance'] = min(fib_resistances)

        # ── 3. Price Congestion Zones ────────────────────────
        # Cluster historical prices into zones to find areas where
        # price spent the most time (natural S/R).
        lookback_cong = min(500, n)
        recent = close.tail(lookback_cong).values
        num_bins = max(10, lookback_cong // 25)

        counts, bin_edges = np.histogram(recent, bins=num_bins)

        # Top congestion zones (highest frequency bins)
        sorted_indices = np.argsort(counts)[::-1]
        zones = []
        for idx in sorted_indices[:5]:
            zone_low = float(bin_edges[idx])
            zone_high = float(bin_edges[idx + 1])
            zone_mid = (zone_low + zone_high) / 2
            frequency = int(counts[idx])
            zone_type = 'SUPPORT' if zone_mid < latest else 'RESISTANCE'
            zones.append({
                'low': round(zone_low, 2),
                'high': round(zone_high, 2),
                'mid': round(zone_mid, 2),
                'frequency': frequency,
                'type': zone_type,
            })

        # Sort by proximity to current price
        zones.sort(key=lambda z: abs(z['mid'] - latest))
        result['congestion_zones'] = zones

        # ── Summary: key levels ──────────────────────────────
        supports = []
        resistances = []

        # From pivot points
        for lbl, val in [('S1', s1), ('S2', s2), ('S3', s3)]:
            if val < latest:
                supports.append({'level': round(val, 2), 'source': f'Pivot {lbl}'})
        for lbl, val in [('R1', r1), ('R2', r2), ('R3', r3)]:
            if val > latest:
                resistances.append({'level': round(val, 2), 'source': f'Pivot {lbl}'})

        # From Fibonacci
        if 'fibonacci' in result:
            fib = result['fibonacci']
            if fib.get('nearest_support'):
                supports.append({
                    'level': fib['nearest_support'],
                    'source': 'Fibonacci'})
            if fib.get('nearest_resistance'):
                resistances.append({
                    'level': fib['nearest_resistance'],
                    'source': 'Fibonacci'})

        # Sort supports descending (nearest first), resistances ascending
        supports.sort(key=lambda x: -x['level'])
        resistances.sort(key=lambda x: x['level'])

        result['key_supports'] = supports[:4]
        result['key_resistances'] = resistances[:4]

        return result

