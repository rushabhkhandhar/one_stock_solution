#!/usr/bin/env python3
"""
Integration Test Suite — 100 % Real Data
==========================================
Every test uses LIVE scraped data from screener.in, real concall
transcripts from disk, and real-time yfinance feeds.

ZERO synthetic / fake / simulated / random data.

Run:   python test_integration.py              (default: RELIANCE)
       python test_integration.py TCS          (any BSE symbol)
       python -m pytest test_integration.py -v
"""
import sys
import os
import time
import tempfile
import traceback

import numpy as np
import pandas as pd

# ── Ensure project root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# =====================================================================
#  Live data loader — scrapes once, reused by every test
# =====================================================================

_LIVE_DATA = None          # cached after first load
_LIVE_CONCALL_TEXTS = None # cached real transcripts
_LIVE_SYMBOL = None


def _load_live_data(symbol: str = 'RELIANCE'):
    """
    Scrape real financial data from screener.in and load real
    concall transcripts from disk.  Called ONCE; cached globally.
    """
    global _LIVE_DATA, _LIVE_CONCALL_TEXTS, _LIVE_SYMBOL

    if _LIVE_DATA is not None:
        return _LIVE_DATA, _LIVE_CONCALL_TEXTS

    from data.ingestion import DataIngestion
    from data.preprocessing import DataPreprocessor

    print(f"\n  ⏳  Scraping LIVE data for {symbol} from screener.in …")
    t0 = time.time()

    di = DataIngestion()
    data = di.load_company(symbol, consolidated=True)

    # Clean & derive
    pp = DataPreprocessor()
    data = pp.clean(data)
    data = pp.compute_derived(data)

    elapsed = time.time() - t0
    print(f"  ✔  Live data loaded in {elapsed:.1f}s  "
          f"(P&L rows={len(data.get('pnl', []))}, "
          f"BS rows={len(data.get('balance_sheet', []))}, "
          f"price rows={len(data.get('price', []))})\n")

    _LIVE_DATA = data
    _LIVE_SYMBOL = data.get('symbol', symbol)

    # ── Real concall transcripts from disk ───────────────────────────
    transcript_dir = os.path.join('.', 'output', 'transcripts')
    transcripts = []
    if os.path.isdir(transcript_dir):
        for fname in sorted(os.listdir(transcript_dir), reverse=True):
            if fname.endswith('.txt'):
                fpath = os.path.join(transcript_dir, fname)
                with open(fpath, 'r', encoding='utf-8') as f:
                    text = f.read()
                if text and len(text) > 200:
                    transcripts.append(text)

    # Also use transcripts fetched by the scraper (if any)
    scraped_texts = data.get('concall_texts', [])
    if scraped_texts:
        existing_lens = {len(t) for t in transcripts}
        for st in scraped_texts:
            if len(st) not in existing_lens:
                transcripts.append(st)

    _LIVE_CONCALL_TEXTS = transcripts
    print(f"  ✔  Loaded {len(transcripts)} real concall transcripts "
          f"(total {sum(len(t) for t in transcripts):,} chars)\n")

    return _LIVE_DATA, _LIVE_CONCALL_TEXTS


def get_data():
    """Return cached live data; load on first call."""
    if _LIVE_DATA is None:
        # When running via pytest, sys.argv may contain pytest arguments.
        # Only use sys.argv[1] as symbol if it looks like a stock ticker
        # (all uppercase, no dots/dashes typically used by pytest).
        sym = 'RELIANCE'
        if len(sys.argv) > 1:
            candidate = sys.argv[1]
            if candidate.isalpha() and candidate.isupper() and len(candidate) <= 20:
                sym = candidate
        _load_live_data(sym)
    return _LIVE_DATA, _LIVE_CONCALL_TEXTS


# =====================================================================
#  Test harness
# =====================================================================

PASS = 0
FAIL = 0
SKIP = 0


def _run(label, fn):
    """Execute a test function; track pass/fail/skip."""
    global PASS, FAIL, SKIP
    try:
        result = fn()
        if result == 'SKIP':
            SKIP += 1
            print(f"  ⏭️  {label} — skipped (optional dependency or insufficient data)")
        else:
            PASS += 1
            print(f"  ✅  {label}")
    except Exception as e:
        FAIL += 1
        print(f"  ❌  {label}  →  {e}")
        traceback.print_exc(limit=4)
        print()


# ─────────────────────────────────────────────────────────────────────
#  1. Config — singleton and defaults
# ─────────────────────────────────────────────────────────────────────
def test_config():
    from config import config, Config
    assert isinstance(config, Config)
    assert config.market.risk_free_rate > 0
    assert config.market.projection_years == 10
    assert config.thresholds.mscore_manipulation == -1.78
    assert config.thresholds.fscore_strong == 8
    assert os.path.isdir(config.output_dir)
    print(f"        Rf={config.market.risk_free_rate}, "
          f"MRP={config.market.market_risk_premium}, "
          f"terminal_g={config.market.terminal_growth_rate}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  2. DataPreprocessor on real data
# ─────────────────────────────────────────────────────────────────────
def test_preprocessor():
    from data.preprocessing import DataPreprocessor, get_value
    data, _ = get_data()
    pp = DataPreprocessor()

    pnl = data.get('pnl', pd.DataFrame())
    assert not pnl.empty, "Real P&L data is empty"

    sales = pp.get(pnl, 'sales')
    assert not sales.empty, "Canonical 'sales' lookup failed on real data"
    v = get_value(sales)
    assert not np.isnan(v), "Latest sales is NaN"
    assert v > 0, f"Latest sales should be positive, got {v}"

    derived = pp.compute_derived(data)
    shares = derived.get('shares_outstanding')
    assert shares is not None, "shares_outstanding not computed"
    latest_shares = get_value(shares)
    assert not np.isnan(latest_shares), "shares_outstanding is NaN"
    print(f"        Latest Sales=₹{v:,.0f} Cr, "
          f"Shares={latest_shares:.2f} Cr")
    return True


# ─────────────────────────────────────────────────────────────────────
#  3. DCF Model on real data
# ─────────────────────────────────────────────────────────────────────
def test_dcf():
    from quant.dcf import DCFModel
    data, _ = get_data()
    result = DCFModel().calculate(data)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'intrinsic_value' in result
        assert 'wacc' in result
        assert 'current_price' in result
        assert isinstance(result['intrinsic_value'], (int, float))
        print(f"        Intrinsic=₹{result['intrinsic_value']:,.2f}, "
              f"CMP=₹{result['current_price']:,.2f}, "
              f"Upside={result.get('upside_pct', 'N/A')}%, "
              f"WACC={result['wacc']}%, "
              f"Beta estimated={result.get('beta_estimated', '?')}")
    else:
        print(f"        DCF not available: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  4. CFO/EBITDA Check on real data
# ─────────────────────────────────────────────────────────────────────
def test_cfo_ebitda():
    from quant.dcf import DCFModel
    data, _ = get_data()
    result = DCFModel.cfo_ebitda_check(data)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'conversion_pct' in result
        assert 'is_red_flag' in result
        print(f"        CFO/EBITDA={result['conversion_pct']}%, "
              f"red_flag={result['is_red_flag']}")
    else:
        print(f"        CFO/EBITDA not available: {result.get('reason')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  5. SOTP Model on real data
# ─────────────────────────────────────────────────────────────────────
def test_sotp():
    from quant.sotp import SOTPModel
    from quant.segmental import SegmentalAnalysis
    data, _ = get_data()

    seg_result = SegmentalAnalysis().extract(data)
    result = SOTPModel().calculate(seg_result, data)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'intrinsic_value' in result
        assert 'segment_valuations' in result
        segs = result['segment_valuations']
        print(f"        SOTP intrinsic=₹{result['intrinsic_value']:,.2f}, "
              f"segments={len(segs)}, "
              f"upside={result.get('upside_pct', 'N/A')}%")
        for s in segs[:3]:
            print(f"          • {s.get('name', '?')}: "
                  f"EV=₹{s.get('ev', 0):,.0f} Cr")
    else:
        print(f"        SOTP not available: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  6. Forensic Dashboard on real data
# ─────────────────────────────────────────────────────────────────────
def test_forensic_dashboard():
    from quant.forensic_dashboard import ForensicDashboard
    from quant.dcf import DCFModel
    data, _ = get_data()

    cfo_check = DCFModel.cfo_ebitda_check(data)
    analysis = {
        'cfo_ebitda_check': cfo_check,
        'rpt': {'available': False},
        'contingent': {'available': False},
    }
    result = ForensicDashboard().analyze(data, analysis)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'forensic_score' in result
        assert 'quality_rating' in result
        assert 'red_flags' in result
        print(f"        Score={result['forensic_score']}/10, "
              f"rating={result['quality_rating']}, "
              f"red_flags={len(result['red_flags'])}")
    else:
        print(f"        ForensicDashboard: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  7. Piotroski F-Score on real data
# ─────────────────────────────────────────────────────────────────────
def test_piotroski():
    from quant.piotroski import PiotroskiFScore
    data, _ = get_data()
    result = PiotroskiFScore().calculate(data)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'f_score' in result
        assert 0 <= result['f_score'] <= 9
        print(f"        F-Score={result['f_score']}/9, "
              f"interpretation={result.get('interpretation', '?')}")
    else:
        print(f"        F-Score not available: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  8. Beneish M-Score on real data
# ─────────────────────────────────────────────────────────────────────
def test_beneish():
    from quant.forensics import BeneishMScore
    data, _ = get_data()
    result = BeneishMScore().calculate(data)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'm_score' in result
        assert 'risk_level' in result
        print(f"        M-Score={result['m_score']}, "
              f"risk={result['risk_level']}, "
              f"defaults={len(result.get('defaulted_components', []))}")
    else:
        print(f"        M-Score not available: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
#  9. Financial Ratios on real data
# ─────────────────────────────────────────────────────────────────────
def test_ratios():
    from quant.ratios import FinancialRatios
    data, _ = get_data()
    result = FinancialRatios().calculate(data)
    assert isinstance(result, dict)
    pe = result.get('pe')
    roe = result.get('roe')
    print(f"        P/E={pe}, ROE={roe}%, "
          f"Revenue Growth={result.get('revenue_growth', 'N/A')}%, "
          f"D/E={result.get('de', 'N/A')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 10. Text Intelligence on real transcripts
# ─────────────────────────────────────────────────────────────────────
def test_text_intelligence():
    from qualitative.text_intelligence import TextIntelligenceEngine
    _, transcripts = get_data()
    if not transcripts:
        print("        No real transcripts available")
        return 'SKIP'

    result = TextIntelligenceEngine().analyze(concall_texts=transcripts)
    assert isinstance(result, dict)
    assert result.get('available') is True, "TextIntelligence should work on real transcripts"
    assert len(result.get('insights', [])) > 0, "Should extract insights from real transcripts"
    topics = list(result.get('topic_analysis', {}).keys())
    fwd = result.get('forward_looking', [])
    print(f"        Insights={len(result['insights'])}, "
          f"Topics={topics[:5]}, "
          f"Forward-looking={len(fwd)}, "
          f"Tone={result.get('overall_tone', 'N/A')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 11. Say-Do Tracker on real transcripts + real data
# ─────────────────────────────────────────────────────────────────────
def test_say_do_tracker():
    from qualitative.say_do_tracker import SayDoTracker
    data, transcripts = get_data()
    if len(transcripts) < 2:
        print("        Need ≥2 real transcripts for Say-Do")
        return 'SKIP'

    result = SayDoTracker().analyze(concall_texts=transcripts, data=data)
    assert isinstance(result, dict)
    if result.get('available'):
        print(f"        Say-Do={result.get('say_do_ratio', 'N/A')}, "
              f"credibility={result.get('credibility_rating', 'N/A')}, "
              f"matched={result.get('num_matched', '?')}")
    else:
        print(f"        Say-Do: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 12. Cross Validator with real AR PDF (if available)
# ─────────────────────────────────────────────────────────────────────
def test_cross_validator():
    from compliance.cross_validator import CrossValidator
    from data.pdf_parser import PDFParser
    data, _ = get_data()

    report_dir = os.path.join('.', 'output', 'reports')
    ar_files = sorted(
        [f for f in os.listdir(report_dir) if f.endswith('.pdf')]
    ) if os.path.isdir(report_dir) else []

    if not ar_files:
        print("        No real AR PDFs found in output/reports/")
        return 'SKIP'

    ar_path = os.path.join(report_dir, ar_files[-1])
    print(f"        Parsing real AR: {ar_files[-1]} …")
    ar_parsed = PDFParser().parse(ar_path)

    result = CrossValidator().validate(data, ar_parsed)
    assert isinstance(result, dict)
    if result.get('available'):
        assert 'trust_score' in result
        checks = result.get('checks', [])
        matched = sum(1 for c in checks if c.get('status') == 'MATCH')
        print(f"        Trust={result['trust_score']}, "
              f"label={result.get('trust_label', 'N/A')}, "
              f"matched={matched}/{len(checks)}")
    else:
        print(f"        CrossValidator: {result.get('reason', 'N/A')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 13. Macro Correlation Engine with real prices
# ─────────────────────────────────────────────────────────────────────
def test_macro_engine():
    from predictive.macro_engine import MacroCorrelationEngine
    data, _ = get_data()
    engine = MacroCorrelationEngine()

    # SECTOR_SENSITIVITY was removed — sensitivities are now computed from data
    assert hasattr(engine, 'analyze')

    price_df = data.get('price', pd.DataFrame())
    if price_df.empty or 'close' not in price_df.columns:
        print("        No real price data for macro correlation")
        return 'SKIP'

    stock_prices = price_df['close'].dropna()
    if len(stock_prices) < 60:
        print(f"        Only {len(stock_prices)} price points (need ≥60)")
        return 'SKIP'

    sector = data.get('sector', 'energy')
    symbol = data.get('symbol', 'RELIANCE')
    result = engine.analyze(
        symbol=symbol, stock_prices=stock_prices, sector=sector)
    assert isinstance(result, dict)
    if result.get('available'):
        signals = result.get('signals', [])
        print(f"        Macro available, sector={result.get('sector')}, "
              f"signals={len(signals)}")
        for s in signals[:3]:
            print(f"          • {s}")
    else:
        print(f"        Macro: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 14. Realtime Feeds — live data
# ─────────────────────────────────────────────────────────────────────
def test_realtime_feeds():
    from data.realtime_feeds import RealtimeFeeds
    feeds = RealtimeFeeds()
    if not feeds.available:
        print("        yfinance not available")
        return 'SKIP'

    macro = feeds.macro_indicators()
    assert isinstance(macro, dict)
    assert macro.get('available') is True
    print(f"        Crude=${macro.get('crude_oil_usd')}, "
          f"USD/INR={macro.get('usdinr')}, "
          f"Gold=${macro.get('gold_usd')}, "
          f"Nifty={macro.get('nifty50')}")

    symbol = (_LIVE_DATA or {}).get('symbol', 'RELIANCE')
    beta = feeds.estimate_beta(symbol)
    if beta.get('available'):
        print(f"        Beta={beta['beta']}, "
              f"R²={beta.get('r_squared')}, "
              f"pts={beta.get('data_points')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 15. Layout-Aware Parser on real AR PDF
# ─────────────────────────────────────────────────────────────────────
def test_layout_parser():
    from data.layout_parser import LayoutAwareParser
    parser = LayoutAwareParser()

    if not parser.available:
        print("        pdfplumber not installed")
        return 'SKIP'

    report_dir = os.path.join('.', 'output', 'reports')
    ar_files = sorted(
        [f for f in os.listdir(report_dir) if f.endswith('.pdf')]
    ) if os.path.isdir(report_dir) else []

    if not ar_files:
        print("        No real AR PDFs in output/reports/")
        return 'SKIP'

    ar_path = os.path.join(report_dir, ar_files[-1])
    print(f"        Parsing tables from {ar_files[-1]} …")
    result = parser.extract_tables(ar_path, table_type='segmental', max_pages=30)
    assert isinstance(result, dict)
    tables = result.get('extracted_tables', [])
    print(f"        Extracted {len(tables)} segmental tables")
    return True


# ─────────────────────────────────────────────────────────────────────
# 16. Trend Analyzer on real data
# ─────────────────────────────────────────────────────────────────────
def test_trend_analyzer():
    from quant.trend_analyzer import TrendAnalyzer
    data, _ = get_data()
    result = TrendAnalyzer().analyze(data)
    assert isinstance(result, dict)
    if result.get('available'):
        print(f"        Health={result.get('health_score')}/10, "
              f"direction={result.get('overall_direction')}")
    else:
        print(f"        Trends: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 17. Technical Analyzer on real price data
# ─────────────────────────────────────────────────────────────────────
def test_technicals():
    from quant.technicals import TechnicalAnalyzer
    data, _ = get_data()
    price_df = data.get('price', pd.DataFrame())
    if price_df.empty:
        print("        No real price data")
        return 'SKIP'

    result = TechnicalAnalyzer().analyze(price_df)
    assert isinstance(result, dict)
    if result.get('available'):
        sig = result.get('overall_signal', {})
        print(f"        Signal={sig.get('signal', '?')}, "
              f"RSI={result.get('rsi', {}).get('value', '?')}")
    else:
        print(f"        Technicals: {result.get('reason', '?')}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 18. Synthesis Agent on real analysis results
# ─────────────────────────────────────────────────────────────────────
def test_synthesis_agent():
    """Run synthesis on real analysis results from all modules."""
    from quant.dcf import DCFModel
    from quant.forensics import BeneishMScore
    from quant.piotroski import PiotroskiFScore
    from quant.ratios import FinancialRatios
    from quant.trend_analyzer import TrendAnalyzer
    from quant.technicals import TechnicalAnalyzer
    from quant.forensic_dashboard import ForensicDashboard
    from qualitative.text_intelligence import TextIntelligenceEngine
    from qualitative.say_do_tracker import SayDoTracker
    from agents.synthesis_agent import SynthesisAgent

    data, transcripts = get_data()
    agent = SynthesisAgent()
    assert agent.available is True

    dcf = DCFModel().calculate(data)
    cfo_check = DCFModel.cfo_ebitda_check(data)
    fscore = PiotroskiFScore().calculate(data)
    mscore = BeneishMScore().calculate(data)
    ratios = FinancialRatios().calculate(data)
    trends = TrendAnalyzer().analyze(data)

    price_df = data.get('price', pd.DataFrame())
    technicals = TechnicalAnalyzer().analyze(price_df) if not price_df.empty else {'available': False}

    forensic = ForensicDashboard().analyze(data, {
        'cfo_ebitda_check': cfo_check,
        'rpt': {'available': False},
        'contingent': {'available': False},
    })

    text_intel = TextIntelligenceEngine().analyze(
        concall_texts=transcripts) if transcripts else {'available': False}
    say_do = SayDoTracker().analyze(
        concall_texts=transcripts, data=data) if len(transcripts) >= 2 else {'available': False}

    analysis = {
        'dcf': dcf,
        'fscore': fscore,
        'mscore': mscore,
        'ratios': ratios,
        'trends': trends,
        'technicals': technicals,
        'cfo_ebitda_check': cfo_check,
        'forensic_dashboard': forensic,
        'text_intel': text_intel,
        'say_do': say_do,
        'sotp': {'available': False},
        'peer_cca': {'available': False},
        'sentiment': {'available': False},
        'prediction': {'available': False},
        'governance': {'available': False},
        'moat': {'available': False},
        'esg': {'available': False},
        'macro_corr': {'available': False},
    }
    result = agent.run(analysis)
    assert isinstance(result, dict)
    assert 'recommendation' in result
    assert 'score' in result
    assert 'max_score' in result
    assert result['max_score'] > 0, "Should have data from real modules"

    pct = result['score'] / result['max_score'] if result['max_score'] > 0 else 0
    print(f"        Rating: {result['recommendation']}, "
          f"score={result['score']}/{result['max_score']} ({pct:.0%}), "
          f"confidence={result['confidence']}")
    print(f"        Thesis ({len(result['thesis'])} points):")
    for t in result['thesis'][:5]:
        print(f"          • {t}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 19. Synthesis Agent — edge case: empty analysis → HOLD
# ─────────────────────────────────────────────────────────────────────
def test_synthesis_empty():
    from agents.synthesis_agent import SynthesisAgent
    result = SynthesisAgent().run({})
    assert result['recommendation'] == 'HOLD ⏸️'
    assert result['max_score'] == 0
    assert result['confidence'] == 'LOW'
    return True


# ─────────────────────────────────────────────────────────────────────
# 20. Report Generator — full report on real data
# ─────────────────────────────────────────────────────────────────────
def test_report_generator():
    from quant.dcf import DCFModel
    from quant.forensics import BeneishMScore
    from quant.piotroski import PiotroskiFScore
    from quant.ratios import FinancialRatios
    from agents.synthesis_agent import SynthesisAgent
    from reports.generator import ReportGenerator

    data, transcripts = get_data()
    symbol = data.get('symbol', 'TEST')

    dcf = DCFModel().calculate(data)
    fscore = PiotroskiFScore().calculate(data)
    mscore = BeneishMScore().calculate(data)
    ratios = FinancialRatios().calculate(data)

    analysis = {
        'dcf': dcf,
        'fscore': fscore,
        'mscore': mscore,
        'ratios': ratios,
        'trends': {'available': False},
        'technicals': {'available': False},
        'cfo_ebitda_check': DCFModel.cfo_ebitda_check(data),
        'forensic_dashboard': {'available': False},
        'text_intel': {'available': False},
        'say_do': {'available': False},
        'sotp': {'available': False},
        'peer_cca': {'available': False},
        'sentiment': {'available': False},
        'prediction': {'available': False},
        'governance': {'available': False},
        'moat': {'available': False},
        'esg': {'available': False},
        'macro_corr': {'available': False},
        'cross_validation': {'available': False},
        'segmental': {'available': False},
        'shareholding': {},
    }
    rating = SynthesisAgent().run(analysis)
    analysis['rating'] = rating

    md = ReportGenerator().generate(symbol, data, analysis)
    assert isinstance(md, str)
    assert len(md) > 500, f"Report too short ({len(md)} chars)"
    assert symbol in md
    print(f"        Report: {len(md):,} chars, "
          f"{md.count(chr(10)):,} lines, "
          f"rating={rating['recommendation']}")
    return True


# ─────────────────────────────────────────────────────────────────────
# 21. Report Generator — save to disk
# ─────────────────────────────────────────────────────────────────────
def test_report_save():
    from quant.ratios import FinancialRatios
    from agents.synthesis_agent import SynthesisAgent
    from reports.generator import ReportGenerator

    data, _ = get_data()
    symbol = data.get('symbol', 'TEST')
    ratios = FinancialRatios().calculate(data)
    analysis = {
        'ratios': ratios,
        'rating': SynthesisAgent().run({'ratios': ratios}),
        'dcf': {'available': False},
        'mscore': {'available': False},
        'fscore': {'available': False},
    }
    md = ReportGenerator().generate(symbol, data, analysis)
    with tempfile.TemporaryDirectory() as tmpdir:
        path = ReportGenerator().save(md, symbol, tmpdir)
        assert os.path.exists(path), f"File not written: {path}"
        sz = os.path.getsize(path)
        assert sz > 100
        print(f"        Saved {os.path.basename(path)} ({sz:,} bytes)")
    return True


# ─────────────────────────────────────────────────────────────────────
# 22. PDF Exporter on real markdown
# ─────────────────────────────────────────────────────────────────────
def test_pdf_exporter():
    from reports.pdf_exporter import export_markdown_to_pdf
    from reports.generator import ReportGenerator
    from agents.synthesis_agent import SynthesisAgent

    data, _ = get_data()
    symbol = data.get('symbol', 'TEST')
    analysis = {
        'ratios': {},
        'rating': SynthesisAgent().run({}),
        'dcf': {'available': False},
        'mscore': {'available': False},
        'fscore': {'available': False},
    }
    md = ReportGenerator().generate(symbol, data, analysis)

    with tempfile.TemporaryDirectory() as tmpdir:
        md_path = os.path.join(tmpdir, f'{symbol}_Research.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md)
        result = export_markdown_to_pdf(md_path, symbol, tmpdir)
        if result:
            sz = os.path.getsize(result)
            print(f"        PDF: {os.path.basename(result)} ({sz:,} bytes)")
            assert sz > 500
        else:
            print("        PDF: weasyprint/fpdf2 not available")
            return 'SKIP'
    return True


# ─────────────────────────────────────────────────────────────────────
# 23. Orchestrator instantiation
# ─────────────────────────────────────────────────────────────────────
def test_orchestrator_init():
    from agents.orchestrator import Orchestrator
    orch = Orchestrator()
    assert hasattr(orch, 'dcf_model')
    assert hasattr(orch, 'forensic_dash')
    assert hasattr(orch, 'sotp_model')
    assert hasattr(orch, 'text_intel')
    assert hasattr(orch, 'say_do_tracker')
    assert hasattr(orch, 'cross_validator')
    assert hasattr(orch, 'macro_engine')
    assert hasattr(orch, 'feeds')
    assert hasattr(orch, 'layout_parser')
    assert hasattr(orch, 'reporter')
    assert hasattr(orch, 'synthesis')
    attrs = [a for a in dir(orch) if not a.startswith('_')]
    print(f"        Orchestrator: {len(attrs)} public attributes")
    return True


# ─────────────────────────────────────────────────────────────────────
# 24. End-to-end: real data → all modules → synthesis → report
# ─────────────────────────────────────────────────────────────────────
def test_end_to_end():
    """Full pipeline on real data — every module fed real inputs."""
    from quant.dcf import DCFModel
    from quant.forensics import BeneishMScore
    from quant.piotroski import PiotroskiFScore
    from quant.ratios import FinancialRatios
    from quant.forensic_dashboard import ForensicDashboard
    from quant.trend_analyzer import TrendAnalyzer
    from quant.technicals import TechnicalAnalyzer
    from qualitative.text_intelligence import TextIntelligenceEngine
    from qualitative.say_do_tracker import SayDoTracker
    from agents.synthesis_agent import SynthesisAgent
    from reports.generator import ReportGenerator

    data, transcripts = get_data()
    symbol = data.get('symbol', 'TEST')

    dcf = DCFModel().calculate(data)
    cfo_check = DCFModel.cfo_ebitda_check(data)
    fscore = PiotroskiFScore().calculate(data)
    mscore = BeneishMScore().calculate(data)
    ratios = FinancialRatios().calculate(data)
    trends = TrendAnalyzer().analyze(data)

    price_df = data.get('price', pd.DataFrame())
    technicals = TechnicalAnalyzer().analyze(price_df) if not price_df.empty else {'available': False}

    forensic = ForensicDashboard().analyze(data, {
        'cfo_ebitda_check': cfo_check,
        'rpt': {'available': False},
        'contingent': {'available': False},
    })

    text_intel = (TextIntelligenceEngine().analyze(concall_texts=transcripts)
                  if transcripts else {'available': False})
    say_do = (SayDoTracker().analyze(concall_texts=transcripts, data=data)
              if len(transcripts) >= 2 else {'available': False})

    analysis = {
        'dcf': dcf, 'fscore': fscore, 'mscore': mscore,
        'ratios': ratios, 'trends': trends, 'technicals': technicals,
        'cfo_ebitda_check': cfo_check, 'forensic_dashboard': forensic,
        'text_intel': text_intel, 'say_do': say_do,
        'sotp': {'available': False}, 'peer_cca': {'available': False},
        'sentiment': {'available': False}, 'prediction': {'available': False},
        'governance': {'available': False}, 'moat': {'available': False},
        'esg': {'available': False}, 'macro_corr': {'available': False},
        'cross_validation': {'available': False}, 'segmental': {'available': False},
        'shareholding': {},
    }
    rating = SynthesisAgent().run(analysis)
    analysis['rating'] = rating

    report = ReportGenerator().generate(symbol, data, analysis)
    assert len(report) > 1000, "Report too short"
    assert rating['max_score'] > 0, "Real data should give max_score > 0"

    available_modules = sum(1 for k, v in analysis.items()
                            if isinstance(v, dict) and v.get('available'))
    pct = rating['score'] / rating['max_score']
    print(f"        E2E: {rating['recommendation']} "
          f"({rating['score']}/{rating['max_score']} = {pct:.0%}), "
          f"confidence={rating['confidence']}, "
          f"modules_available={available_modules}, "
          f"report={len(report):,} chars")
    return True


# ─────────────────────────────────────────────────────────────────────
# 25. Cross-module consistency check
# ─────────────────────────────────────────────────────────────────────
def test_cross_module_consistency():
    """Verify field-map covers all DCF/forensic needs + synthesis graceful skip."""
    from data.preprocessing import DataPreprocessor
    from agents.synthesis_agent import SynthesisAgent

    pp = DataPreprocessor()
    field_map = pp.FIELD_MAP
    assert 'operating_cf' in field_map
    assert 'sales' in field_map
    assert 'net_profit' in field_map
    assert 'borrowings' in field_map

    minimal = {k: {'available': False} for k in [
        'dcf', 'sotp', 'fscore', 'mscore', 'trends', 'technicals',
        'peer_cca', 'sentiment', 'prediction', 'cfo_ebitda_check',
        'governance', 'moat', 'esg', 'text_intel', 'forensic_dashboard',
        'say_do', 'macro_corr',
    ]}
    minimal['ratios'] = {}
    result = SynthesisAgent().run(minimal)
    assert result['recommendation'] == 'HOLD ⏸️'
    print(f"        All-unavailable → {result['recommendation']} ✓")
    return True


# =====================================================================
#  Runner
# =====================================================================

def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'RELIANCE'

    print("\n" + "=" * 65)
    print(f"  Integration Test Suite — 100% REAL DATA ({symbol})")
    print("  ZERO synthetic data — everything live-scraped")
    print("=" * 65)

    _load_live_data(symbol)

    tests = [
        ("Config singleton & defaults", test_config),
        ("DataPreprocessor (real scraped data)", test_preprocessor),
        ("DCF Model (real financials)", test_dcf),
        ("CFO/EBITDA Check (real cash flow)", test_cfo_ebitda),
        ("SOTP Model (real segmental data)", test_sotp),
        ("Forensic Dashboard (real P&L/BS/CF)", test_forensic_dashboard),
        ("Piotroski F-Score (real financials)", test_piotroski),
        ("Beneish M-Score (real financials)", test_beneish),
        ("Financial Ratios (real data)", test_ratios),
        ("Text Intelligence (real transcripts)", test_text_intelligence),
        ("Say-Do Tracker (real transcripts)", test_say_do_tracker),
        ("Cross Validator (real AR PDF)", test_cross_validator),
        ("Macro Correlation (real prices + yfinance)", test_macro_engine),
        ("Realtime Feeds (live macro + beta)", test_realtime_feeds),
        ("Layout-Aware Parser (real AR PDF)", test_layout_parser),
        ("Trend Analyzer (real 10Y data)", test_trend_analyzer),
        ("Technical Analyzer (real prices)", test_technicals),
        ("Synthesis Agent (real analysis)", test_synthesis_agent),
        ("Synthesis Agent — empty → HOLD", test_synthesis_empty),
        ("Report Generator (real data → MD)", test_report_generator),
        ("Report Generator — save to disk", test_report_save),
        ("PDF Exporter (real report → PDF)", test_pdf_exporter),
        ("Orchestrator instantiation", test_orchestrator_init),
        ("End-to-end (real data → full pipeline)", test_end_to_end),
        ("Cross-module consistency", test_cross_module_consistency),
    ]

    for label, fn in tests:
        _run(label, fn)

    print("\n" + "─" * 65)
    total = PASS + FAIL + SKIP
    print(f"  Results:  {PASS} passed  |  {FAIL} failed  |  {SKIP} skipped  "
          f"({total} total)")
    print("─" * 65 + "\n")

    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
