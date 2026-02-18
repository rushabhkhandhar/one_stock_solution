"""
Pipeline Orchestrator
=====================
Coordinates all analysis modules in a clean, unified pipeline:

  Phase 1   → Data Ingestion & Preprocessing
  Phase 2   → Core Quantitative Analysis (Ratios, DCF, M-Score, F-Score)
  Phase 2.5 → Annual Report Download & Validation
  Phase 2.6 → Layout-Aware Table Extraction (BRSR, Segmental)
  Phase 3   → Extended Quant (CFO/EBITDA, Peer CCA, 5Y Trends)
  Phase 3.5 → Forensic Deep Dive (RPT, Contingent, Auditor)
  Phase 3.6 → Segmental + SOTP Valuation + Governance + ESG
  Phase 3.9 → Forensic Dashboard (Unified Earnings Quality)
  Phase 4   → Qualitative Intelligence (Sentiment, RAG, Text Intel, Moat)
  Phase 4.6 → Text Intelligence Engine
  Phase 4.7 → Say-Do Ratio (Management Credibility)
  Phase 5   → Technical & Predictive (Technicals, ARIMA, Macro-ARDL)
  Phase 6   → Synthesis (Buy / Hold / Sell rating)
  Phase 7   → Report Generation + PDF Export

No LLM / API keys required — runs entirely on scraped data + maths.
"""
import pandas as pd
from data.ingestion import DataIngestion
from data.preprocessing import DataPreprocessor
from data.report_downloader import ReportDownloader
from data.pdf_parser import PDFParser
from data.realtime_feeds import RealtimeFeeds
from quant.dcf import DCFModel
from quant.forensics import BeneishMScore
from quant.piotroski import PiotroskiFScore
from quant.ratios import FinancialRatios
from quant.peer_comparables import PeerComparables
from quant.forensic_extras import ForensicExtras
from quant.governance import GovernanceDashboard
from quant.segmental import SegmentalAnalysis
from quant.esg_brsr import ESGAnalyzer
from quant.trend_analyzer import TrendAnalyzer
from quant.technicals import TechnicalAnalyzer
from quant.sotp import SOTPModel
from quant.forensic_dashboard import ForensicDashboard
from predictive.arima_ets import HybridPredictor
from predictive.flow_correlation import FlowCorrelation
from predictive.macro_engine import MacroCorrelationEngine
from agents.rag_agent import RAGAgent
from agents.synthesis_agent import SynthesisAgent
from compliance.cross_validator import CrossValidator
from compliance.safety import KillSwitch, stamp_source
from qualitative.moat_identifier import MoatIdentifier
from qualitative.text_intelligence import TextIntelligenceEngine
from qualitative.say_do_tracker import SayDoTracker
from data.layout_parser import LayoutAwareParser
from reports.generator import ReportGenerator
from config import config


class Orchestrator:

    def __init__(self):
        self.ingestion        = DataIngestion()
        self.preprocessor     = DataPreprocessor()
        self.report_dl        = ReportDownloader()
        self.pdf_parser       = PDFParser()
        self.dcf_model        = DCFModel()
        self.mscore_model     = BeneishMScore()
        self.fscore_model     = PiotroskiFScore()
        self.ratios_calc      = FinancialRatios()
        self.peer_model       = PeerComparables()
        self.forensic_extras  = ForensicExtras()
        self.governance       = GovernanceDashboard()
        self.segmental        = SegmentalAnalysis()
        self.esg_analyzer     = ESGAnalyzer()
        self.moat_identifier  = MoatIdentifier()
        self.trend_analyzer   = TrendAnalyzer()
        self.technical        = TechnicalAnalyzer()
        self.text_intel       = TextIntelligenceEngine()
        self.sotp_model       = SOTPModel()
        self.forensic_dash    = ForensicDashboard()
        self.say_do_tracker   = SayDoTracker()
        self.macro_engine     = MacroCorrelationEngine()
        self.layout_parser    = LayoutAwareParser()
        self.predictor        = HybridPredictor()
        self.flow_corr        = FlowCorrelation()
        self.rag_agent        = RAGAgent()
        self.synthesis        = SynthesisAgent()
        self.cross_validator  = CrossValidator()
        self.kill_switch      = KillSwitch()
        self.feeds            = RealtimeFeeds()
        self.reporter         = ReportGenerator()

    # ==================================================================
    # Main entry point
    # ==================================================================
    def analyze(self, stock_name: str) -> str:
        """
        Full analysis pipeline.

        Parameters
        ----------
        stock_name : str
            Company name or BSE symbol (e.g. "TCS", "RELIANCE", "HDFC").

        Returns
        -------
        str : absolute path to the generated Markdown report.
        """

        # ── Phase 1: Data Ingestion ──────────────────────────
        print("\n📥  PHASE 1 — Data Ingestion & Preprocessing")
        data = self.ingestion.load_company(stock_name,
                                           consolidated=config.consolidated)
        data = self.preprocessor.clean(data)
        data = self.preprocessor.compute_derived(data)

        # ── Kill Switch Check ────────────────────────────────
        if not self.kill_switch.check(data):
            print(f"  🛑 KILL SWITCH triggered: {self.kill_switch.reason}")
            print("  Proceeding with warnings …")

        # ── Phase 2: Core Quantitative Analysis ──────────────
        print("\n🔢  PHASE 2 — Core Quantitative Analysis")
        analysis = {}

        print("  ▸ Financial Ratios …")
        analysis['ratios'] = self.ratios_calc.calculate(data)

        print("  ▸ DCF Valuation …")
        analysis['dcf'] = self.dcf_model.calculate(data)

        print("  ▸ Beneish M-Score …")
        analysis['mscore'] = self.mscore_model.calculate(data)

        print("  ▸ Piotroski F-Score …")
        analysis['fscore'] = self.fscore_model.calculate(data)

        # Shareholding summary
        analysis['shareholding'] = self._summarize_shareholding(data)

        # ── Phase 2.5: Annual Report Validation ──────────────
        print("\n📑  PHASE 2.5 — Annual Report Download & Validation")
        ar_reports = data.get('annual_reports', {})
        if ar_reports:
            print("  ▸ Downloading annual reports …")
            downloaded = self.report_dl.download_reports(
                data.get('symbol', stock_name), ar_reports, latest_n=2)
            data['downloaded_reports'] = downloaded

            if downloaded:
                latest_pdf = downloaded[0]['path']
                print("  ▸ Parsing annual report …")
                ar_parsed = self.pdf_parser.parse(
                    latest_pdf, consolidated=config.consolidated)
                analysis['ar_parsed'] = ar_parsed

                # Extract FY year from filename (e.g. AXISCADES_AR_2025.pdf → 2025)
                import re as _re
                _yr_match = _re.search(r'_AR_(\d{4})', latest_pdf)
                ar_year = int(_yr_match.group(1)) if _yr_match else None

                print("  ▸ Cross-validating numbers …")
                validation = self.cross_validator.validate(
                    data, ar_parsed, ar_year=ar_year)
                analysis['validation'] = validation

                ts = validation.get('trust_score')
                tl = validation.get('trust_label', '')
                if ts is not None:
                    print(f"  ✔ Trust Score: {ts}/100 — {tl}")
                else:
                    print(f"  ⚠ Trust Score: N/A — {tl}")
            else:
                analysis['ar_parsed'] = {'available': False}
                analysis['validation'] = {'available': False,
                                          'reason': 'No reports downloaded'}
        else:
            print("  ⚠ No annual report links found.")
            analysis['ar_parsed'] = {'available': False}
            analysis['validation'] = {'available': False,
                                      'reason': 'No annual report links'}

        # ── Phase 2.6: Layout-Aware Table Extraction ─────────
        print("\n📐  PHASE 2.6 — Layout-Aware Table Extraction")
        try:
            downloaded = data.get('downloaded_reports', [])
            if downloaded:
                pdf_path = downloaded[0].get('path')
                if pdf_path:
                    print("  ▸ Extracting structured tables …")
                    layout_tables = self.layout_parser.extract_facts(
                        pdf_path, table_type='all')
                    analysis['layout_tables'] = layout_tables
                    if layout_tables.get('available'):
                        print(f"  ✔ Extracted {layout_tables.get('num_facts', 0)} "
                              f"facts from {layout_tables.get('num_tables', 0)} tables")
                    else:
                        print(f"  ⚠ Layout extraction: "
                              f"{layout_tables.get('reason', 'N/A')}")

                    # BRSR-specific extraction (skip if layout already got facts)
                    if layout_tables.get('num_facts', 0) < 50:
                        brsr_tables = self.layout_parser.extract_brsr_metrics(pdf_path)
                        analysis['brsr_tables'] = brsr_tables
                        if brsr_tables.get('available') and brsr_tables.get('metrics'):
                            print(f"  ✔ BRSR metrics: {brsr_tables.get('num_metrics', 0)} found")
                    else:
                        analysis['brsr_tables'] = {'available': False,
                                                   'reason': 'Covered by general extraction'}

                    # Segmental extraction via layout parser (skip if layout got facts)
                    if layout_tables.get('num_facts', 0) < 50:
                        seg_layout = self.layout_parser.extract_segmental(pdf_path)
                        analysis['segmental_layout'] = seg_layout
                        if seg_layout.get('available') and seg_layout.get('segments'):
                            print(f"  ✔ Layout segments: "
                                  f"{seg_layout.get('num_segments', 0)} detected")
                    else:
                        analysis['segmental_layout'] = {'available': False,
                                                        'reason': 'Covered by general extraction'}
            else:
                analysis['layout_tables'] = {'available': False,
                                             'reason': 'No reports downloaded'}
        except Exception as e:
            print(f"  ⚠ Layout extraction error: {e}")
            analysis['layout_tables'] = {'available': False, 'reason': str(e)}

        # ── Phase 3: Extended Quant ──────────────────────────
        print("\n📊  PHASE 3 — Extended Quantitative Analysis")

        # WACC Sensitivity (already computed inside dcf_model.calculate())

        # CFO / EBITDA quality check
        print("  ▸ CFO / EBITDA Quality Check …")
        analysis['cfo_ebitda_check'] = self.dcf_model.cfo_ebitda_check(data)
        cfo_res = analysis['cfo_ebitda_check']
        if cfo_res.get('available') and cfo_res.get('is_red_flag'):
            print(f"  ⚠ CFO/EBITDA = {cfo_res.get('ratio', 'N/A')}% — RED FLAG")
        elif cfo_res.get('available'):
            print(f"  ✔ CFO/EBITDA = {cfo_res.get('ratio', 'N/A')}% — Healthy")

        # Peer Comparable Analysis (enhanced with mcap)
        print("  ▸ Peer Comparable Analysis …")
        symbol = data.get('symbol', stock_name)
        pe = analysis.get('ratios', {}).get('pe_ratio')
        ev_ebitda = None  # yfinance provides this via peer_model
        try:
            analysis['peer_cca'] = self.peer_model.analyze(symbol, pe, ev_ebitda)
        except Exception as e:
            analysis['peer_cca'] = {'available': False, 'reason': str(e)}
        if analysis['peer_cca'].get('available'):
            assessments = analysis['peer_cca'].get('assessment', [])
            verdict = assessments[0] if assessments else 'No assessment'
            print(f"  ✔ Peer CCA: {verdict}")
            mcap_tier = analysis['peer_cca'].get('stock_mcap_tier', '')
            if mcap_tier:
                print(f"    Market Cap Tier: {mcap_tier}")

        # 5-Year Trend Analysis
        print("  ▸ 5-Year Trend Analysis …")
        try:
            analysis['trends'] = self.trend_analyzer.analyze(data)
            tr = analysis['trends']
            if tr.get('available'):
                print(f"  ✔ Trends: {tr.get('overall_direction')} "
                      f"(health {tr.get('health_score')}/10, "
                      f"{tr.get('num_years', 0)}Y data)")
            else:
                print(f"  ⚠ Trends: {tr.get('reason', 'Not available')}")
        except Exception as e:
            analysis['trends'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.5: Forensic Extras (RPT, Contingent, Auditor) ────
        print("\n🔬  PHASE 3.5 — Forensic Deep Dive")
        ar_parsed = analysis.get('ar_parsed', {})

        # RPT Structured Analysis
        print("  ▸ Related Party Transactions …")
        try:
            analysis['rpt'] = self.forensic_extras.extract_rpt(ar_parsed, data)
            rpt = analysis['rpt']
            if rpt.get('available'):
                print(f"  ✔ RPT: {rpt.get('flag', 'Analyzed')}")
            else:
                print(f"  ⚠ RPT: {rpt.get('reason', 'Not available')}")
        except Exception as e:
            analysis['rpt'] = {'available': False, 'reason': str(e)}

        # Contingent Liabilities Analysis
        print("  ▸ Contingent Liabilities …")
        try:
            analysis['contingent'] = self.forensic_extras.analyze_contingent(
                ar_parsed, data)
            cl = analysis['contingent']
            if cl.get('available'):
                print(f"  ✔ Contingent: {cl.get('detail', 'Analyzed')}")
        except Exception as e:
            analysis['contingent'] = {'available': False, 'reason': str(e)}

        # Auditor Red Flags
        print("  ▸ Auditor Observations …")
        try:
            analysis['auditor_analysis'] = self.forensic_extras.summarize_auditor_flags(
                ar_parsed)
            aud = analysis['auditor_analysis']
            if aud.get('available'):
                print(f"  ✔ Auditor: {aud.get('summary', 'Analyzed')}")
        except Exception as e:
            analysis['auditor_analysis'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.6: Segmental Performance ─────────────────
        print("\n📊  PHASE 3.6 — Segmental Performance & SOTP Valuation")
        try:
            analysis['segmental'] = self.segmental.extract(ar_parsed)
            seg = analysis['segmental']
            if seg.get('available') and seg.get('segments'):
                print(f"  ✔ {seg.get('num_segments', 0)} segments detected "
                      f"| Dominant: {seg.get('dominant_segment', 'N/A')} "
                      f"({seg.get('dominant_pct', 0)}%)")

                # SOTP Valuation (for multi-segment companies)
                if seg.get('num_segments', 0) >= 2:
                    print("  ▸ SOTP Valuation (Sum-of-the-Parts) …")
                    try:
                        analysis['sotp'] = self.sotp_model.calculate(
                            seg, data, analysis.get('peer_cca'))
                        sotp = analysis['sotp']
                        if sotp.get('available'):
                            iv = sotp.get('intrinsic_value')
                            up = sotp.get('upside_pct')
                            print(f"  ✔ SOTP IV: ₹{iv:,.2f} "
                                  f"(upside {up:+.1f}%) | "
                                  f"{sotp.get('num_segments_valued', 0)} segments, "
                                  f"{sotp.get('num_distinct_sectors', 0)} sectors")
                        else:
                            print(f"  ⚠ SOTP: {sotp.get('reason', 'N/A')}")
                    except Exception as e:
                        analysis['sotp'] = {'available': False, 'reason': str(e)}
                        print(f"  ⚠ SOTP error: {e}")
                else:
                    analysis['sotp'] = {'available': False,
                                        'reason': 'Single-segment company'}
            else:
                print(f"  ⚠ Segmental: {seg.get('reason', 'Not available')}")
                analysis['sotp'] = {'available': False,
                                    'reason': 'No segmental data'}
        except Exception as e:
            analysis['segmental'] = {'available': False, 'reason': str(e)}
            analysis['sotp'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.7: Governance Dashboard ──────────────────
        print("\n🏛️  PHASE 3.7 — Corporate Governance")
        try:
            analysis['governance'] = self.governance.analyze(ar_parsed, data)
            gov = analysis['governance']
            if gov.get('available'):
                print(f"  ✔ Governance Score: {gov.get('governance_score', 'N/A')}/10")
                for f in gov.get('flags', []):
                    print(f"    ⚠ [{f['severity']}] {f['flag']}")
            else:
                print(f"  ⚠ Governance: {gov.get('reason', 'Not available')}")
        except Exception as e:
            analysis['governance'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.8: ESG / BRSR ───────────────────────────
        print("\n🌱  PHASE 3.8 — ESG / BRSR Intelligence")
        try:
            pdf_path = None
            downloaded = data.get('downloaded_reports', [])
            if downloaded:
                pdf_path = downloaded[0].get('path')
            analysis['esg'] = self.esg_analyzer.analyze(ar_parsed, pdf_path)
            esg = analysis['esg']
            if esg.get('available'):
                print(f"  ✔ ESG Score: {esg.get('esg_score', 'N/A')}/10 "
                      f"| BRSR: {'Found' if esg.get('brsr_found') else 'Not found'}")
                metrics = esg.get('metrics', {})
                if metrics:
                    print(f"    Metrics: {', '.join(metrics.keys())}")
            else:
                print(f"  ⚠ ESG: {esg.get('reason', 'Not available')}")
        except Exception as e:
            analysis['esg'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.9: Forensic Dashboard (Unified) ─────────
        print("\n🔬  PHASE 3.9 — Forensic Earnings Quality Dashboard")
        try:
            analysis['forensic_dashboard'] = self.forensic_dash.analyze(
                data, analysis)
            fd = analysis['forensic_dashboard']
            if fd.get('available'):
                print(f"  ✔ Forensic Score: {fd.get('forensic_score', 0)}/10 "
                      f"({fd.get('quality_rating', 'N/A')}) "
                      f"| {fd.get('num_passed', 0)}/{fd.get('num_checks', 0)} checks passed")
                for rf in fd.get('red_flags', []):
                    print(f"    🔴 [{rf['severity']}] {rf['category']}: "
                          f"{rf['detail'][:80]}")
            else:
                print(f"  ⚠ Forensic Dashboard: {fd.get('reason', 'N/A')}")
        except Exception as e:
            analysis['forensic_dashboard'] = {'available': False, 'reason': str(e)}

        # ── Phase 4: Qualitative Intelligence ────────────────
        print("\n🧠  PHASE 4 — Qualitative Intelligence")
        try:
            qual_data = {
                'concall_texts': data.get('concall_texts', []),
                'announcements': data.get('announcements', []),
            }
            analysis['qualitative'] = self.rag_agent.run(qual_data)
            sent = analysis['qualitative'].get('sentiment', {})
            if sent.get('available'):
                print(f"  ✔ Sentiment: {sent.get('tone', 'N/A')} "
                      f"(score: {sent.get('overall_score', 'N/A')})")
            else:
                print("  ⚠ Sentiment analysis not available (no transcript text)")
        except Exception as e:
            print(f"  ⚠ Qualitative analysis error: {e}")
            analysis['qualitative'] = {'available': False, 'reason': str(e)}

        # Expose sentiment at top level for synthesis
        analysis['sentiment'] = analysis.get('qualitative', {}).get('sentiment', {})

        # ── Phase 4.5: Moat Identification ───────────────────
        print("\n🏰  PHASE 4.5 — Competitive Moat Identification")
        try:
            concall_texts = data.get('concall_texts', [])
            analysis['moat'] = self.moat_identifier.analyze(
                ar_parsed, concall_texts, data)
            moat = analysis['moat']
            if moat.get('available'):
                print(f"  ✔ Moat Score: {moat.get('moat_score', 0)}/10 "
                      f"| Dominant: {moat.get('dominant_moat', 'None')}")
                for adv in moat.get('competitive_advantages', [])[:3]:
                    print(f"    • {adv}")
            else:
                print(f"  ⚠ Moat: {moat.get('reason', 'Not available')}")
        except Exception as e:
            analysis['moat'] = {'available': False, 'reason': str(e)}

        # ── Phase 4.6: Text Intelligence Engine ──────────────
        print("\n📝  PHASE 4.6 — Unified Text Intelligence")
        try:
            concall_texts = data.get('concall_texts', [])
            ar_parsed = analysis.get('ar_parsed', {})
            announcements = data.get('announcements', [])
            analysis['text_intel'] = self.text_intel.analyze(
                concall_texts, ar_parsed, announcements)
            ti = analysis['text_intel']
            if ti.get('available'):
                src = ti.get('source_breakdown', {})
                print(f"  ✔ Analyzed {ti.get('num_sources', 0)} text sources "
                      f"(concall: {src.get('concall', 0)}, "
                      f"AR: {src.get('annual_report', 0)}, "
                      f"ann: {src.get('announcement', 0)})")
                print(f"    Overall tone: {ti.get('overall_tone', 'N/A')}")
                topics = list(ti.get('topic_analysis', {}).keys())
                if topics:
                    print(f"    Key topics: {', '.join(topics[:5])}")
            else:
                print(f"  ⚠ Text Intel: {ti.get('reason', 'Not available')}")
        except Exception as e:
            analysis['text_intel'] = {'available': False, 'reason': str(e)}

        # ── Phase 4.7: Say-Do Ratio (Management Credibility) ─
        print("\n🤝  PHASE 4.7 — Say-Do Ratio (Management Credibility)")
        try:
            concall_texts_sd = data.get('concall_texts', [])
            analysis['say_do'] = self.say_do_tracker.analyze(
                concall_texts_sd, data)
            sd = analysis['say_do']
            if sd.get('available'):
                print(f"  ✔ Say-Do Ratio: {sd.get('say_do_ratio', 'N/A'):.2f} "
                      f"({sd.get('credibility_rating', 'N/A')}) "
                      f"| {sd.get('num_promises_tracked', 0)} promises tracked")
                if sd.get('is_governance_risk'):
                    print("    🔴 GOVERNANCE RISK — management credibility below threshold")
            else:
                print(f"  ⚠ Say-Do: {sd.get('reason', 'Not available')}")
        except Exception as e:
            analysis['say_do'] = {'available': False, 'reason': str(e)}

        # ── Phase 5: Technical & Predictive ─────────────────
        print("\n📈  PHASE 5 — Technical & Predictive Analysis")
        try:
            # Get stock price history
            bse_symbol = data.get('symbol', stock_name)
            price_hist = self.feeds.stock_history(bse_symbol, period='2y')

            # ── Technical Analysis (new) ──
            if price_hist is not None and len(price_hist) > 30:
                print("  ▸ Technical Indicators …")
                analysis['technicals'] = self.technical.analyze(
                    price_hist, bse_symbol)
                tech = analysis['technicals']
                if tech.get('available'):
                    sig = tech.get('overall_signal', {})
                    print(f"  ✔ Technical Signal: {sig.get('signal', 'N/A')} "
                          f"(conf: {sig.get('confidence', 'N/A')})")
                    mom = tech.get('momentum', {})
                    if mom.get('rsi'):
                        print(f"    RSI: {mom['rsi']} ({mom.get('rsi_signal', '')})")
                    vol = tech.get('volume_analysis', {})
                    if vol.get('obv_trend'):
                        print(f"    Volume: {vol['obv_trend']} | "
                              f"Rel. Vol: {vol.get('relative_volume', 'N/A')}x")
            else:
                analysis['technicals'] = {'available': False,
                                          'reason': 'Insufficient price data'}

            # ── Predictive Model ──
            if price_hist is not None and len(price_hist) > 60:
                close_col = 'close' if 'close' in price_hist.columns else \
                    ('Close' if 'Close' in price_hist.columns else price_hist.columns[0])
                close_series = price_hist[close_col].dropna()

                print("  ▸ Training ARIMA-ETS + GARCH ensemble …")
                train_result = self.predictor.train(close_series)
                if train_result.get('available'):
                    garch_info = ''
                    gm = train_result.get('garch_model', 'N/A')
                    if gm and gm != 'N/A':
                        garch_info = f", {gm} AIC={train_result.get('garch_aic')}"
                    print(f"  ✔ Model trained (ARIMA{train_result.get('arima_order')},"
                          f" AIC={train_result.get('arima_aic')}{garch_info})")
                    vr = train_result.get('vol_regime', 'Unknown')
                    if vr != 'Unknown':
                        print(f"  ✔ Vol regime: {vr} "
                              f"(ann.vol {train_result.get('annualised_vol_pct', 'N/A')}%)")

                    analysis['prediction'] = self.predictor.predict(days=30)
                    pred = analysis['prediction']
                    if pred.get('available'):
                        print(f"  ✔ 30-day forecast: {pred.get('trend')} "
                              f"({pred.get('pct_change_30d'):+.1f}%)")
                else:
                    analysis['prediction'] = train_result
                    print(f"  ⚠ Training failed: {train_result.get('reason')}")

                # Flow Correlation
                print("  ▸ Market correlation analysis …")
                nifty_hist = self.feeds.nifty50_history(period='2y')
                if nifty_hist is not None:
                    close_nifty = nifty_hist['close'] if 'close' in nifty_hist.columns \
                        else (nifty_hist['Close'] if 'Close' in nifty_hist.columns
                              else nifty_hist.iloc[:, 0])
                    analysis['flow_corr'] = self.flow_corr.compute(
                        close_series, close_nifty)
                    fc = analysis['flow_corr']
                    if fc.get('available'):
                        print(f"  ✔ Market corr: {fc.get('current_corr_with_market')} "
                              f"| RS: {fc.get('relative_strength_trend')}")
                else:
                    analysis['flow_corr'] = {'available': False,
                                             'reason': 'No Nifty data'}
            else:
                print("  ⚠ Insufficient price history for prediction model")
                analysis['prediction'] = {'available': False,
                                          'reason': 'Insufficient price history'}
                analysis['flow_corr'] = {'available': False,
                                         'reason': 'Insufficient price history'}

            # ── Macro-Correlation Engine ──
            print("  ▸ Macro-correlation analysis …")
            try:
                peer_sector = analysis.get('peer_cca', {}).get('sector', '')
                close_col_m = 'close' if 'close' in price_hist.columns else \
                    ('Close' if 'Close' in price_hist.columns else price_hist.columns[0])
                close_series_m = price_hist[close_col_m].dropna() if price_hist is not None else None
                if close_series_m is not None and len(close_series_m) > 60:
                    analysis['macro_corr'] = self.macro_engine.analyze(
                        bse_symbol, close_series_m, sector=peer_sector)
                    mc = analysis['macro_corr']
                    if mc.get('available'):
                        ardl = mc.get('ardl', {})
                        print(f"  ✔ Macro ARDL R²: {ardl.get('r_squared', 0):.3f} "
                              f"| Significant factors: {len(ardl.get('significant_factors', []))}")
                        for sig in mc.get('signals', [])[:3]:
                            print(f"    • {sig}")
                    else:
                        print(f"  ⚠ Macro: {mc.get('reason', 'N/A')}")
                else:
                    analysis['macro_corr'] = {'available': False,
                                              'reason': 'Insufficient price data'}
            except Exception as e:
                analysis['macro_corr'] = {'available': False, 'reason': str(e)}

        except Exception as e:
            print(f"  ⚠ Technical/Predictive error: {e}")
            if 'technicals' not in analysis:
                analysis['technicals'] = {'available': False, 'reason': str(e)}
            analysis['prediction'] = {'available': False, 'reason': str(e)}
            analysis['flow_corr'] = {'available': False, 'reason': str(e)}

        # ── Phase 6: Synthesis ───────────────────────────────
        print("\n🧪  PHASE 6 — Synthesis & Rating")
        analysis['rating'] = self.synthesis.run(analysis)
        rec = analysis['rating'].get('recommendation', 'N/A')
        conf = analysis['rating'].get('confidence', '')
        print(f"  ✔ Rating: {rec} (confidence: {conf})")

        # ── Phase 7: Report ──────────────────────────────────
        print("\n📝  PHASE 7 — Report Generation")
        report  = self.reporter.generate(symbol, data, analysis)
        filepath = self.reporter.save(report, symbol, config.output_dir)

        # ── Phase 7b: PDF Export ─────────────────────────────
        pdf_path = None
        try:
            from reports.pdf_exporter import export_markdown_to_pdf
            pdf_path = export_markdown_to_pdf(
                filepath, symbol, config.output_dir)
            if pdf_path:
                print(f"  ✔ PDF exported → {pdf_path}")
            else:
                print("  ⚠ PDF export returned empty path")
        except Exception as e:
            import traceback
            print(f"  ❌ PDF export FAILED: {e}")
            traceback.print_exc()

        print(f"\n{'═'*60}")
        print(f"  ✅  Report saved → {filepath}")
        if pdf_path:
            print(f"  ✅  PDF   saved → {pdf_path}")
        print(f"{'═'*60}\n")

        return filepath

    # ==================================================================
    # Shareholding helper
    # ==================================================================
    def _summarize_shareholding(self, data: dict) -> dict:
        shp = data.get('shareholding', pd.DataFrame())
        if shp.empty:
            return {}

        summary = {}
        categories = {
            'Promoters': ['Promoters'],
            'FIIs':      ['FIIs', 'FII', 'Flls', 'FlIs'],
            'DIIs':      ['DIIs', 'DII', 'Dils', 'DlIs', 'DIls'],
            'Government':['Government', 'Gov'],
            'Public':    ['Public'],
        }
        for label, names in categories.items():
            col = None
            for n in names:
                if n in shp.columns:
                    col = n
                    break
                for c in shp.columns:
                    if n.lower() in c.lower():
                        col = c
                        break
                if col:
                    break
            if col and col in shp.columns:
                vals = shp[col].dropna()
                def _pct(v):
                    f = float(v)
                    if f <= 1.0:
                        f = round(f * 100, 2)
                    return round(f, 2)

                if len(vals) >= 2:
                    summary[label] = {
                        'current':  _pct(vals.iloc[-1]),
                        'previous': _pct(vals.iloc[-2]),
                    }
                elif len(vals) == 1:
                    summary[label] = {
                        'current':  _pct(vals.iloc[-1]),
                        'previous': 'N/A',
                    }

        # ── Promoter Pledging Detection ──
        # Look for "Pledged" column in shareholding data
        pledged_col = None
        for col in shp.columns:
            if 'pledge' in col.lower():
                pledged_col = col
                break
        if pledged_col:
            vals = shp[pledged_col].dropna()
            if len(vals) >= 1:
                def _pct(v):
                    f = float(v)
                    if f <= 1.0:
                        f = round(f * 100, 2)
                    return round(f, 2)
                current_pledge = _pct(vals.iloc[-1])
                prev_pledge = _pct(vals.iloc[-2]) if len(vals) >= 2 else 'N/A'
                summary['PromoterPledging'] = {
                    'current': current_pledge,
                    'previous': prev_pledge,
                    # Any non-zero pledging is a signal; severity scales with amount
                    'is_red_flag': current_pledge > 0,
                    'severity': ('CRITICAL' if current_pledge > prev_pledge * 2 and current_pledge > 10
                                 else 'HIGH' if current_pledge > 15
                                 else 'MEDIUM' if current_pledge > 0
                                 else 'LOW'),
                }

        # Also try to detect pledging from the Screener.in page
        # by scanning the scraper's soup for pledging info
        try:
            pledge_text = ''
            if hasattr(self.ingestion.scraper, 'soup') and self.ingestion.scraper.soup:
                soup = self.ingestion.scraper.soup
                # Look for pledge info in the shareholding section
                for el in soup.find_all(['td', 'span', 'div']):
                    text = el.get_text(strip=True).lower()
                    if 'pledge' in text:
                        pledge_text = el.get_text(strip=True)
                        break
                if pledge_text and 'PromoterPledging' not in summary:
                    import re
                    pct_match = re.search(r'([\d.]+)\s*%', pledge_text)
                    if pct_match:
                        pct_val = float(pct_match.group(1))
                        summary['PromoterPledging'] = {
                            'current': pct_val,
                            'previous': 'N/A',
                            'is_red_flag': pct_val > 0,
                            'severity': ('CRITICAL' if pct_val > 30
                                         else 'HIGH' if pct_val > 15
                                         else 'MEDIUM' if pct_val > 0
                                         else 'LOW'),
                        }
        except Exception:
            pass

        return summary
