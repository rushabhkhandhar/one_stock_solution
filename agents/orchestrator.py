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
  Phase 4   → Qualitative Intelligence (Document Extraction Only)
  Phase 4.6 → Text Intelligence Engine (keyword-based)
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
from quant.tier2_analytics import (
    DuPontAnalysis, AltmanZScore, WorkingCapitalTrend,
    HistoricalValuationBand, QuarterlyPerformanceMatrix)
from quant.tier3_analytics import (
    DividendDashboard, CapitalAllocationScorecard, ScenarioAnalysis)
from predictive.arima_ets import HybridPredictor
from predictive.flow_correlation import FlowCorrelation
from predictive.macro_engine import MacroCorrelationEngine
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
        self.synthesis        = SynthesisAgent()
        self.cross_validator  = CrossValidator()
        self.kill_switch      = KillSwitch()
        self.feeds            = RealtimeFeeds()
        self.dupont            = DuPontAnalysis()
        self.altman            = AltmanZScore()
        self.wcc_trend         = WorkingCapitalTrend()
        self.hist_valuation    = HistoricalValuationBand()
        self.qtr_matrix        = QuarterlyPerformanceMatrix()
        self.dividend_dash     = DividendDashboard()
        self.cap_alloc         = CapitalAllocationScorecard()
        self.scenario          = ScenarioAnalysis()
        self.reporter          = ReportGenerator()

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

        # Resolve sector early (lightweight yfinance call) so DCF
        # can skip for banks/NBFCs before burning compute time.
        _sector_early = ''
        try:
            import yfinance as _yf, io as _io, sys as _sys
            _old = _sys.stderr; _sys.stderr = _io.StringIO()
            try:
                _tk = _yf.Ticker(f"{data.get('symbol', stock_name)}.BO")
                _info = _tk.info or {}
            finally:
                _sys.stderr = _old
            _sector_early = _info.get('sector', '')
            _industry_early = _info.get('industry', '')
            analysis['sector'] = _sector_early
            analysis['industry'] = _industry_early
        except Exception:
            analysis['sector'] = ''
            analysis['industry'] = ''

        print("  ▸ Financial Ratios …")
        analysis['ratios'] = self.ratios_calc.calculate(data)

        print("  ▸ DCF Valuation …")
        analysis['dcf'] = self.dcf_model.calculate(data, sector=_sector_early)

        print("  ▸ Beneish M-Score …")
        analysis['mscore'] = self.mscore_model.calculate(data)

        print("  ▸ Piotroski F-Score …")
        analysis['fscore'] = self.fscore_model.calculate(data)

        # Shareholding summary
        analysis['shareholding'] = self._summarize_shareholding(data)

        # Quarterly Shareholding Tracker (QoQ institutional flows)
        analysis['quarterly_shareholding'] = self._summarize_quarterly_shareholding(data)

        # Upcoming Results Calendar
        analysis['upcoming_results'] = data.get('upcoming_results', [])

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

        # ── Phase 3.4: Tier 2 Extended Analytics ────────────
        print("\n📐  PHASE 3.4 — Tier 2 Extended Analytics")

        print("  ▸ DuPont Decomposition …")
        try:
            analysis['dupont'] = self.dupont.analyze(data)
            dp = analysis['dupont']
            if dp.get('available'):
                print(f"  ✔ DuPont ROE: {dp.get('roe_dupont')}% "
                      f"| Weakest: {dp.get('weakest_factor')}")
            else:
                print(f"  ⚠ DuPont: {dp.get('reason', 'N/A')}")
        except Exception as e:
            analysis['dupont'] = {'available': False, 'reason': str(e)}

        print("  ▸ Altman Z-Score …")
        try:
            analysis['altman_z'] = self.altman.calculate(data)
            az = analysis['altman_z']
            if az.get('available'):
                print(f"  ✔ Z-Score: {az.get('z_score')} ({az.get('zone')})")
            else:
                print(f"  ⚠ Z-Score: {az.get('reason', 'N/A')}")
        except Exception as e:
            analysis['altman_z'] = {'available': False, 'reason': str(e)}

        print("  ▸ Working Capital Cycle Trend …")
        try:
            analysis['wcc_trend'] = self.wcc_trend.analyze(data)
            wc = analysis['wcc_trend']
            if wc.get('available'):
                print(f"  ✔ WCC: {wc.get('overall', 'N/A')} "
                      f"| {len(wc.get('metrics', []))} metrics tracked")
            else:
                print(f"  ⚠ WCC: {wc.get('reason', 'N/A')}")
        except Exception as e:
            analysis['wcc_trend'] = {'available': False, 'reason': str(e)}

        print("  ▸ Historical Valuation Band …")
        try:
            analysis['valuation_band'] = self.hist_valuation.analyze(data)
            vb = analysis['valuation_band']
            if vb.get('available'):
                pe_b = vb.get('pe_band', {})
                print(f"  ✔ P/E Band: {pe_b.get('min_pe')}x — {pe_b.get('max_pe')}x "
                      f"(current: {pe_b.get('current_pe')}x)")
            else:
                print(f"  ⚠ Valuation Band: {vb.get('reason', 'N/A')}")
        except Exception as e:
            analysis['valuation_band'] = {'available': False, 'reason': str(e)}

        print("  ▸ Quarterly Performance Matrix …")
        try:
            analysis['qtr_matrix'] = self.qtr_matrix.analyze(data)
            qm = analysis['qtr_matrix']
            if qm.get('available'):
                print(f"  ✔ Quarterly Matrix: {qm.get('num_quarters', 0)} quarters "
                      f"| Revenue: {qm.get('revenue_momentum', 'N/A')}")
            else:
                print(f"  ⚠ Quarterly Matrix: {qm.get('reason', 'N/A')}")
        except Exception as e:
            analysis['qtr_matrix'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.7: Tier 3 Extended Analytics ────────────
        print("\n📊  PHASE 3.7 — Tier 3 Extended Analytics")

        print("  ▸ Dividend Dashboard …")
        try:
            analysis['dividend_dash'] = self.dividend_dash.analyze(data)
            dd = analysis['dividend_dash']
            if dd.get('available'):
                print(f"  ✔ Dividends: Payout {dd.get('latest_payout_pct', 0):.1f}% "
                      f"| Sustainability: {dd.get('sustainability', 'N/A')}")
            else:
                print(f"  ⚠ Dividends: {dd.get('reason', 'N/A')}")
        except Exception as e:
            analysis['dividend_dash'] = {'available': False, 'reason': str(e)}

        print("  ▸ Capital Allocation Scorecard …")
        try:
            analysis['cap_alloc'] = self.cap_alloc.analyze(data)
            ca = analysis['cap_alloc']
            if ca.get('available'):
                print(f"  ✔ Capital Allocation: {ca.get('style', 'N/A')} "
                      f"| CapEx {ca.get('avg_capex_pct', 0):.0f}% "
                      f"| Div {ca.get('avg_dividends_pct', 0):.0f}%")
            else:
                print(f"  ⚠ Cap Alloc: {ca.get('reason', 'N/A')}")
        except Exception as e:
            analysis['cap_alloc'] = {'available': False, 'reason': str(e)}

        print("  ▸ Scenario Analysis (Bull/Base/Bear) …")
        try:
            analysis['scenario'] = self.scenario.analyze(data, analysis)
            sc = analysis['scenario']
            if sc.get('available'):
                wt = sc.get('weighted_target', 0)
                wu = sc.get('weighted_upside_pct', 0)
                print(f"  ✔ Scenario: Weighted Target ₹{wt:,.2f} ({wu:+.1f}%)")
            else:
                print(f"  ⚠ Scenario: {sc.get('reason', 'N/A')}")
        except Exception as e:
            analysis['scenario'] = {'available': False, 'reason': str(e)}

        # ── Phase 3.5: Forensic Extras (RPT, Contingent, Auditor) ────
        print("\n🔬  PHASE 3.5 — Forensic Deep Dive")
        ar_parsed = analysis.get('ar_parsed', {})

        # RPT Structured Analysis
        print("  ▸ Related Party Transactions …")
        try:
            # Rule 3: Pass holding-company context for RPT severity
            # contextualisation.  At this point segmental_layout
            # (Phase 2.6) may be available; full segmental comes in 3.6.
            _seg_layout = analysis.get('segmental_layout', {})
            _n_seg_early = len(_seg_layout.get('segments', []))
            analysis['rpt'] = self.forensic_extras.extract_rpt(
                ar_parsed, data,
                sotp_available=False,  # SOTP runs later in 3.6
                num_segments=_n_seg_early)
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

        # Rule 3: Retroactively enrich RPT with SOTP/segment context
        # now that Phase 3.6 has run.
        _rpt = analysis.get('rpt', {})
        _sotp_avail = analysis.get('sotp', {}).get('available', False)
        _full_seg = analysis.get('segmental', {}).get('segments', [])
        _n_full_seg = len(_full_seg) if _full_seg else 0
        if (_rpt.get('available') and not _rpt.get('is_holding_structure')
                and (_sotp_avail or _n_full_seg >= 3)):
            # Re-run RPT with updated holding context
            try:
                analysis['rpt'] = self.forensic_extras.extract_rpt(
                    ar_parsed, data,
                    sotp_available=_sotp_avail,
                    num_segments=_n_full_seg)
                print(f"  ✔ RPT re-evaluated with holding-company context "
                      f"(segments={_n_full_seg}, SOTP={_sotp_avail})")
            except Exception:
                pass  # keep original RPT result

        # ── Price Target Reconciliation ──────────────────────
        print("  ▸ Price Target Reconciliation …")
        analysis['price_target_recon'] = self._reconcile_price_targets(
            analysis)

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

        # ── Phase 4: Qualitative Intelligence (document-only) ─
        # RAG / FinBERT removed — all qualitative analysis now uses
        # direct keyword extraction from scraped documents.
        print("\n🧠  PHASE 4 — Qualitative Intelligence (document extraction)")
        analysis['qualitative'] = {
            'available': False,
            'reason': 'RAG pipeline removed; using document-level extraction only '
                      '(see Text Intelligence, Moat, and Say-Do sections).',
        }
        analysis['sentiment'] = {'available': False}

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

                # Inject delivery % from screener price data into
                # the yfinance DataFrame so technicals can analyse it
                screener_price = data.get('price')
                if (screener_price is not None
                        and not screener_price.empty
                        and 'delivery_pct' in screener_price.columns):
                    # Align by date index and merge
                    _del = screener_price[['delivery_pct']].copy()
                    _del.index = pd.to_datetime(_del.index)
                    _ph_idx = pd.to_datetime(price_hist.index)
                    price_hist = price_hist.copy()
                    price_hist.index = _ph_idx
                    price_hist = price_hist.join(_del, how='left')

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

            # ── ARIMAX: ARIMA with Macro Exogenous Regressors ──
            print("  ▸ ARIMAX (macro-augmented forecast) …")
            try:
                mc = analysis.get('macro_corr', {})
                if (mc.get('available')
                        and self.predictor.available
                        and close_series_m is not None
                        and len(close_series_m) > 60):
                    # Fetch macro price series from the engine
                    macro_price_data = self.macro_engine._fetch_macro_series('2y')
                    if macro_price_data:
                        arimax_train = self.predictor.train_arimax(
                            close_series_m, macro_price_data)
                        if arimax_train.get('available'):
                            analysis['arimax_train'] = arimax_train
                            analysis['arimax_forecast'] = self.predictor.predict_arimax(days=30)
                            axf = analysis['arimax_forecast']
                            aic_imp = arimax_train.get('aic_improvement')
                            print(f"  ✔ ARIMAX trained: AIC {arimax_train.get('arimax_aic')} "
                                  f"(improvement: {aic_imp:+.1f})" if aic_imp else
                                  f"  ✔ ARIMAX trained: AIC {arimax_train.get('arimax_aic')}")
                            sig_f = arimax_train.get('significant_factors', [])
                            if sig_f:
                                print(f"    Significant macro regressors: {', '.join(sig_f)}")
                            if axf.get('available'):
                                print(f"    30-day ARIMAX target: ₹{axf.get('end_price', 0):,.2f} "
                                      f"({axf.get('pct_change_30d', 0):+.1f}%)")
                        else:
                            analysis['arimax_train'] = arimax_train
                            analysis['arimax_forecast'] = {'available': False,
                                                          'reason': arimax_train.get('reason', 'Training failed')}
                            print(f"  ⚠ ARIMAX: {arimax_train.get('reason', 'N/A')}")
                    else:
                        analysis['arimax_train'] = {'available': False,
                                                    'reason': 'No macro price data'}
                        analysis['arimax_forecast'] = {'available': False,
                                                      'reason': 'No macro price data'}
                else:
                    analysis['arimax_train'] = {'available': False,
                                                'reason': 'Prerequisites not met (macro-corr or price data)'}
                    analysis['arimax_forecast'] = {'available': False,
                                                  'reason': 'Prerequisites not met'}
            except Exception as e:
                analysis['arimax_train'] = {'available': False, 'reason': str(e)}
                analysis['arimax_forecast'] = {'available': False, 'reason': str(e)}

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

    # ==================================================================
    # Quarterly Shareholding Tracker
    # ==================================================================
    def _summarize_quarterly_shareholding(self, data: dict) -> dict:
        """Compute QoQ changes from quarterly shareholding data."""
        qshp = data.get('quarterly_shareholding', pd.DataFrame())
        if isinstance(qshp, pd.DataFrame) and qshp.empty:
            return {'available': False,
                    'reason': 'No quarterly shareholding data'}
        if not isinstance(qshp, pd.DataFrame):
            return {'available': False,
                    'reason': 'Quarterly shareholding not in DataFrame format'}

        result = {'available': True, 'quarters': [], 'flows': {}}

        # Categories to track
        cat_map = {
            'Promoters': ['Promoters'],
            'FIIs': ['FIIs', 'FII', 'Flls', 'FlIs', 'FPIs'],
            'DIIs': ['DIIs', 'DII', 'Dils', 'DlIs', 'DIls'],
            'Public': ['Public'],
        }

        # Collect quarter dates (column-like index)
        if hasattr(qshp, 'columns'):
            result['quarters'] = [str(c) for c in qshp.columns
                                  if str(c).strip() != '']

        for label, aliases in cat_map.items():
            col = None
            for a in aliases:
                if a in qshp.columns:
                    col = a
                    break
                for c in qshp.columns:
                    if a.lower() in str(c).lower():
                        col = c
                        break
                if col:
                    break
            if not col or col not in qshp.columns:
                continue

            # qshp is typically categories × quarters
            # Try both orientations
            vals = qshp[col].dropna() if col in qshp.columns else pd.Series()

            # If rows are categories, try index-based lookup
            if vals.empty:
                for a in aliases:
                    if a in qshp.index:
                        vals = qshp.loc[a].dropna()
                        break
                    for idx in qshp.index:
                        if a.lower() in str(idx).lower():
                            vals = qshp.loc[idx].dropna()
                            break
                    if not vals.empty:
                        break

            if vals.empty or len(vals) < 2:
                continue

            def _to_pct(v):
                f = float(v)
                if f <= 1.0:
                    f = round(f * 100, 2)
                return round(f, 2)

            values = [_to_pct(v) for v in vals]
            qoq_changes = []
            for i in range(1, len(values)):
                qoq_changes.append(round(values[i] - values[i - 1], 2))

            result['flows'][label] = {
                'values': values,
                'latest': values[-1],
                'qoq_change': qoq_changes[-1] if qoq_changes else 0,
                'qoq_changes': qoq_changes,
                'trend': ('INCREASING' if sum(1 for d in qoq_changes[-3:] if d > 0) >= 2
                          else 'DECREASING' if sum(1 for d in qoq_changes[-3:] if d < 0) >= 2
                          else 'STABLE'),
            }

        if not result['flows']:
            return {'available': False,
                    'reason': 'Could not parse quarterly shareholding categories'}

        return result

    # ==================================================================
    # Price Target Reconciliation
    # ==================================================================
    def _reconcile_price_targets(self, analysis: dict) -> dict:
        """Reconcile DCF, SOTP, and peer-implied fair values."""
        recon = {'available': False, 'methods': []}

        # 1. DCF-derived target
        dcf = analysis.get('dcf', {})
        if (dcf.get('available')
                and not dcf.get('dcf_ev_mismatch')
                and dcf.get('intrinsic_value') is not None):
            recon['methods'].append({
                'method': 'DCF (Free Cash Flow)',
                'fair_value': round(dcf['intrinsic_value'], 2),
                'current_price': round(dcf['current_price'], 2),
                'upside_pct': round(dcf.get('upside_pct', 0), 1),
            })

        # 2. SOTP-derived target
        sotp = analysis.get('sotp', {})
        if (sotp.get('available')
                and sotp.get('intrinsic_value') is not None):
            recon['methods'].append({
                'method': 'SOTP (Sum-of-Parts)',
                'fair_value': round(sotp['intrinsic_value'], 2),
                'current_price': round(sotp['current_price'], 2),
                'upside_pct': round(sotp.get('upside_pct', 0), 1),
            })

        # 3. Peer-implied fair value = stock EPS × median peer P/E
        peer = analysis.get('peer_cca', {})
        ratios = analysis.get('ratios', {})
        if (peer.get('available')
                and peer.get('median_pe') is not None
                and ratios.get('pe_ratio') is not None):
            # Derive EPS from current_price / pe_ratio
            cmp = ratios.get('current_price')
            pe = ratios.get('pe_ratio')
            median_pe = peer['median_pe']
            if cmp and pe and pe > 0 and median_pe > 0:
                stock_eps = cmp / pe
                peer_implied = round(stock_eps * median_pe, 2)
                peer_upside = round((peer_implied / cmp - 1) * 100, 1)
                recon['methods'].append({
                    'method': f'Peer CCA (Median P/E {median_pe:.1f}x)',
                    'fair_value': peer_implied,
                    'current_price': round(cmp, 2),
                    'upside_pct': peer_upside,
                })

        # 4. Peer-implied P/B fair value (critical for banks/NBFCs
        #    where DCF is skipped)
        if peer.get('available') and peer.get('median_pb') is not None:
            cmp = ratios.get('current_price')
            median_pb = peer['median_pb']
            _roe = ratios.get('roe')
            _eps = ratios.get('ttm_eps') or ratios.get('eps')
            if (cmp and _eps and _eps > 0 and _roe and _roe > 0):
                bvps = _eps / (_roe / 100)
                pb_implied = round(bvps * median_pb, 2)
                if pb_implied > 0:
                    pb_upside = round((pb_implied / cmp - 1) * 100, 1)
                    recon['methods'].append({
                        'method': f'Peer CCA (Median P/B {median_pb:.1f}x)',
                        'fair_value': pb_implied,
                        'current_price': round(cmp, 2),
                        'upside_pct': pb_upside,
                    })

        # 5. Historical P/E Mean Reversion — uses the stock's OWN
        #    historical median P/E × current EPS.  Does NOT need peer data.
        vband = analysis.get('valuation_band', {})
        pe_band = vband.get('pe_band', {}) if vband.get('available') else {}
        median_hist_pe = pe_band.get('median_pe')
        if median_hist_pe and median_hist_pe > 0:
            cmp = ratios.get('current_price')
            _eps = ratios.get('ttm_eps') or ratios.get('eps')
            if cmp and _eps and _eps > 0:
                hist_pe_fv = round(_eps * median_hist_pe, 2)
                hist_pe_up = round((hist_pe_fv / cmp - 1) * 100, 1)
                recon['methods'].append({
                    'method': f'Historical Median P/E ({median_hist_pe:.1f}x)',
                    'fair_value': hist_pe_fv,
                    'current_price': round(cmp, 2),
                    'upside_pct': hist_pe_up,
                })

        # 6. Historical P/B Mean Reversion (useful for banks)
        pb_band = vband.get('pb_band', {}) if vband.get('available') else {}
        median_hist_pb = pb_band.get('median_pb')
        if median_hist_pb and median_hist_pb > 0:
            cmp = ratios.get('current_price')
            _eps = ratios.get('ttm_eps') or ratios.get('eps')
            _roe = ratios.get('roe')
            if cmp and _eps and _eps > 0 and _roe and _roe > 0:
                bvps = _eps / (_roe / 100)
                hist_pb_fv = round(bvps * median_hist_pb, 2)
                if hist_pb_fv > 0:
                    hist_pb_up = round((hist_pb_fv / cmp - 1) * 100, 1)
                    recon['methods'].append({
                        'method': f'Historical Median P/B ({median_hist_pb:.1f}x)',
                        'fair_value': hist_pb_fv,
                        'current_price': round(cmp, 2),
                        'upside_pct': hist_pb_up,
                    })

        if recon['methods']:
            recon['available'] = True
            fair_values = [m['fair_value'] for m in recon['methods']]
            recon['avg_fair_value'] = round(sum(fair_values) / len(fair_values), 2)
            recon['min_fair_value'] = round(min(fair_values), 2)
            recon['max_fair_value'] = round(max(fair_values), 2)
            cmp = recon['methods'][0]['current_price']
            recon['avg_upside_pct'] = round(
                (recon['avg_fair_value'] / cmp - 1) * 100, 1)
            print(f"  ✔ Reconciled {len(recon['methods'])} valuation methods "
                  f"→ avg fair value ₹{recon['avg_fair_value']:,.2f} "
                  f"({recon['avg_upside_pct']:+.1f}%)")
        else:
            recon['reason'] = 'No valuation methods produced fair values'
            print("  ⚠ No valuation methods available for reconciliation")

        return recon
