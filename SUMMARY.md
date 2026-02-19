# 📊 Advanced Equity Research System — Complete Summary

> **Fully automated, institutional-grade Indian equity research pipeline.**
> One command. Zero API keys. Goldman-standard output.

```
python main.py RELIANCE
```

---

## Table of Contents

1. [What Is This?](#1-what-is-this)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Sources & Ingestion](#3-data-sources--ingestion)
4. [The 7-Phase Pipeline](#4-the-7-phase-pipeline)
5. [Core Quantitative Engine (Phase 2)](#5-core-quantitative-engine-phase-2)
6. [Annual Report Processing (Phase 2.5–2.6)](#6-annual-report-processing-phase-25-26)
7. [Extended Quantitative Analysis (Phase 3)](#7-extended-quantitative-analysis-phase-3)
8. [Tier 1 Features (Features 1–5)](#8-tier-1-features-features-1-5)
9. [Tier 2 Features (Features 6–10)](#9-tier-2-features-features-6-10)
10. [Tier 3 Features (Features 11–15)](#10-tier-3-features-features-11-15)
11. [Forensic Deep-Dive (Phase 3.5–3.9)](#11-forensic-deep-dive-phase-35-39)
12. [Qualitative Intelligence (Phase 4)](#12-qualitative-intelligence-phase-4)
13. [Technical & Predictive (Phase 5)](#13-technical--predictive-phase-5)
14. [Synthesis & Rating (Phase 6)](#14-synthesis--rating-phase-6)
15. [Report Generation & PDF Export (Phase 7)](#15-report-generation--pdf-export-phase-7)
16. [Compliance & Safety](#16-compliance--safety)
17. [Configuration Philosophy](#17-configuration-philosophy)
18. [Project Structure](#18-project-structure)
19. [Dependencies](#19-dependencies)
20. [How to Run](#20-how-to-run)

---

## 1. What Is This?

A **single-command equity research system** for Indian listed companies. Give it a stock name — it scrapes live financial data from public sources, runs 30+ quantitative and qualitative analyses, and produces a comprehensive Markdown + PDF research report.

**Key principles:**

- ❌ **ZERO hardcoded financial values** — every parameter (risk-free rate, ERP, terminal growth, credit spread) is fetched live
- ❌ **ZERO fallback values** — if data can't be fetched, the module returns "unavailable" rather than substituting a default
- ❌ **ZERO API keys required** for core analysis — all data from Screener.in, BSE India, and yfinance (free)
- ✅ **Bank-aware** — auto-detects banks/NBFCs and adjusts analysis (skips inapplicable modules like DCF, Altman Z)
- ✅ **Corporate-action aware** — detects bonus/split/merger distortions in EPS, P/E, and trend analysis

---

## 2. Architecture Overview

```
main.py
  └─→ Orchestrator.analyze(stock_name)
        │
        ├── Phase 1   → Data Ingestion & Preprocessing
        ├── Phase 2   → Core Quant (Ratios, DCF, M-Score, F-Score)
        ├── Phase 2.5 → Annual Report Download & Validation
        ├── Phase 2.6 → Layout-Aware Table Extraction (BRSR, Segmental)
        ├── Phase 3   → Extended Quant (Peers, Trends, CFO/EBITDA)
        │   ├── Phase 3.4 → Tier 2 Analytics (DuPont, Altman, WCC, ValBand, QtrMatrix)
        │   └── Phase 3.7 → Tier 3 Analytics (Dividend, CapAlloc, Scenario)
        ├── Phase 3.5 → Forensic Deep Dive (RPT, Contingent, Auditor)
        ├── Phase 3.6 → Segmental + SOTP + Governance + ESG
        ├── Phase 3.9 → Forensic Dashboard (Unified Earnings Quality)
        ├── Phase 4   → Qualitative (Moat, Text Intel, Say-Do Tracker)
        ├── Phase 5   → Technical & Predictive (Technicals, ARIMA, ARIMAX, Macro)
        ├── Phase 6   → Synthesis (BUY / HOLD / SELL / SUSPENDED)
        └── Phase 7   → Report Generation (Markdown + PDF)
```

**Orchestrator:** `agents/orchestrator.py` — 1171 lines, coordinates 30+ analysis modules.

---

## 3. Data Sources & Ingestion

### Primary Sources

| Source | What We Get | Module |
|--------|-------------|--------|
| **Screener.in** | P&L, Balance Sheet, Cash Flow, Quarterly Results, Ratios, Shareholding Pattern, Price/Volume/Delivery %, Annual Report links, Peer/Sector info | `screenerScraper.py` |
| **BSE India API** | Corporate Announcements, Upcoming Results Calendar, Concall Transcript PDFs | `screenerScraper.py` |
| **yfinance** | Nifty 50 OHLCV, Stock beta, Macro prices (Crude Oil, USD/INR, Gold, India VIX), Peer financials for CCA | `data/realtime_feeds.py` |
| **worldgovernmentbonds.com** | India 10Y G-Sec yield (risk-free rate) | `data/realtime_feeds.py` |
| **IMF WEO** | India real GDP growth forecast (terminal growth rate) | `config.py` |

### Data Flow

```
screenerScraper.py   →   data/ingestion.py   →   data/preprocessing.py   →   Analysis Modules
   (raw HTML/JSON)        (dict → DataFrame)       (field mapping, cleaning)     (ratios, DCF, etc.)
```

### Key Modules

| Module | Class | Purpose |
|--------|-------|---------|
| `screenerScraper.py` | `ScreenerScrape` | Core scraper — pulls all structured data from Screener.in and BSE India with rate limiting and retry logic |
| `data/ingestion.py` | `DataIngestion` | Symbol resolution, dict→DataFrame conversion, price/volume parsing, TTM EPS computation, transcript PDF download & text extraction |
| `data/preprocessing.py` | `DataPreprocessor` | Canonical field-name resolution via `FIELD_MAP` (handles Screener's stripped HTML headers), bank-column aliasing (Revenue↔FinancingProfit, Deposits), derived metrics (shares outstanding, PAT margin, ROE, D/E) |
| `data/realtime_feeds.py` | `RealtimeFeeds` | Live market data via yfinance — Nifty history, stock beta (rolling covariance), macro indicators, risk-free rate, sector/peer lookup |

---

## 4. The 7-Phase Pipeline

| Phase | Name | What Happens |
|-------|------|-------------|
| **1** | Data Ingestion | Resolve symbol → BSE token, scrape all financial tables, parse price/volume/delivery data, download concall transcripts |
| **2** | Core Quant | Financial ratios (20+), DCF valuation, Beneish M-Score, Piotroski F-Score |
| **2.5** | AR Download | Download latest Annual Report PDFs from BSE, extract structured data (tables, footnotes, auditor observations) |
| **2.6** | Layout Parsing | Layout-aware table extraction — BRSR/ESG tables, segmental revenue, notes to accounts |
| **3** | Extended Quant | CFO/EBITDA quality, Peer CCA, 5Y Trends, Tier 1 features, Tier 2 features, Tier 3 features |
| **3.5–3.9** | Forensics | RPT extraction, contingent liabilities, auditor red flags, unified forensic dashboard |
| **3.6** | Segmental + SOTP | Segment-wise breakdown, Sum-of-the-Parts valuation, governance scoring, ESG/BRSR |
| **4** | Qualitative | Moat identification, text intelligence (keyword NLP), say-do management credibility tracker |
| **5** | Technical & Predictive | Technical analysis (trend, momentum, volume, volatility, S/R levels), ARIMA+ETS+GARCH ensemble, ARIMAX with macro regressors, flow correlation, macro-ARDL |
| **6** | Synthesis | Equal-weight voting across 17+ signals → BUY / HOLD / SELL / SUSPENDED |
| **7** | Report + PDF | Full Markdown report (2600+ lines template), PDF export with professional styling |

---

## 5. Core Quantitative Engine (Phase 2)

### 5.1 Financial Ratios — `quant/ratios.py`

**Class:** `FinancialRatios`

Computes 20+ financial ratios from scraped data:

| Category | Ratios |
|----------|--------|
| **Profitability** | ROE, ROA, ROCE, PAT Margin, OPM |
| **Leverage** | D/E Ratio, Interest Coverage |
| **Valuation** | P/E (TTM), EPS, PEG Ratio, Dividend Yield |
| **Growth** | Revenue Growth, Profit Growth, 3Y CAGR, 5Y CAGR |
| **Efficiency** | Debtors Turnover, Inventory Turnover, Current Ratio, Cash Conversion Cycle |

- **Corporate-action detection**: Compares implied shares (PAT/EPS) with B/S equity capital to detect bonus/split events that distort P/E
- **Bank-aware**: Uses bank-specific column aliases (Revenue → FinancingProfit, Deposits instead of Borrowings)

### 5.2 DCF Valuation — `quant/dcf.py`

**Class:** `DCFModel` (546 lines)

Full Discounted Cash Flow model:

- **WACC computation** via CAPM: Rf (live 10Y G-Sec) + β × ERP (live Nifty 50 CAGR − Rf), with cost of debt from actual interest expense
- **FCF projection**: Linearly decaying growth over 10 years from current growth to terminal growth
- **Terminal value**: Gordon Growth Model (FCF × (1+g)) / (WACC − g)
- **4-step waterfall**: PV of projected FCFs → PV of Terminal Value → Enterprise Value → subtract net debt → Equity Value → per share
- **WACC sensitivity grid**: ±200bps growth × ±200bps WACC matrix
- **Guardrails**: Peak-CapEx flagging, DCF EV vs Market EV deviation warning
- **Auto-skips**: Banks and NBFCs (detected via Deposits column)

### 5.3 Beneish M-Score — `quant/forensics.py`

**Class:** `BeneishMScore` (~210 lines)

8-component earnings manipulation detector (Beneish 1999):

| Component | Full Name | What It Measures |
|-----------|-----------|-----------------|
| DSRI | Days Sales Receivable Index | Revenue quality |
| GMI | Gross Margin Index | Margin deterioration |
| AQI | Asset Quality Index | Asset capitalization |
| SGI | Sales Growth Index | Unsustainable growth |
| DEPI | Depreciation Index | Depreciation policy |
| SGAI | SGA Expense Index | Overhead management |
| TATA | Total Accruals to Assets | Earnings vs cash |
| LVGI | Leverage Index | Debt increase |

- **Thresholds** (from original paper — never modified): M > −1.78 = Likely Manipulator, M < −2.22 = Unlikely
- **Graceful degradation**: Excludes components with missing data, reports confidence level

### 5.4 Piotroski F-Score — `quant/piotroski.py`

**Class:** `PiotroskiFScore` (~170 lines)

9-criteria fundamental strength score (Piotroski 2000):

| Category | Criteria |
|----------|----------|
| **Profitability (4)** | ROA > 0, CFO > 0, ΔROA > 0, CFO > Net Income |
| **Leverage (3)** | ΔDebt ≤ 0, ΔCurrent Ratio > 0, No share dilution |
| **Efficiency (2)** | ΔGross Margin > 0, ΔAsset Turnover > 0 |

- Rating: **STRONG** (8–9), **MODERATE** (5–7), **WEAK** (0–4)

---

## 6. Annual Report Processing (Phase 2.5–2.6)

### 6.1 Report Downloader — `data/report_downloader.py`

**Class:** `ReportDownloader`

- Downloads Annual Report PDFs from BSE India links (found on Screener.in company page)
- Smart caching: only downloads once per symbol+year (cache dir: `output/reports/`)
- BSE rate limiting to avoid IP blocks

### 6.2 PDF Parser — `data/pdf_parser.py`

**Class:** `PDFParser` (668 lines)

6-phase Annual Report extraction pipeline:

1. **Page Classification** — 15+ section patterns (Director's Report, Auditor's Report, Notes, BRSR, etc.)
2. **Table Extraction** — via pdfplumber with structured column→value mapping
3. **Footnote Extraction** — exceptional items, restatements, going concern warnings
4. **Key Figure Extraction** — standalone revenue, PAT, EPS mentions
5. **Auditor Observations** — qualified/adverse opinions, emphasis of matter
6. **Special Sections** — contingent liabilities, related party transactions

### 6.3 Layout-Aware Parser — `data/layout_parser.py`

**Class:** `LayoutAwareParser` (447 lines)

High-fidelity table extraction that preserves column→header relationships (standard extraction "flattens" complex tables):

- PyMuPDF for fast page scanning, pdfplumber in "accurate mode" for targeted extraction
- Specialized extractors: BRSR tables, Segmental Revenue, Notes to Accounts, Financial Overview
- Produces structured JSON/dict + Markdown representation

---

## 7. Extended Quantitative Analysis (Phase 3)

### 7.1 Peer Comparable Company Analysis — `quant/peer_comparables.py`

**Class:** `PeerComparables` (390 lines)

- **Dynamic peer discovery**: Finds peers at runtime from Screener.in sector pages + yfinance industry classification (ZERO hardcoded peer lists)
- **Metrics compared**: P/E, EV/EBITDA, P/B, ROE, Dividend Yield
- **Market cap tiers**: Computed dynamically from live Nifty 50 median
- **Output**: Premium/discount vs peers, sector ranking, relative positioning

### 7.2 5-Year Trend Analysis — `quant/trend_analyzer.py`

**Class:** `TrendAnalyzer` (~370 lines)

Tracks 11 key metrics over 5 years:

| Metrics Tracked |
|----------------|
| Revenue, PAT, Operating Profit, EPS, OPM, ROE, ROCE, D/E, CFO, Borrowings, Reserves |

- **Linear regression** for direction detection
- **CAGR** computation for each metric
- **Acceleration/deceleration** detection (YoY change trends)
- **Corporate-action distortion** flagging (softens DECELERATING labels when bonus/split detected)
- **Overall health score** (0–10) with direction: IMPROVING / STABLE / DETERIORATING

### 7.3 CFO/EBITDA Quality Check

Validates operating cash flow against EBITDA to detect earnings quality issues:
- CFO/EBITDA ratio < 0.5 → red flag
- Persistent divergence over 3+ years → structural concern

---

## 8. Tier 1 Features (Features 1–5)

| # | Feature | Module | What It Does |
|---|---------|--------|-------------|
| 1 | **Delivery Volume Analysis** | `quant/technicals.py` | Smart-money signal detection from delivery % vs volume. Four quadrants: Accumulation (high delivery + high volume), Distribution (low delivery + high volume), Speculative Rally, Panic Selling |
| 2 | **Quarterly Shareholding Tracker** | `agents/orchestrator.py` | QoQ changes in Promoter, FII, DII, Public holdings from Screener's shareholding data. Flags significant moves (>1%) |
| 3 | **Upcoming Results Calendar** | `agents/orchestrator.py` | Board meeting dates for results declaration from BSE corporate announcements API |
| 4 | **Price Target Reconciliation** | `agents/orchestrator.py` | Cross-validates DCF intrinsic value, SOTP valuation, Peer CCA relative value, Historical P/E & P/B implied values. Shows convergence/divergence across methods |
| 5 | **PEG Ratio** | `quant/ratios.py` | Price/Earnings-to-Growth ratio from TTM P/E and earnings growth. PEG < 1 = undervalued, > 2 = overvalued |

---

## 9. Tier 2 Features (Features 6–10)

All implemented in `quant/tier2_analytics.py` (686 lines):

| # | Feature | Class | What It Does |
|---|---------|-------|-------------|
| 6 | **DuPont Decomposition** | `DuPontAnalysis` | 5-factor ROE breakdown: Tax Burden × Interest Burden × EBIT Margin × Asset Turnover × Equity Multiplier. Multi-year history. Identifies weakest and strongest ROE driver |
| 7 | **Altman Z-Score** | `AltmanZScore` | Bankruptcy predictor: Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E. Zones: SAFE (>2.99), GREY (1.81–2.99), DISTRESS (<1.81). Auto-skips banks/financials |
| 8 | **Working Capital Cycle Trend** | `WorkingCapitalTrend` | Multi-year Debtor Days, Inventory Days, Creditor Days, Cash Conversion Cycle. Trend: IMPROVING / STABLE / WORSENING. Bank-aware (skips for banks) |
| 9 | **Historical Valuation Band** | `HistoricalValuationBand` | P/E and P/B historical ranges with current percentile position. Zone: UNDERVALUED / FAIRLY_VALUED / OVERVALUED based on where current valuation sits in its own history |
| 10 | **Quarterly Performance Matrix** | `QuarterlyPerformanceMatrix` | QoQ and YoY revenue and profit trend analysis from quarterly results. Growth acceleration/deceleration detection |

---

## 10. Tier 3 Features (Features 11–15)

### Feature 11 — Dividend Dashboard

**Module:** `quant/tier3_analytics.py` → `DividendDashboard`

| Metric | How It's Computed |
|--------|------------------|
| Payout Ratio History | EPS × Dividend Payout % from P&L, per year |
| Dividend Yield Trend | DPS / average annual price, per year |
| Dividend CAGR | Compound annual growth of DPS over available history |
| Sustainability Score | CFO ÷ Total Dividends Paid → STRONG (≥2×), ADEQUATE (≥1×), AT_RISK (<1×) |
| Consistency | % of years with dividends paid out of total years of data |

### Feature 12 — Capital Allocation Scorecard

**Module:** `quant/tier3_analytics.py` → `CapitalAllocationScorecard`

Analyzes how management deploys Cash from Operations:

| Deployment | Source |
|-----------|--------|
| CapEx | Fixed Assets Purchased (or total Investing CF if unavailable) |
| Dividends | Net Profit × Payout Ratio |
| Debt Repayment | YoY decrease in Borrowings (if positive) |
| Residual | CFO minus all the above |

**Style Classification:**
- 📈 **GROWTH-ORIENTED** — CapEx > 50% of CFO
- 💵 **SHAREHOLDER-FRIENDLY** — Dividends > 30% of CFO
- 🏦 **DELEVERAGING** — Debt Repayment > 25% of CFO
- ⚖️ **BALANCED** — No single category dominant, all present
- 🔀 **MIXED** — Everything else

### Feature 13 — Scenario Analysis (Bull / Base / Bear)

**Module:** `quant/tier3_analytics.py` → `ScenarioAnalysis`

| Scenario | Revenue Growth | PAT Margin | Exit P/E | Source |
|----------|---------------|-----------|---------|--------|
| 🟢 Bull | 75th percentile of history | 75th percentile | 75th percentile | Real historical distribution |
| 🟡 Base | 50th percentile (median) | 50th percentile | 50th percentile | Real historical distribution |
| 🔴 Bear | 25th percentile of history | 25th percentile | 25th percentile | Real historical distribution |

- **Probability weighting** via mean-reversion logic: if current metrics sit above historical median → more weight to Bear; if below → more weight to Bull
- **1-year forward target** for each scenario: (Current Revenue × (1 + growth)) × margin × exit P/E ÷ shares
- **Probability-weighted target price** = Σ (scenario target × probability)

### Feature 14 — ARIMAX with Macro Regressors

**Module:** `predictive/arima_ets.py` → `HybridPredictor.train_arimax()` / `predict_arimax()`

| Aspect | Detail |
|--------|--------|
| Model | SARIMAX (Seasonal ARIMA with eXogenous variables) |
| Exogenous Variables | Crude Oil (CL=F), USD/INR (INR=X), Gold (GC=F), India VIX (^INDIAVIX) — returns, not levels |
| Grid Search | p ∈ [0,3], q ∈ [0,3], d = 1 — best by AIC |
| Forward Projection | 30-day forecast with macro variables projected via trailing 20-day trend extrapolation |
| Output | Forecast array, confidence intervals, significant macro regressors with p-values, AIC improvement vs plain ARIMA |

### Feature 15 — Support / Resistance Levels

**Module:** `quant/technicals.py` → `TechnicalAnalyzer._support_resistance()`

Three complementary methods:

| Method | How It Works |
|--------|-------------|
| **Classic Pivot Points** | P = (H+L+C)/3, then R1/R2/R3 and S1/S2/S3 from standard formulas. Zone classification: ABOVE_R1, ABOVE_PIVOT, BELOW_PIVOT, BELOW_S1 |
| **Fibonacci Retracement** | 52-week High/Low range with 7 levels: 0%, 23.6%, 38.2%, 50%, 61.8%, 78.6%, 100%. Nearest support/resistance identification |
| **Price Congestion Zones** | Histogram clustering of recent prices (up to 500 bars). Top 5 zones by frequency, classified as SUPPORT or RESISTANCE relative to current price |

**Summary output:** `key_supports` (nearest first, descending) and `key_resistances` (nearest first, ascending) aggregated from all three methods with source labels.

---

## 11. Forensic Deep-Dive (Phase 3.5–3.9)

### 11.1 Forensic Extras — `quant/forensic_extras.py`

**Class:** `ForensicExtras` (485 lines)

Structured extraction from Annual Report PDFs:

| Check | What It Finds |
|-------|--------------|
| **Related Party Transactions** | RPT value as % of revenue, holding-company identification, transaction types |
| **Contingent Liabilities** | Total contingent liabilities as % of net worth, plausibility bounding |
| **Auditor Red Flags** | Keyword-based severity classification: HIGH (qualified opinion, going concern, fraud), MEDIUM (emphasis of matter, non-compliance), LOW (prior period adjustments) |

### 11.2 Forensic Dashboard — `quant/forensic_dashboard.py`

**Class:** `ForensicDashboard` (470 lines)

Unified earnings quality composite score (0–10):

| Check | Weight | What It Measures |
|-------|--------|-----------------|
| CFO/EBITDA Realism | Equal | Is operating cash flow consistent with reported EBITDA? |
| Accruals Quality | Equal | Are earnings driven by cash or accruals? |
| Revenue-Receivables Divergence | Equal | Is revenue growing faster than receivables? |
| RPT Exposure | Equal | Related party transaction risk level |
| Contingent Liabilities | Equal | Off-balance-sheet risk |
| CFO Trend | Equal | Is operating cash flow improving or deteriorating? |

**Quality Ratings:** HIGH (≥7.5), MODERATE (≥5), LOW (≥2.5), VERY_LOW (<2.5)

---

## 12. Qualitative Intelligence (Phase 4)

### 12.1 Moat Identifier — `qualitative/moat_identifier.py`

**Class:** `MoatIdentifier` (~300 lines)

Detects competitive moat from Annual Report and concall text. Scans for 7 moat types:

| Moat Type | Keywords Scanned |
|-----------|-----------------|
| R&D / Innovation | R&D, patent, proprietary, technology leadership |
| Brand Power | brand value, consumer loyalty, premium pricing |
| Network Effect | platform, ecosystem, user base, marketplace |
| Switching Costs | long-term contracts, lock-in, integration |
| Cost Advantage | economies of scale, low-cost producer, cost leadership |
| Regulatory Moat | license, government contract, monopoly, spectrum |
| Market Share | market leader, dominant, #1 position |

**Output:** Moat score (0–10), dominant moat type, competitive advantages list, R&D % of revenue, patent mentions

### 12.2 Text Intelligence Engine — `qualitative/text_intelligence.py`

**Class:** `TextIntelligenceEngine` (552 lines)

Unified keyword-based NLP analysis across concalls, AR sections, and announcements. **No LLM required.**

| Feature | Method |
|---------|--------|
| Topic Extraction | 11 predefined topics (Growth, Margins, Debt, CapEx, Market Share, Digital, ESG, etc.) with keyword matching |
| Forward-Looking Statements | Pattern matching for guidance, targets, expectations |
| Tone Classification | POSITIVE / NEUTRAL / NEGATIVE from keyword density |
| Structured Insights | Auto-generated insight bullets from detected patterns |

### 12.3 Say-Do Tracker — `qualitative/say_do_tracker.py`

**Class:** `SayDoTracker` (~400 lines)

Management credibility tracker:

- Extracts forward-looking guidance from **prior** concall transcripts (% targets, ₹ amounts, growth rates, margin targets)
- Compares against **actual** reported results
- **Exponential time-decay** weighting: recent quarters weighted 2–3× more than older ones
- **Say-Do Ratio**: weighted % of guidance actually delivered

| Credibility | Say-Do Ratio |
|------------|-------------|
| EXCELLENT | ≥ 90% |
| GOOD | ≥ 75% |
| FAIR | ≥ 60% |
| POOR | ≥ 40% |
| VERY_POOR | < 40% |

- Flags governance risk if Say-Do Ratio < 50%

---

## 13. Technical & Predictive (Phase 5)

### 13.1 Technical Analysis — `quant/technicals.py`

**Class:** `TechnicalAnalyzer` (708 lines)

| Category | Indicators |
|----------|-----------|
| **Trend** | 50-DMA, 200-DMA, Golden Cross / Death Cross detection |
| **Momentum** | RSI(14), MACD(12,26,9), Rate of Change (20), 52W High/Low proximity |
| **Volume** | OBV, Volume Trend, Volume-Price Divergence, Delivery Volume Analysis (4-quadrant smart-money signal) |
| **Volatility** | ATR(14), Bollinger Bands(20,2), Historical Volatility (20d annualized) |
| **Support/Resistance** | Classic Pivot Points, Fibonacci Retracement, Price Congestion Zones |
| **Composite** | STRONG_BUY → STRONG_SELL (aggregated from all indicators) |

### 13.2 ARIMA + ETS + GARCH Ensemble — `predictive/arima_ets.py`

**Class:** `HybridPredictor` (629 lines)

| Component | Method |
|-----------|--------|
| **ARIMA** | Auto-selected order via AIC grid search (p,d,q ∈ [0–3]) |
| **ETS** | Holt-Winters with damped trend (exponential smoothing) |
| **GARCH** | Fits GARCH(1,1), EGARCH(1,1), GJR-GARCH(1,1) — selects best by AIC. Student's t distribution |
| **Ensemble** | AIC-weighted softmax combination of ARIMA + ETS |
| **ARIMAX** | SARIMAX with macro exogenous regressors (Crude, USD/INR, Gold, VIX) |

**Output:** 30-day forecast, confidence intervals (GARCH-enhanced), trend (BULLISH/BEARISH/NEUTRAL), volatility regime (LOW/MEDIUM/HIGH), conditional volatility

### 13.3 Flow Correlation — `predictive/flow_correlation.py`

**Class:** `FlowCorrelation` (~160 lines)

- Rolling 30-day correlation between stock returns and Nifty 50
- Regime classification: HIGHLY CORRELATED → MODERATELY → LOW → NEGATIVE CORRELATION
- Relative Strength trend: OUTPERFORMING / UNDERPERFORMING vs market

### 13.4 Macro Correlation Engine — `predictive/macro_engine.py`

**Class:** `MacroCorrelationEngine` (~400 lines)

| Aspect | Detail |
|--------|--------|
| Macro Variables | Crude Oil (CL=F), USD/INR (INR=X), Gold (GC=F), India VIX (^INDIAVIX), Nifty 50 (^NSEI) |
| Lagged Correlations | Lags: 0, 1, 5, 10, 20 days |
| ARDL-Lite | OLS regression of stock returns on lagged macro returns → R², significant factors |
| Sector Sensitivity | COMPUTED from actual correlations (not hardcoded profiles) |
| Signals | Actionable tailwind/headwind alerts based on current macro trends + stock sensitivity |

---

## 14. Synthesis & Rating (Phase 6)

### `agents/synthesis_agent.py`

**Class:** `SynthesisAgent` (347 lines)

**Equal-weight voting system** — every available signal gets exactly 1 vote (no hardcoded weights):

| Signal | Source |
|--------|--------|
| DCF Valuation | Intrinsic value vs market price |
| SOTP Valuation | Sum-of-parts vs market price |
| Piotroski F-Score | Strong/Moderate/Weak |
| Beneish M-Score | Manipulation risk |
| Growth Trajectory | Revenue + Profit trends |
| 5Y Trend Health | Overall trend score |
| Technical Signal | Composite technical signal |
| Peer Positioning | Premium/discount vs peers |
| Price Prediction | ARIMA forecast direction |
| CFO/EBITDA Quality | Earnings quality |
| Governance Score | Board composition, related parties |
| Moat Strength | Competitive advantage |
| ESG Score | Environmental, Social, Governance |
| Text Intelligence | NLP-based tone |
| Forensic Dashboard | Earnings quality composite |
| Say-Do Ratio | Management credibility |
| Macro Correlation | Macro environment alignment |

**Rating Logic:**
- Positive signal % → BUY (≥65%), HOLD (≥45%), SELL (<45%)
- **Hard-stop**: If cross-validation trust score < threshold → rating is **SUSPENDED** regardless of score
- Output: Recommendation, investment thesis, confidence level, suggested horizon

---

## 15. Report Generation & PDF Export (Phase 7)

### 15.1 Markdown Report — `reports/generator.py`

**Class:** `ReportGenerator` (2611 lines)

Produces a Goldman-standard research report with 20+ sections:

| Section | Content |
|---------|---------|
| Executive Summary | Company name, sector, market cap, key metrics |
| Rating Box | BUY/HOLD/SELL/SUSPENDED with score and confidence |
| Investment Thesis | Bull points, bear risks from synthesis |
| Financial Summary | 20+ ratios in a structured table |
| 5-Year Trends | 11 metrics with direction, CAGR, projections, corporate-action notes |
| DuPont Decomposition | 5-factor ROE breakdown table |
| Altman Z-Score | Bankruptcy risk with component breakdown |
| Working Capital Cycle | CCC trend table |
| Historical Valuation Band | P/E and P/B percentile ranges |
| Quarterly Performance | QoQ and YoY matrix |
| Dividend Dashboard | Payout history, yield trend, sustainability |
| Capital Allocation | CFO deployment style with year-by-year breakdown |
| Scenario Analysis | Bull/Base/Bear targets with probability-weighted outcome |
| DCF Valuation | Full waterfall + WACC sensitivity grid |
| Peer Comparison | CCA table with premium/discount |
| Forensic Analysis | M-Score, RPT, Contingent, Auditor flags |
| Shareholding Pattern | Promoter/FII/DII/Public with QoQ changes |
| Technical Analysis | All indicators + S/R levels + composite signal |
| Price Prediction | ARIMA+ETS+GARCH ensemble forecast with CI |
| ARIMAX Forecast | Macro-augmented forecast with significant regressors |
| Support/Resistance | Pivot Points + Fibonacci + Congestion Zones |
| Macro Correlation | ARDL results + tailwind/headwind signals |
| ESG/BRSR | Environmental and governance metrics |
| Moat Analysis | Competitive advantage identification |
| Text Intelligence | Concall NLP insights |
| Say-Do Tracker | Management credibility scorecard |
| Forensic Dashboard | Unified earnings quality score |
| Risk Factors | Automated risk identification from all modules |
| SEBI Disclaimer | Regulatory compliance notice |

### 15.2 PDF Export — `reports/pdf_exporter.py`

**Function:** `export_markdown_to_pdf()` (772 lines)

3-tier fallback PDF generation:

1. **Tier 1:** markdown2 + weasyprint (best quality, needs system libs)
2. **Tier 2:** markdown + pdfkit/wkhtmltopdf
3. **Tier 3:** fpdf2 (pure Python, always works)

Features: Professional A4 layout, CSS styling, zebra-striped tables, emoji→text fallback, system Unicode font auto-detection (macOS/Linux/Windows)

---

## 16. Compliance & Safety

### 16.1 Cross-Validator — `compliance/cross_validator.py`

**Class:** `CrossValidator` (563 lines)

Validates scraper data against Annual Report PDFs:

| Check | What It Does |
|-------|-------------|
| Revenue Match | Scraped vs AR (auto Lakhs↔Crores normalization) |
| PAT Match | Net Profit cross-check |
| EPS Match | With corporate-action adjustment detection |
| CFO Match | Operating Cash Flow cross-check |
| Footnote Analysis | Exceptional items, restatements, going concern |
| Auditor Opinion | Qualified/adverse/emphasis-of-matter detection |

**Trust Score:** 0–100 with auditor penalty deductions. Labels: HIGH CONFIDENCE (≥75), MODERATE (≥60), UNRELIABLE (<60)

### 16.2 Kill Switch & Safety — `compliance/safety.py`

| Component | What It Does |
|-----------|-------------|
| `stamp_source()` | Appends source citations to AI-generated text |
| `KillSwitch` | Triggers on: stale data (data-derived frequency), critical missing fields, price anomalies (5-sigma single-day moves) |
| `SEBI_DISCLAIMER` | Regulatory compliance text for report headers |

---

## 17. Configuration Philosophy

**File:** `config.py`

```
ZERO hardcoded financial values. Every parameter is fetched LIVE.
If a parameter cannot be fetched, it remains None.
Modules MUST handle None gracefully — return "unavailable", never substitute defaults.
```

| Parameter | Source | Fallback |
|-----------|--------|----------|
| Risk-Free Rate | India 10Y G-Sec yield (worldgovernmentbonds.com → yfinance ^TNX with CIP) | `None` |
| Equity Risk Premium | Nifty 50 10Y CAGR − Risk-Free Rate | `None` |
| Terminal Growth Rate | IMF WEO India GDP forecast | `None` |
| Credit Spread | LQD vs TLT spread + EM premium | `None` |
| Beta | Rolling covariance with Nifty 50 (yfinance) | `None` |
| Beneish/Piotroski thresholds | Academic paper constants (immutable) | N/A |

---

## 18. Project Structure

```
screener-scraper/
│
├── main.py                          # CLI entry point
├── config.py                        # Central config (all live-fetched)
├── screenerScraper.py               # Core web scraper
├── requirements.txt                 # Dependencies
├── SUMMARY.md                       # This file
├── test_tier3.py                    # Tier 3 functional tests
├── test_integration.py              # Integration tests
│
├── agents/
│   ├── orchestrator.py              # 7-phase pipeline controller (1171 lines)
│   ├── synthesis_agent.py           # Equal-weight BUY/HOLD/SELL voting (347 lines)
│   └── rag_agent.py                 # RAG retrieval agent
│
├── data/
│   ├── ingestion.py                 # Symbol resolution, DataFrame conversion (464 lines)
│   ├── preprocessing.py             # Field mapping, bank aliases, derived metrics
│   ├── report_downloader.py         # Annual Report PDF downloader
│   ├── pdf_parser.py                # 6-phase AR PDF extraction (668 lines)
│   ├── layout_parser.py             # Layout-aware table extraction (447 lines)
│   └── realtime_feeds.py            # Live market data via yfinance
│
├── quant/
│   ├── ratios.py                    # 20+ financial ratios
│   ├── dcf.py                       # DCF valuation + WACC sensitivity (546 lines)
│   ├── forensics.py                 # Beneish M-Score (8 components)
│   ├── piotroski.py                 # Piotroski F-Score (9 criteria)
│   ├── peer_comparables.py          # Dynamic Peer CCA (390 lines)
│   ├── forensic_extras.py           # RPT, Contingent Liabilities, Auditor (485 lines)
│   ├── forensic_dashboard.py        # Unified Earnings Quality (470 lines)
│   ├── governance.py                # Corporate Governance scoring
│   ├── segmental.py                 # Segment-wise breakdown + HHI
│   ├── esg_brsr.py                  # ESG/BRSR extraction
│   ├── trend_analyzer.py            # 5Y trends for 11 metrics
│   ├── technicals.py                # Technical analysis + S/R levels (708 lines)
│   ├── sotp.py                      # Sum-of-the-Parts valuation
│   ├── tier2_analytics.py           # Tier 2: DuPont, Altman, WCC, ValBand, QtrMatrix (686 lines)
│   └── tier3_analytics.py           # Tier 3: Dividend, CapAlloc, Scenario (546 lines)
│
├── predictive/
│   ├── arima_ets.py                 # ARIMA+ETS+GARCH+ARIMAX ensemble (629 lines)
│   ├── flow_correlation.py          # Market correlation + Relative Strength
│   └── macro_engine.py              # Macro-ARDL + lagged correlations (400 lines)
│
├── qualitative/
│   ├── moat_identifier.py           # 7-type competitive moat detection
│   ├── text_intelligence.py         # Keyword NLP (no LLM) (552 lines)
│   ├── say_do_tracker.py            # Management credibility tracker
│   ├── sentiment.py                 # Sentiment analysis
│   ├── management_tracker.py        # Management change tracker
│   └── rag_engine.py                # RAG retrieval engine
│
├── compliance/
│   ├── cross_validator.py           # Data cross-validation + Trust Score (563 lines)
│   └── safety.py                    # Kill Switch + SEBI disclaimer
│
├── reports/
│   ├── generator.py                 # Goldman-standard Markdown report (2611 lines)
│   └── pdf_exporter.py              # PDF export with 3-tier fallback (772 lines)
│
├── output/
│   ├── reports/                     # Cached Annual Report PDFs
│   └── transcripts/                 # Cached concall transcripts
│
└── tokens/                          # Token usage tracking
```

---

## 19. Dependencies

| Category | Packages |
|----------|----------|
| **Core** | beautifulsoup4, requests, pandas, numpy, lxml |
| **PDF** | PyMuPDF (fitz), pdfplumber |
| **Market Data** | yfinance |
| **Quant** | statsmodels, scikit-learn, arch |
| **NLP** | transformers, torch, sentence-transformers, rank-bm25 |
| **Report** | fpdf2, markdown2 |

---

## 20. How to Run

```bash
# 1. Clone and setup
cd screener-scraper
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Run for any Indian listed company
python main.py RELIANCE
python main.py TCS
python main.py "HDFC BANK"
python main.py AXISCADES

# 3. Output
# → output/<SYMBOL>_Research_<DATE>.md   (Markdown report)
# → output/<SYMBOL>_Research_<DATE>.pdf  (PDF report)
```

---

## Feature Implementation Timeline

| Tier | Features | Status |
|------|----------|--------|
| **Core** | Data ingestion, Ratios, DCF, M-Score, F-Score, AR Parsing, Peer CCA, Trends, Technicals, Forensics, Governance, ESG, Moat, NLP, Synthesis, Report | ✅ Complete |
| **Tier 1** | Delivery Volume, Shareholding Tracker, Results Calendar, Price Target Recon, PEG Ratio | ✅ Complete |
| **Tier 2** | DuPont Decomposition, Altman Z-Score, Working Capital Cycle, Historical Valuation Band, Quarterly Matrix | ✅ Complete |
| **Tier 3** | Dividend Dashboard, Capital Allocation Scorecard, Scenario Analysis, ARIMAX, Support/Resistance | ✅ Complete |

**Total: 15 feature enhancements + full core system = 30+ analysis modules**

---

*Generated on 19 February 2026. System runs entirely on publicly available data with zero API keys required for core analysis.*
