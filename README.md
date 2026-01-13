# Screener Scraper

Utilities for downloading financial statements, shareholding data, and announcement feeds from [Screener.in](https://www.screener.in) and the BSE corporate announcement API.  The project exposes two helper classes:

- `stockScreener` – low-level scraper that logs into Screener.in anonymously, parses HTML blocks, and calls companion JSON endpoints.
- `ScreenerScrape` – user-facing wrapper that manages BSE token caching, provides convenience helpers, and mirrors the smoke flow found in `testScreener.py`.

The repository is structured so it can be dropped into any open-source workflow without bundling personal credentials.  All calls rely on public endpoints, but Angel Broking tokens must be refreshed regularly to keep the ticker cache in sync.

## Requirements

- Python 3.10+ (conda or `venv` are both supported)
- Outbound HTTPS access to `screener.in`, `api.bseindia.com`, and `margincalculator.angelbroking.com`
- Packages pinned from the `buildalgos` conda environment:

```
beautifulsoup4==4.14.3
pandas==2.2.3
requests==2.31.0
```

Install them with `pip install -r requirements.txt` inside your virtual environment.

## Quick Start

1. **Create / activate an environment**
   ```powershell
   conda create -n screener-scraper python=3.11 -y
   conda activate screener-scraper
   pip install -r requirements.txt
   ```
   or, if you prefer `venv`, run `python -m venv .venv`, activate it, and `pip install -r requirements.txt`.

2. **Warm the token cache (optional)**
   ```text
   python
   >>> from screenerScraper import ScreenerScrape
   >>> ScreenerScrape().getTokendf()
   >>> exit()
   ```
   The scraper downloads the latest `tokens/tokens_YYYYMMDD.csv` automatically the first time you instantiate `ScreenerScrape`; running the snippet above simply forces a refresh without invoking other endpoints. Keep only the most recent file inside `tokens/`.

3. **Run the smoke script**
   ```powershell
   python testScreener.py
   ```
   The test hits live HTTPS endpoints and may print stack traces if an upstream API returns `None` (for example, when corporate schedules are unavailable for the requested period). Re-run after a few seconds or wrap new tests in try/except blocks to avoid masking other issues.

## Typical Usage

```python
import datetime
from screenerScraper import ScreenerScrape

scraper = ScreenerScrape()
token = scraper.getBSEToken("RELIANCE")  # BSE symbol
scraper.loadScraper(token, consolidated=True)

quarterly = scraper.quarterlyReport(withAddon=True)
pnl = scraper.pnlReport(withAddon=True)
balance_sheet = scraper.balanceSheet(withAddon=True)
cash_flow = scraper.cashFLow(withAddon=True)
ratios = scraper.ratios()
shareholding = scraper.shareHolding(quarterly=False, withAddon=True)
announcements = scraper.corporateAnnouncements(datetime.date(2022, 7, 23), datetime.date(2024, 7, 20))
```

Available entry points mirror the Screener UI:

- `quarterlyReport`, `pnlReport`, `balanceSheet`, `cashFLow`, `ratios`, `shareHolding` – parse Screener tables and optionally enrich each column with data fetched from JSON schedules.
- `closePrice()` and `chart()` – pull daily price and moving-average series.
- `annualReports()` and `concallTranscript()` – scrape document links.
- `corporateAnnouncements(from_date, to_date)` – iterate through BSE pages while respecting the API’s 365-day pagination limit.
- `latestAnnouncements(date=today)` and `upcomingResults()` – quick helpers to poll today’s filings via `requestBSE`.

Invoke `python -i screenerScraper.py` to explore helper methods interactively without re-importing modules.

## Token Management

- Instantiating `ScreenerScrape` triggers a check for `tokens/tokens_YYYYMMDD.csv`; if today’s file is missing it will download a fresh copy automatically before any other call is made.
- Call `ScreenerScrape.getTokendf()` whenever Angel Broking rotates its `OpenAPIScripMaster` dump; delete stale CSVs from `tokens/` afterward.
- Use `ScreenerScrape.getBSEToken("SYMBOL")` to translate a BSE ticker into a numeric token that `loadScraper` and BSE endpoints expect.

## Development Workflow

1. Update dependencies via `requirements.txt` (pin new packages before committing).
2. Run `python -m compileall .` or your editor’s formatter to catch syntax errors.
3. Execute `python testScreener.py` to ensure live endpoints still respond. Because this test suite relies on third-party services, prefer wrapping new assertions with try/except to keep signal-to-noise high.
4. Remove `__pycache__/` artifacts before packaging or zipping the repo.

## Troubleshooting

- **`NoneType` errors while enriching reports** – Upstream schedules occasionally respond with `null`. These show up as `TypeError: 'NoneType' object is not iterable` during `__addonData`. Retry later or guard calls with retries and `if resp` checks when adding new scrapers.
- **Token not found** – Ensure `tokens/tokens_YYYYMMDD.csv` contains the BSE symbol you need. Re-run `getTokendf()` or inspect the CSV with pandas to confirm.
- **Rate limiting / Captcha** – Screener.in may throttle aggressive scraping. Throttle requests with `time.sleep` if you add loops, and keep the default headers intact.
- **Corporate-announcement pagination** – The helper already fetches in 365-day chunks. For longer spans, rely on the returned list or adjust the chunk size in `corporateAnnouncements` if BSE changes its rules.

## Community

- Join the Python Algo Trading community on Telegram: https://t.me/pythonalgodiscussion
- Follow updates on X / Twitter: https://x.com/niraj_munot

Happy scraping!
