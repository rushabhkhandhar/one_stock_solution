"""
Data Ingestion Pipeline
-----------------------
Pulls structured financial data from Screener.in via the scraper,
resolves symbols, and converts everything into clean Pandas DataFrames.
"""
import pandas as pd
import numpy as np
import datetime
import re
import os
import time
import tempfile
from bs4 import BeautifulSoup
from screenerScraper import ScreenerScrape

try:
    import fitz  # PyMuPDF — for extracting text from transcript PDFs
except ImportError:
    fitz = None


class DataIngestion:

    def __init__(self):
        self.scraper = ScreenerScrape()

    # ------------------------------------------------------------------
    # Symbol Resolution
    # ------------------------------------------------------------------
    def resolve_symbol(self, query: str) -> tuple:
        """Resolve a stock name / symbol to (symbol, BSE token)."""
        query = query.strip().upper()

        # Exact symbol match
        token = self.scraper.getBSEToken(query)
        if token:
            return query, token

        # Fuzzy match on symbol or name
        df = self.scraper.tokendf
        mask = (
            df['symbol'].str.upper().str.contains(query, na=False) |
            df['name'].str.upper().str.contains(query, na=False)
        )
        matches = df[mask]
        if not matches.empty:
            row = matches.iloc[0]
            return str(row['symbol']), str(row['token'])

        raise ValueError(
            f"Could not find BSE token for '{query}'. "
            "Check the symbol/name and try again."
        )

    # ------------------------------------------------------------------
    # TTM EPS computation
    # ------------------------------------------------------------------
    def _compute_ttm_eps(self, quarterly_raw: dict) -> float:
        """
        Compute Trailing Twelve Months EPS from quarterly data.

        1. If the scraper provides a 'TTM' row, use its EPS directly.
        2. Otherwise, sum the EPS of the last 4 quarters.
        """
        import numpy as np

        # Method 1: TTM row from scraper
        if 'TTM' in quarterly_raw:
            for entry in quarterly_raw['TTM']:
                if isinstance(entry, dict):
                    eps_val = entry.get('EPSinRs')
                    if eps_val is not None and not isinstance(eps_val, str):
                        return float(eps_val)

        # Method 2: Sum last 4 quarters' EPS
        keys = [k for k in quarterly_raw.keys() if k != 'TTM']
        keys.sort()
        if len(keys) >= 4:
            last_4 = keys[-4:]
            total_eps = 0.0
            count = 0
            for k in last_4:
                for entry in quarterly_raw[k]:
                    if isinstance(entry, dict) and 'EPSinRs' in entry:
                        val = entry['EPSinRs']
                        if val is not None and not isinstance(val, str):
                            total_eps += float(val)
                            count += 1
                            break
            if count == 4:
                return round(total_eps, 2)

        return None  # Cannot compute TTM EPS

    # ------------------------------------------------------------------
    # Dict → DataFrame conversion
    # ------------------------------------------------------------------
    def _dict_to_dataframe(self, raw: dict) -> pd.DataFrame:
        """
        Convert the scraper's {date_str: [{key: val}, ...]} format
        into a proper DataFrame (dates as index, metrics as columns).
        """
        records = {}
        for date_key, entries in raw.items():
            if date_key == "TTM":
                continue  # TTM captured separately via _extract_ttm
            row = {}
            for entry in entries:
                for k, v in entry.items():
                    if not isinstance(v, str):   # skip href links
                        row[k] = v
            records[date_key] = row

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame.from_dict(records, orient='index')
        # Strip \xa0 (non-breaking space) from column names
        df.columns = [c.replace('\xa0', '').strip() for c in df.columns]
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        # Ensure numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    # ------------------------------------------------------------------
    # Price data parsing
    # ------------------------------------------------------------------
    def _parse_price_data(self, price_data) -> pd.DataFrame:
        """Parse the chart API response into a price DataFrame.

        Actual format from screener.in:
            {'datasets': [
                {'metric': 'Price', 'values': [['2005-04-01', '181.37'], ...], ...},
                {'metric': 'DMA50', 'values': ...},
                {'metric': 'DMA200', 'values': ...},
                {'metric': 'Volume', 'values': [['2005-04-01', 7312110, {'delivery': 39}], ...], ...},
            ]}
        """
        if not price_data:
            return pd.DataFrame()
        try:
            if isinstance(price_data, dict) and 'datasets' in price_data:
                datasets = price_data['datasets']
                if not isinstance(datasets, list) or len(datasets) == 0:
                    return pd.DataFrame()

                # Find the Price dataset
                price_ds = None
                volume_ds = None
                for ds in datasets:
                    if isinstance(ds, dict):
                        metric = ds.get('metric', '').lower()
                        if metric == 'price':
                            price_ds = ds
                        elif metric == 'volume':
                            volume_ds = ds

                if price_ds and 'values' in price_ds:
                    rows = price_ds['values']
                    df = pd.DataFrame(rows, columns=['date', 'close'])
                    df['date'] = pd.to_datetime(df['date'])
                    df['close'] = pd.to_numeric(df['close'], errors='coerce')
                    df = df.set_index('date')

                    # Attach volume if available
                    if volume_ds and 'values' in volume_ds:
                        vol_rows = []
                        for row in volume_ds['values']:
                            vol_rows.append({'date': row[0], 'volume': row[1]})
                        vdf = pd.DataFrame(vol_rows)
                        vdf['date'] = pd.to_datetime(vdf['date'])
                        vdf = vdf.set_index('date')
                        df = df.join(vdf, how='left')

                    return df
        except Exception as e:
            print(f"  ⚠ Could not parse price data: {e}")
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Concall Transcript Text Downloader
    # ------------------------------------------------------------------
    def _download_transcripts(self, concall_links: dict,
                               max_transcripts: int = 4,
                               symbol: str = '') -> list:
        """
        Download concall transcript PDFs (from BSE India) and extract text.

        The scraper returns links like:
          https://www.bseindia.com/xml-data/corpfiling/AttachHis/<guid>.pdf

        We download each PDF, extract text with PyMuPDF, and return as
        plain-text strings for qualitative analysis.

        Parameters:
            concall_links : {date_str: pdf_url} from scraper
            max_transcripts : max transcripts to download (latest first)
            symbol : stock ticker used to namespace the transcript cache
                     so that different companies don't share cached files.

        Returns:
            list of transcript text strings (latest first)
        """
        if not concall_links:
            return []

        if fitz is None:
            print("      ⚠ PyMuPDF (fitz) not installed — cannot parse transcript PDFs")
            return []

        transcripts = []
        cache_dir = os.path.join('.', 'output', 'transcripts')
        os.makedirs(cache_dir, exist_ok=True)

        # Sort by date descending (latest first); handle unknown_ keys
        sorted_dates = sorted(
            concall_links.keys(),
            key=lambda d: d if not d.startswith('unknown') else '0000',
            reverse=True
        )

        for date_key in sorted_dates[:max_transcripts]:
            url = concall_links[date_key]
            if not url:
                continue

            # Cache filename: prefix with symbol to prevent cross-company
            # cache collisions (e.g., RELIANCE_2025_04_01.txt)
            safe_name = re.sub(r'[^\w\-]', '_', date_key)
            sym_prefix = re.sub(r'[^\w]', '', symbol.upper()) if symbol else ''
            cache_file = os.path.join(
                cache_dir,
                f'{sym_prefix}_{safe_name}.txt' if sym_prefix else f'{safe_name}.txt'
            )

            # Check cache first
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    text = f.read()
                if text and len(text) > 200:
                    text = self._clean_transcript(text)
                    transcripts.append(text)
                    print(f"      ✔ Transcript {date_key}: "
                          f"{len(text):,} chars (cached)")
                    continue

            try:
                time.sleep(0.5)  # polite crawl delay

                # Download the PDF
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/126.0.0.0 Safari/537.36',
                    'Accept': 'application/pdf,*/*',
                    'Referer': 'https://www.bseindia.com/',
                }
                resp = self.scraper.reqSession.request(
                    'GET', url, headers=headers, timeout=30)

                if resp.status_code != 200:
                    print(f"      ⚠ Transcript {date_key}: HTTP {resp.status_code}")
                    continue

                content_type = resp.headers.get('Content-Type', '')

                # --- PDF file ---
                if url.lower().endswith('.pdf') or 'pdf' in content_type.lower():
                    # Write to temp file and extract with PyMuPDF
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                        tmp.write(resp.content)
                        tmp_path = tmp.name

                    try:
                        doc = fitz.open(tmp_path)
                        pages_text = []
                        for page in doc:
                            pages_text.append(page.get_text('text'))
                        doc.close()
                        text = '\n'.join(pages_text)
                    finally:
                        os.unlink(tmp_path)

                # --- HTML page (fallback) ---
                else:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    text_div = (
                        soup.find('div', class_='concall-text') or
                        soup.find('div', class_='sub') or
                        soup.find('div', id='transcript')
                    )
                    if text_div:
                        text = text_div.get_text(separator='\n', strip=True)
                    else:
                        main = soup.find('main') or soup.find('body')
                        paragraphs = main.find_all('p') if main else []
                        text = '\n\n'.join(
                            p.get_text(strip=True) for p in paragraphs
                            if len(p.get_text(strip=True)) > 30
                        )

                if text and len(text) > 200:
                    # Clean up excessive whitespace
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    text = self._clean_transcript(text)
                    transcripts.append(text)
                    # Cache to disk
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        f.write(text)
                    print(f"      ✔ Transcript {date_key}: "
                          f"{len(text):,} chars")
                else:
                    print(f"      ⚠ Transcript {date_key}: too short "
                          f"({len(text) if text else 0} chars), skipped")

            except Exception as e:
                print(f"      ⚠ Transcript {date_key} error: {e}")

        return transcripts

    @staticmethod
    def _clean_transcript(text: str) -> str:
        """Delegate to text_intelligence's transcript cleaner."""
        try:
            from qualitative.text_intelligence import clean_transcript_noise
            return clean_transcript_noise(text)
        except Exception:
            return text

    # ------------------------------------------------------------------
    # Master loader
    # ------------------------------------------------------------------
    def load_company(self, query: str, consolidated: bool = True) -> dict:
        """
        Load ALL available financial data for a company.

        Returns a dict with keys:
            symbol, token, quarterly, pnl, balance_sheet, cash_flow,
            ratios, shareholding, price, annual_reports
        """
        symbol, token = self.resolve_symbol(query)

        print(f"\n{'═'*60}")
        print(f"   Loading data for {symbol}  (BSE: {token})")
        print(f"{'═'*60}")

        self.scraper.loadScraper(token, consolidated=consolidated)

        print("  [1/11] Quarterly results …")
        quarterly_raw = self.scraper.quarterlyReport(withAddon=True)        # Extract TTM EPS from quarterly data
        ttm_eps = self._compute_ttm_eps(quarterly_raw)
        print("  [2/11] Profit & Loss …")
        pnl_raw = self.scraper.pnlReport(withAddon=True)

        print("  [3/11] Balance Sheet …")
        bs_raw = self.scraper.balanceSheet(withAddon=True)

        print("  [4/11] Cash Flow …")
        cf_raw = self.scraper.cashFLow(withAddon=True)

        print("  [5/11] Financial Ratios …")
        ratios_raw = self.scraper.ratios()

        print("  [6/11] Shareholding Pattern …")
        shp_raw = self.scraper.shareHolding(quarterly=False, withAddon=True)

        print("  [7/11] Price History …")
        price_raw = self.scraper.closePrice()

        print("  [8/11] Annual Report Links …")
        try:
            annual_reports = self.scraper.annualReports()
        except Exception:
            annual_reports = {}

        print("  [9/11] Concall Transcripts …")
        try:
            concall_links = self.scraper.concallTranscript()
        except Exception:
            concall_links = {}

        # Download actual transcript TEXT from the URLs
        concall_texts = []
        if concall_links:
            print("    Downloading transcript text …")
            concall_texts = self._download_transcripts(
                concall_links, max_transcripts=4, symbol=symbol)

        print("  [10/11] Corporate Announcements (1 yr) …")
        try:
            to_date = datetime.datetime.now()
            from_date = to_date - datetime.timedelta(days=365)
            announcements = self.scraper.corporateAnnouncements(from_date, to_date)
        except Exception:
            announcements = []

        print("  [11/11] Market context (Nifty, macro) …")
        from data.realtime_feeds import RealtimeFeeds
        feeds = RealtimeFeeds()
        macro = feeds.macro_indicators() if feeds.available else {}
        beta_info = feeds.estimate_beta(symbol) if feeds.available else {}

        data = {
            'symbol':          symbol,
            'token':           token,
            'quarterly':       self._dict_to_dataframe(quarterly_raw),
            'pnl':             self._dict_to_dataframe(pnl_raw),
            'balance_sheet':   self._dict_to_dataframe(bs_raw),
            'cash_flow':       self._dict_to_dataframe(cf_raw),
            'ratios':          self._dict_to_dataframe(ratios_raw),
            'shareholding':    self._dict_to_dataframe(shp_raw),
            'price':           self._parse_price_data(price_raw),
            'annual_reports':  annual_reports,
            'concall_links':   concall_links,
            'concall_texts':   concall_texts,
            'announcements':   announcements,
            'macro':           macro,
            'beta_info':       beta_info,
            'ttm_eps':         ttm_eps,
        }

        print(f"\n  ✔ Data loaded successfully for {symbol}.")
        return data
