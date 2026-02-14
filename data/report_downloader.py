"""
Annual Report Downloader
========================
Downloads and caches Annual Report PDFs from BSE India links
provided by the scraper.

Features:
  • Smart caching — only downloads once per symbol+year
  • Downloads latest N years (configurable)
  • Handles BSE India rate limits
  • Progress reporting
"""
import os
import time
import requests


class ReportDownloader:
    """Download and manage annual report PDFs."""

    CACHE_DIR = "./output/reports"
    HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/pdf,*/*',
    }

    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or self.CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def download_reports(self, symbol: str, report_links: dict,
                         latest_n: int = 3) -> list:
        """
        Download the latest N annual reports for a symbol.

        Args:
            symbol:       Company symbol (e.g. 'TCS')
            report_links: {date_str: url} from scraper.annualReports()
            latest_n:     How many recent reports to download

        Returns:
            List of dicts: [{'year': '2025', 'path': '...pdf', 'url': '...'}, ...]
        """
        if not report_links:
            print("  ⚠ No annual report links available.")
            return []

        # Sort by date descending (latest first)
        sorted_links = sorted(report_links.items(), key=lambda x: x[0],
                              reverse=True)[:latest_n]

        results = []
        for date_str, url in sorted_links:
            year = date_str[:4]
            filename = f"{symbol}_AR_{year}.pdf"
            filepath = os.path.join(self.cache_dir, filename)

            # Check cache
            if os.path.exists(filepath) and os.path.getsize(filepath) > 10_000:
                print(f"  ✔ {filename} — cached ({self._human_size(filepath)})")
                results.append({
                    'year': year,
                    'path': filepath,
                    'url': url,
                    'cached': True,
                })
                continue

            # Download
            print(f"  ⏳ Downloading {filename} …", end=" ", flush=True)
            try:
                resp = requests.get(url, headers=self.HEADERS, timeout=60,
                                    stream=True)
                if resp.status_code == 200:
                    content = resp.content
                    if len(content) > 10_000:  # Sanity check
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        print(f"✔ ({self._human_size(filepath)})")
                        results.append({
                            'year': year,
                            'path': filepath,
                            'url': url,
                            'cached': False,
                        })
                    else:
                        print(f"⚠ Too small ({len(content)} bytes), skipped")
                else:
                    print(f"⚠ HTTP {resp.status_code}")
            except Exception as e:
                print(f"⚠ Error: {e}")

            # Be polite — BSE may rate-limit
            time.sleep(1)

        return results

    # ------------------------------------------------------------------
    def list_cached(self, symbol: str) -> list:
        """List all cached PDFs for a symbol."""
        cached = []
        prefix = f"{symbol}_AR_"
        for fname in sorted(os.listdir(self.cache_dir)):
            if fname.startswith(prefix) and fname.endswith('.pdf'):
                path = os.path.join(self.cache_dir, fname)
                year = fname.replace(prefix, '').replace('.pdf', '')
                cached.append({
                    'year': year,
                    'path': path,
                    'size': os.path.getsize(path),
                })
        return cached

    # ------------------------------------------------------------------
    @staticmethod
    def _human_size(filepath: str) -> str:
        size = os.path.getsize(filepath)
        if size > 1_000_000:
            return f"{size / 1_000_000:.1f} MB"
        return f"{size / 1_000:.0f} KB"
