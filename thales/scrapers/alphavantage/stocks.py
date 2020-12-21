
import os
import pandas as pd
import sys
import time
import warnings

from thales.data import CSVLoader
from thales.config.exceptions import custom_format_warning, InvalidApiCall, RateLimitExceeded
from thales.config.fieldmaps import apply_fieldmap
from thales.config.utils import PASS, FAIL, now_str, SECOND_FORMAT
from thales.config.symbols import Symbols
from thales.scrapers.base_scraper import _BaseScraper

warnings.formatwarning = custom_format_warning


class AlphaVantageStocks(_BaseScraper):
    """Class for getting stock data from www.alphavantage.co
    Requires a free API key which can be obtained here:

        https://www.alphavantage.co/support/#api-key
    """

    def __init__(self, **kwargs):
        super().__init__(name="alphavantage",
                         base_url="https://www.alphavantage.co/",
                         default_endpoint="TIME_SERIES_DAILY_ADJUSTED",
                         **kwargs)
        self.Symbols = Symbols(src=self.name)

    def scrape(self, *sym: str, endpoint: str = None,
               rate_limit_pause: int = 10, **kwargs):
        """Iterate through the stocks passed as `symbol` and save the data in
        CSV files in the `scraped_data` directory.

        Args:
            sym: valid stock ticker symbols. If not passed all symbols in the
                given filename for this source will be iterated.
            endpoint: function to query for data - see:
                https://www.alphavantage.co/documentation/
            rate_limit_pause: number of seconds to wait before trying again when
                encountering rate limits.
        """
        if endpoint is None:
            endpoint = self.default_endpoint
        if not sym:
            sym = self.Symbols.get(filename=kwargs.get("filename", None))
        sym = self.prioritize(*sym, endpoint=endpoint)
        if sym:
            print(f"Scraping {self.name}:")
        fail_rl = f"{FAIL} RateLimitExceeded: trying again every {rate_limit_pause:,} seconds\r"
        fail_api = f"{FAIL} InvalidApiCall: bad symbol or function ({endpoint})\n"
        line_len = max([len(fail_rl), len(fail_api)])
        spaces = " " * line_len
        endpoint_dir = self.endpoint_data_dir(endpoint=endpoint)
        for s in sym:
            s = str.upper(s)
            r, n = None, 0
            msg = f"- {s}"
            while not r:
                sys.stdout.write(msg + spaces)
                try:
                    request_time = now_str(SECOND_FORMAT)
                    r = self.get(symbol=s, endpoint=endpoint, **kwargs)
                except RateLimitExceeded:
                    sys.stdout.write(f"\r{msg}: {fail_rl}")
                    time.sleep(rate_limit_pause)  # Pause if rate limit has been exceeded.
                    continue  # Loop will go forever until requests accepted again.
                except InvalidApiCall:
                    # Assume invalid symbol/function and move on to next symbol:
                    sys.stdout.write(f"\r{msg}: {fail_api}")
                    break

            if r:
                json_object = r.json()
                df = self._json_to_dataframe(json_object)
                df["SYMBOL"], df["request_time"] = s, request_time
                fp = os.path.join(endpoint_dir, f"{s}.csv")
                if os.path.exists(fp):
                    old = CSVLoader.load_by_symbol(s, src=self.name, subdir=endpoint)
                    df = df.append(old, sort=False)
                    df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
                df.to_csv(fp, encoding="utf-8", index=False)
                new_rows = df["request_time"].value_counts()[request_time]
                sys.stdout.write(f"\r{msg}: {PASS} {new_rows:,} datapoints\n")

    def _json_to_dataframe(self, json_object) -> pd.DataFrame:
        """Logic to convert JSON returned by the request to a formatted
        DataFrame for saving as CSV."""
        data_key = [i for i in json_object.keys() if i != "Meta Data"][0]
        df = pd.DataFrame(json_object[data_key]).T.reset_index().rename(columns={"index": "DateTime"})
        df["DateTime"] = pd.to_datetime(df["DateTime"])
        for k, v in json_object["Meta Data"].items():
            df[k] = v
        df = apply_fieldmap(df.reset_index(drop=True), src=self.name)
        return df

    def scraped(self, endpoint: str = None):
        """Pandas DataFrame of stock symbols and dates they were scraped."""
        scrape_dir = self.endpoint_data_dir(endpoint)
        scraped_sym = [f.replace(".csv", "") for f in os.listdir(scrape_dir) if f.endswith(".csv")]
        modified = [os.path.getmtime(os.path.join(scrape_dir, f"{f}.csv")) for f in scraped_sym]
        df = pd.DataFrame(data={"symbol": scraped_sym, "modified": modified})
        df["modified"] = pd.to_datetime(df["modified"], unit="s")
        return df.sort_values(by=["modified"], ascending=False).reset_index(drop=True)

    def prioritize(self, *sym, endpoint: str = None, **kwargs):
        """Prioritize symbols for scraping in order:

        1) Symbols which have never been scraped.
        2) Symbols which have been scraped before, from the least recently
           scraped to the most.
        """
        if not sym:
            sym = self.Symbols.get(filename=kwargs.get("filename", None))
        scraped = self.scraped(endpoint)
        scraped = scraped.loc[(scraped["symbol"].isin(sym))]
        scraped.sort_values(by=["modified"], ascending=True, inplace=True)
        last = scraped["symbol"].to_list()
        first = [s for s in sym if s not in last]
        return first + last
