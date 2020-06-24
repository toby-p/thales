
import os
import pandas as pd
import requests
import sys
import time
import warnings

from thales.dataset import DataSet
from thales.config.exceptions import custom_format_warning, InvalidApiCall
from thales.config.utils import PASS, DIR_SCRAPED_DATA, FAIL
from thales.config import get_credentials, Symbols


warnings.formatwarning = custom_format_warning


class RateLimitExceeded(Exception):
    pass


class AlphaVantageUnknownError(Exception):
    pass


class AlphaVantage:
    """Class for getting data from www.alphavantage.co
    Requires a free API key which can be obtained here:

        https://www.alphavantage.co/support/#api-key
    """
    name = "alphavantage"
    Symbols = Symbols(src=name)
    base_url = "https://www.alphavantage.co/"
    data_dir = os.path.join(DIR_SCRAPED_DATA, name)
    default_function = "TIME_SERIES_DAILY_ADJUSTED"
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    limit_exceeded = {
        "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 "
                "calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a "
                "higher API call frequency."
    }

    @staticmethod
    def construct_query(symbol: str, api_key: str, function: str = None,
                        **kwargs):
        default_kw = dict(interval="5min", outputsize="full", datatype="json")
        kw = {**default_kw, **kwargs}
        if not function:
            function = AlphaVantage.default_function
        query = f"{AlphaVantage.base_url}query?function={function}&symbol={symbol}&apikey={api_key}"
        for k, v in kw.items():
            query += f"&{k}={v}"
        return query

    @staticmethod
    def get(symbol: str, api_key: str = None, function: str = None, **kwargs):
        """Submit a get request to the api. Valid parameters can be passed as
        kwargs and will be added to the API request. See documentation here:

            https://www.alphavantage.co/documentation/
        """
        if not api_key:
            api_key = get_credentials(AlphaVantage.name)["key"]
        if not function:
            function = AlphaVantage.default_function
        symbol = "".join(symbol.split())  # Remove any whitespace.
        symbol = str.upper(symbol)
        query = AlphaVantage.construct_query(symbol, api_key, function, **kwargs)
        r = requests.get(query)
        json_data = r.json()
        if len(json_data) == 1:
            if json_data == AlphaVantage.limit_exceeded:
                raise RateLimitExceeded
            elif list(json_data.keys())[0] == "Error Message":
                error = json_data["Error Message"]
                raise InvalidApiCall(f"{error}\n\nsymbol: {symbol}\nfunction: {function}")
        else:
            return r

    @staticmethod
    def scrape(*sym: str, api_key: str = None, filename: str = "master",
               function: str = None, rate_limit_pause: int = 10, **kwargs):
        """Iterate through the stocks passed as `symbol` and save the data in
        CSV files in the `scraped_data` directory.

        Args:
            sym (str): valid stock ticker symbols. If not passed all symbols
                stored in the given filename for this source will be iterated.
            api_key (str): authentication key.
            filename (str): internal symbol directory to get symbols from.
            function (str): type of data to collect - see:
                https://www.alphavantage.co/documentation/
            rate_limit_pause (int): number of seconds to wait before trying
                again when encountering rate limits.
        """
        if not api_key:
            api_key = get_credentials(AlphaVantage.name)["key"]
        if not function:
            function = AlphaVantage.default_function
        target = os.path.join(AlphaVantage.data_dir, function)
        if not os.path.isdir(target):
            os.mkdir(target)
        if not sym:
            sym = AlphaVantage.Symbols.get(filename=filename)
        sym = AlphaVantage.prioritize(*sym, function=function)
        if sym:
            print(f"Scraping {AlphaVantage.name}:")
        rl_fail_msg = f"{FAIL} RateLimitExceeded: trying again every {rate_limit_pause:,} seconds\r"
        inv_fail_msg = f"{FAIL} InvalidApiCall: bad symbol or function ({function})\n"
        for s in sym:
            s = str.upper(s)
            r, n = None, 0
            msg = f"- {s}"
            while not r:
                sys.stdout.write(msg + " " * (len(rl_fail_msg) + 5))
                try:
                    r = AlphaVantage.get(symbol=s, api_key=api_key, function=function, **kwargs)
                except RateLimitExceeded:
                    sys.stdout.write(f"\r{msg}: {rl_fail_msg}")
                    time.sleep(rate_limit_pause)  # Pause if rate limit has been exceeded.
                    continue  # Loop will go forever until requests accepted again.
                except InvalidApiCall:
                    # Assume invalid symbol/function and move on to next symbol:
                    sys.stdout.write(f"\r{msg}: {inv_fail_msg}")
                    break

            if r:
                data = r.json()
                keys = [i for i in data.keys() if i != "Meta Data"]
                df = pd.DataFrame(data[keys[0]]).T
                df.reset_index(inplace=True)
                df.rename(columns={"index": "DateTime"}, inplace=True)
                df["DateTime"] = pd.to_datetime(df["DateTime"])
                df["SYMBOL"] = s
                n = len(df)
                fp = os.path.join(target, f"{s}.csv")
                if os.path.exists(fp):
                    old = DataSet.load_by_symbol(s, src=AlphaVantage.name, subdir=function, standard_fields=False)
                    df = df.append(old, sort=False)
                    df.drop_duplicates(keep="first", inplace=True)
                df.to_csv(fp, encoding="utf-8", index=False)
                sys.stdout.write(f"\r{msg}: {PASS} {n:,} datapoints\n")

    @staticmethod
    def scraped(function: str = None):
        """Pandas DataFrame of stock symbols and dates they were scraped."""
        if not function:
            function = AlphaVantage.default_function
        scrape_dir = os.path.join(AlphaVantage.data_dir, function)
        symbol = [f.replace(".csv", "") for f in os.listdir(scrape_dir) if f.endswith(".csv")]
        modified = [os.path.getmtime(os.path.join(scrape_dir, f"{f}.csv")) for f in symbol]
        df = pd.DataFrame(data={"symbol": symbol, "modified": modified})
        df["modified"] = pd.to_datetime(df["modified"], unit="s")
        return df.sort_values(by=["modified"], ascending=False).reset_index(drop=True)

    @staticmethod
    def prioritize(*sym, function: str = None):
        """Prioritize symbols for scraping in order:

        1) Symbols which have never been scraped.
        2) Symbols which have been scraped before, in from the least recently
           scraped to the most.
        """
        if not sym:
            sym = AlphaVantage.Symbols.get(filename="master")
        if not function:
            function = AlphaVantage.default_function
        scraped = AlphaVantage.scraped(function)
        scraped = scraped.loc[(scraped["symbol"].isin(sym))]
        scraped.sort_values(by=["modified"], ascending=True, inplace=True)
        last = scraped["symbol"].to_list()
        first = [s for s in sym if s not in last]
        return first + last
