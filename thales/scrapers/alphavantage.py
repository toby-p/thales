
import os
import pandas as pd
import requests
import sys
import time
import warnings

from thales.query import load_data
from thales.utils import PASS, custom_format_warning, DIR_SCRAPED_DATA, FAIL, InvalidApiCall
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
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    limit_exceeded = {
        "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 "
                "calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a "
                "higher API call frequency."
    }

    def __init__(self):
        pass

    @staticmethod
    def construct_query(symbol: str, api_key: str,
                        function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        default_kw = dict(interval="5min", outputsize="full", datatype="json")
        kw = {**default_kw, **kwargs}
        query = f"{AlphaVantage.base_url}query?function={function}&symbol={symbol}&apikey={api_key}"
        for k, v in kw.items():
            query += f"&{k}={v}"
        return query

    @staticmethod
    def get(symbol: str, api_key: str = None,
            function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        """Submit a get request to the api. Valid parameters can be passed as
        kwargs and will be added to the API request. See documentation here:

            https://www.alphavantage.co/documentation/
        """
        if not api_key:
            api_key = get_credentials(AlphaVantage.name)["key"]
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
    def scrape(*symbol: str, api_key: str = None, filename: str = "master",
               function: str = "TIME_SERIES_DAILY_ADJUSTED",
               rate_limit_pause: int = 10, **kwargs):
        """Iterate through the stocks passed as `symbol` and save the data in
        CSV files in the `scraped_data` directory.

        Args:
            symbol (str): valid stock ticker symbols. If not passed then all
                symbols stored in the package will be iterated.
            api_key (str): authentication key.
            filename (str): internal symbol directory to get symbols from.
            function (str): type of data to collect - see:
                https://www.alphavantage.co/documentation/
            rate_limit_pause (int): number of seconds to wait before trying
                again when encountering rate limits.
        """
        if not api_key:
            api_key = get_credentials(AlphaVantage.name)["key"]
        target = os.path.join(AlphaVantage.data_dir, function)
        if not os.path.isdir(target):
            os.mkdir(target)
        symbol = AlphaVantage.Symbols.get(filename=filename) if not symbol else symbol
        symbol = AlphaVantage.Symbols.prioritize_for_scraping(*symbol, additional_scrape_dirs=[function])
        if symbol:
            print(f"Scraping {AlphaVantage.name}:")
        rl_fail_msg = f"{FAIL} RateLimitExceeded: trying again every {rate_limit_pause:,} seconds\r"
        inv_fail_msg = f"{FAIL} InvalidApiCall: bad symbol or function ({function})\n"
        for s in symbol:
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
                    old = load_data(s, src=AlphaVantage.name, subdir=function)
                    df = df.append(old, sort=False)
                    df.drop_duplicates(keep="first", inplace=True)
                df.to_csv(fp, encoding="utf-8", index=False)
                sys.stdout.write(f"\r{msg}: {PASS} {n:,} datapoints\n")
