
import os
import pandas as pd
import requests
import sys
import time
import warnings

from thales.config.credentials import get_credentials
from thales.config.exceptions import custom_format_warning, InvalidApiCall
from thales.config.paths import io_path
from thales.config.fx_pairs import FXPairs
from thales.config.utils import PASS, FAIL, now_str
from thales.data import DataSet


warnings.formatwarning = custom_format_warning


class RateLimitExceeded(Exception):
    pass


class AlphaVantageFX:
    """Class for getting FX data from www.alphavantage.co
    Requires a free API key which can be obtained here:

        https://www.alphavantage.co/support/#api-key
    """
    name = "alphavantage"
    FXPairs = FXPairs(src=name)
    base_url = "https://www.alphavantage.co/"
    data_dir = io_path("scraped_data", name)
    default_function = "FX_INTRADAY"
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    limit_exceeded = {
        "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 "
                "calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a "
                "higher API call frequency."
    }

    @staticmethod
    def construct_query(from_symbol: str, to_symbol: str, api_key: str = None,
                        function: str = None, **kwargs):
        default_kw = dict(interval="1min", outputsize="full", datatype="json")
        kw = {**default_kw, **kwargs}
        if not api_key:
            api_key = get_credentials(AlphaVantageFX.name)["key"]
        if not function:
            function = AlphaVantageFX.default_function
        query = f"{AlphaVantageFX.base_url}query?function={function}"
        if function == "CURRENCY_EXCHANGE_RATE":
            query += f"&from_currency={from_symbol}&to_currency={to_symbol}&apikey={api_key}"
        elif function in ("FX_INTRADAY", "FX_DAILY", "FX_WEEKLY", "FX_MONTHLY"):
            query += f"&from_symbol={from_symbol}&to_symbol={to_symbol}&apikey={api_key}"
        else:
            raise InvalidApiCall(f"Invalid FX API function: {function}")
        for k, v in kw.items():
            query += f"&{k}={v}"
        return query

    @staticmethod
    def get(from_symbol: str, to_symbol: str, api_key: str = None,
            function: str = None, **kwargs):
        """Submit a get request to the api. Valid parameters can be passed as
        kwargs and will be added to the API request. See documentation here:

            https://www.alphavantage.co/documentation/
        """
        from_symbol = str.upper("".join(from_symbol.split()))  # Remove whitespace and make uppercase.
        to_symbol = str.upper("".join(to_symbol.split()))  # Remove whitespace and make uppercase.
        query = AlphaVantageFX.construct_query(from_symbol, to_symbol, api_key=api_key, function=function, **kwargs)
        r = requests.get(query)
        json_data = r.json()
        if len(json_data) == 1:
            if json_data == AlphaVantageFX.limit_exceeded:
                raise RateLimitExceeded
            elif list(json_data.keys())[0] == "Error Message":
                error = json_data["Error Message"]
                raise InvalidApiCall(f"{error}\n\nfrom_symbol: {from_symbol}\nto_symbol: {to_symbol}"
                                     f"\nfunction: {function}")
        else:
            return r

    @staticmethod
    def scrape(*pair: tuple, api_key: str = None, filename: str = "master",
               function: str = None, rate_limit_pause: int = 10, **kwargs):
        """Iterate through the currency pairs passed as `pair` and save the data
        in CSV files in the `scraped_data` directory.

        Args:
            pair (tuple: str): valid currency symbol pairs. If not passed all
                pairs stored in the given filename for this source will be
                iterated.
            api_key (str): authentication key.
            filename (str): internal directory to get pairs from.
            function (str): type of data to collect - see:
                https://www.alphavantage.co/documentation/
            rate_limit_pause (int): number of seconds to wait before trying
                again when encountering rate limits.
        """
        if not function:
            function = AlphaVantageFX.default_function
        target = os.path.join(AlphaVantageFX.data_dir, function)
        if not os.path.isdir(target):
            os.mkdir(target)
        if not pair:
            pair = AlphaVantageFX.FXPairs.get(filename=filename)
        pair = AlphaVantageFX.prioritize(*pair, function=function)
        if pair:
            print(f"Scraping {AlphaVantageFX.name}:")
        rl_fail_msg = f"{FAIL} RateLimitExceeded: trying again every {rate_limit_pause:,} seconds\r"
        inv_fail_msg = f"{FAIL} InvalidApiCall: bad symbol or function ({function})\n"
        for p in pair:
            from_symbol, to_symbol = p[0], p[1]
            pair_name = f"({from_symbol}, {to_symbol})"
            r, n = None, 0
            msg = f"- ({from_symbol}, {to_symbol})"
            while not r:
                sys.stdout.write(msg + " " * (len(rl_fail_msg) + 5))
                try:
                    request_time = now_str()
                    r = AlphaVantageFX.get(from_symbol=from_symbol, to_symbol=to_symbol, api_key=api_key,
                                           function=function, **kwargs)
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
                df["PAIR"] = pair_name
                df["request_time"] = request_time
                n = len(df)
                fp = os.path.join(target, f"{pair_name}.csv")
                if os.path.exists(fp):
                    old = DataSet.load_by_fxpair(pair_name, src=AlphaVantageFX.name, subdir=function)
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
        2) Symbols which have been scraped before, from the least recently
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
