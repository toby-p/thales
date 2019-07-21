import os
import pandas as pd
import requests
import time

from ..query.alphavantage import AlphaVantage as Query
from ..nav import DIR_DATA, SYMBOLS

data_dir = os.path.join(DIR_DATA, "alphavantage")
if not os.path.isdir(data_dir):
    os.mkdir(data_dir)


class AlphaVantage:
    """Class for getting data from www.alphavantage.co
    Requires a free API key which can be obtained here:

    https://www.alphavantage.co/support/#api-key
    """
    def __init__(self):
        pass

    @staticmethod
    def construct_query(symbol: str, api_key: str, function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        d = dict(interval="5min", outputsize="full", datatype="json")
        kw = {**d, **kwargs}
        query = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}"
        for k, v in kw.items():
            query += f"&{k}={v}"
        return query

    @staticmethod
    def get(symbol: str, api_key: str, function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        """Method to get datastore from the alphavantage api. Valid api parameters can be passed as keyword args and
        will be added to the constructed API request. See documentation here:
            https://www.alphavantage.co/documentation/
        """
        symbol = str.upper(symbol)
        query = AlphaVantage.construct_query(symbol, api_key, function, **kwargs)
        r = requests.get(query)
        if len(r.json()) == 1 and list(r.json().keys())[0] == "Note":  # API rate limit exceeded.
            return None
        else:
            return r

    @staticmethod
    def scrape(api_key: str, function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        """Iterate through the stocks saved in the `symbols` YAML file, and collect the data returned by the query
        constructed via the function & kwargs. Data will be saved in CSV files in the `datastore` directory.
        """
        target = os.path.join(DIR_DATA, "alphavantage", function)
        if not os.path.isdir(target):
            os.mkdir(target)

        for s in SYMBOLS:
            got_data = False
            while not got_data:
                r = AlphaVantage.get(symbol=s, api_key=api_key, function=function, **kwargs)
                if r:
                    got_data = True
                else:
                    time.sleep(5)  # Pause if the rate limit has been exceeded.

            data = r.json()
            keys = list(data.keys())
            try:
                keys.remove("Meta Data")
            except ValueError:
                pass
            key = keys[0]
            df = pd.DataFrame(data[key]).T
            df.reset_index(inplace=True)
            df.rename(columns={"index": "DateTime"}, inplace=True)
            df["DateTime"] = pd.to_datetime(df["DateTime"])
            df["SYMBOL"] = s
            fp = os.path.join(target, f"{s}.csv")
            if os.path.exists(fp):
                old_df = Query.load(function, s)
                df = df.append(old_df, sort=False)
                df.drop_duplicates(keep="first", inplace=True)

            df.to_csv(fp, encoding="utf-8", index=False)
