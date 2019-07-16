import os
import pandas as pd
import requests
import yaml

from nav import DATA, SYM

with open(SYM, "r") as stream:
    SYMBOLS = yaml.safe_load(stream)["symbols"]

data_dir = os.path.join(DATA, "alphavantage")
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
        """Method to get data from the alphavantage api. Valid api parameters can be passed as keyword args and will be
        added to the constructed API request. See documentation here:
            https://www.alphavantage.co/documentation/
        """
        query = AlphaVantage.construct_query(symbol, api_key, function, **kwargs)
        return requests.get(query)

    @staticmethod
    def scrape(api_key: str, function: str = "TIME_SERIES_DAILY_ADJUSTED", **kwargs):
        """Iterate through the stocks saved in the `symbols` YAML file, and collect the data returned by the query
        constructed via the function & kwargs. Data will be saved in CSV files in the `data` directory.
        """
        target = os.path.join(DATA, "alphavantage", function)
        if not os.path.isdir(target):
            os.mkdir(target)

        for s in SYMBOLS:
            query = AlphaVantage.construct_query(symbol=s, api_key=api_key, function=function, **kwargs)
            data = requests.get(query).json()
            keys = list(data.keys())
            try:
                keys.remove("Meta Data")
            except ValueError:
                pass
            key = keys[0]
            df = pd.DataFrame(data[key]).T
            df.to_csv(os.path.join(target, f"{s}.csv"), encoding="utf-8")
