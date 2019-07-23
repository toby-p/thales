import pandas as pd
import requests
import yaml

from .nav import SYMBOL_PATH
from .query import Query
from .scrape import Get


class Tradez:

    Get = Get
    Query = Query

    def __init__(self):
        pass

    @staticmethod
    def add_symbol(*sym: str):
        """Add stock symbols to the saved list. Doesn't validate to check if
        symbols are valid stocks.
        """
        sym = [str.upper(s) for s in sym]
        with open(SYMBOL_PATH) as stream:
            symbols = yaml.safe_load(stream)["symbols"]
        add = sorted(set(sym) - set(symbols))
        symbols = sorted(set(symbols) | set(sym))
        with open(SYMBOL_PATH, "w") as stream:
            yaml.safe_dump({"symbols": symbols}, stream)
        print(f"Added symbols to target list: {', '.join(add)}")

    @staticmethod
    def remove_symbol(*sym: str):
        """Remove stock symbols from the saved list.
        """
        sym = [s.upper() for s in sym]
        with open(SYMBOL_PATH) as stream:
            symbols = yaml.safe_load(stream)["symbols"]
        remove = sorted(set(sym) & set(symbols))
        symbols = sorted(set(symbols) - set(sym))
        with open(SYMBOL_PATH, "w") as stream:
            yaml.safe_dump({"symbols": symbols}, stream)
        print(f"Removed symbols from target list: {', '.join(remove)}")

    @property
    def symbols(self):
        with open(SYMBOL_PATH, "r") as stream:
            return yaml.safe_load(stream)["symbols"]

    @staticmethod
    def s_and_p_500():
        """Pandas DataFrame of S&P500 constituents scraped from: https://datahub.io/core
        """
        url = "https://pkgstore.datahub.io/core/s-and-p-500-companies/" \
              "constituents_json/data/64dd3e9582b936b0352fdd826ecd3c95/constituents_json.json"
        r = requests.get(url)
        assert r.status_code == 200, f"Unable to get S&P500 from url: {url}"
        return pd.DataFrame(r.json())

    @staticmethod
    def add_s_and_p_500_to_symbols():
        snp500 = sorted(Tradez.s_and_p_500()["Symbol"])
        Tradez.add_symbol(*snp500)
