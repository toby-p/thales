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

