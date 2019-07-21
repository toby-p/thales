import yaml

from .nav import SYMBOL_PATH
from .query import Query
from .scrape import Get


class Tradez:

    Get = Get
    Query = Query

    def add_symbol(*sym):
        """Add stock symbols to the saved list. Doesn't validate to check if 
        symbols are valid stocks.
        """
        with open(SYMBOL_PATH) as stream:
            symbols = yaml.safe_load(stream)["symbols"]
        new = sorted(set(sym) - set(symbols))
        symbols = sorted(set(symbols) | set([str.upper(s) for s in sym]))
        with open(SYMBOL_PATH, "w") as stream:
            yaml.safe_dump({"symbols": symbols}, stream)
        print(f"Added symbols to target list: {', '.join(new)}")
