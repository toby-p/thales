"""The symbols class is used to set target lists of stock symbols associated to
a registered data source."""

import os
from pathlib import Path
import yaml

from thales.config.paths import io_path
from thales.config.sources import validate_source
from thales.config.utils import sp500


class Symbols:
    """API for controlling the target stocks associated to a data source."""
    def __init__(self, src: str = None):
        if src:
            self.fp = io_path("stocks", validate_source(src))
            self.scraped_fp = io_path("scraped_data", validate_source(src))
        else:
            self.fp = io_path("stocks")
            self.scraped_fp = None  # There is no master directory of scraped data.

    def get_path(self, filename: str = "master") -> str:
        """Construct a filepath to a saved yaml symbols list."""
        fp = os.path.join(self.fp, f"{filename}.yaml")
        if os.path.exists(fp):
            return fp
        else:
            print(f"No such symbols list: {fp}\nUse `new_symbols_list({filename})` to create the list.")

    def new_symbol_list(self, filename: str):
        """Create a file to store a new list of symbols."""
        fp = os.path.join(self.fp, f"{filename}.yaml")
        if os.path.exists(fp):
            print(f"Symbols file already exists: {fp}")
        else:
            Path(fp).touch()

    def get(self, filename: str = "master") -> list:
        """Open a list of symbols associated to this instance's source."""
        fp = self.get_path(filename)
        with open(fp, "r") as stream:
            data = yaml.safe_load(stream)
            return list() if not data else data["symbols"]

    def add(self, *sym: str, filename: str):
        """Add stock symbols to a list. Doesn't validate to check if symbols are
        valid stocks."""
        current_symbols = self.get(filename)
        sym = [str.upper(s) for s in sym]
        new = sorted(set(sym) - set(current_symbols))
        if new:
            new_symbols = sorted(set(sym) | set(current_symbols))
            fp = self.get_path(filename)
            with open(fp, "w") as stream:
                yaml.safe_dump({"symbols": new_symbols}, stream)
            print(f"Added symbols to {fp}:\n{', '.join(new)}")

    def remove(self, *sym: str, filename: str, remove_all: bool = False):
        """Remove stock symbols from a list."""
        if sym:
            sym = [str.upper(s) for s in sym]
        elif remove_all:
            sym = self.get(filename)
        else:
            return
        current_symbols = self.get(filename)
        remove = sorted(set(sym) & set(current_symbols))
        if remove:
            new_symbols = sorted(set(current_symbols) - set(sym))
            fp = self.get_path(filename)
            with open(fp, "w") as stream:
                yaml.safe_dump({"symbols": new_symbols}, stream)
            print(f"Removed symbols from {fp}:\n{', '.join(remove)}")

    def add_sp500(self, filename: str = "master"):
        """Add all S&P500 to a symbol list."""
        symbols = sorted(sp500()["Symbol"].unique())
        self.add(*symbols, filename=filename)


MasterSymbols = Symbols()
