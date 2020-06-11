
import os
import pandas as pd
from pathlib import Path
import yaml

from thales.utils import DIR_PACKAGE_DATA, DIR_SCRAPED_DATA, DIR_SYMBOLS, InvalidSource, sp500


SOURCES_PATH = os.path.join(DIR_PACKAGE_DATA, "sources.yaml")
DIR_CREDENTIALS = os.path.join(DIR_PACKAGE_DATA, "credentials")


def available_sources():
    """Get the list of currently registered data sources."""
    with open(SOURCES_PATH, "r") as stream:
        data = yaml.safe_load(stream)
        return list() if not data else data["sources"]


def validate_source(src: str):
    """Returns a data source only if it is valid else raises InvalidSource."""
    if src in available_sources():
        return src
    else:
        raise InvalidSource(src)


def register_source(src: str):
    """Register a new data source to be developed within the package - creates
    the required directories for storing data, symbols etc."""
    assert isinstance(src, str) and src, f"src must be valid str"
    sources = available_sources()
    assert src not in sources, f"src already registered: {src}"
    sources.append(src)
    with open(SOURCES_PATH, "w") as stream:
        yaml.safe_dump({"sources": sources}, stream)

    credentials_fp = os.path.join(DIR_CREDENTIALS, f"{src}.yaml")
    if not os.path.exists(credentials_fp):
        Path(credentials_fp).touch()

    scrape_dir = os.path.join(DIR_SCRAPED_DATA, src)
    if not os.path.isdir(scrape_dir):
        os.mkdir(scrape_dir)

    symbols_dir = os.path.join(DIR_SYMBOLS, src)
    if not os.path.isdir(symbols_dir):
        os.mkdir(symbols_dir)
    master_symbols = os.path.join(symbols_dir, "master.yaml")
    if not os.path.exists(master_symbols):
        Path(master_symbols).touch()


def get_credentials(src: str):
    """Load stored API/website credentials for the specified source."""
    src = validate_source(src)
    credentials_fp = os.path.join(DIR_CREDENTIALS, f"{src}.yaml")
    with open(credentials_fp) as stream:
        credentials = yaml.safe_load(stream)
    return dict() if not credentials else credentials


def save_credentials(src: str, **credentials):
    """Save new credentials for the specified API/website sources. Any key-value
    pair can be saved as a credential for a source."""
    saved = get_credentials(src)
    credentials = {**saved, **credentials}
    credentials_fp = os.path.join(DIR_CREDENTIALS, f"{src}.yaml")
    with open(credentials_fp, "w") as stream:
        yaml.safe_dump(credentials, stream)


class Symbols:
    """API for controlling the target symbols associated to a data source.
    """
    def __init__(self, src: str = None):
        if src:
            self.fp = os.path.join(DIR_SYMBOLS, validate_source(src))
            self.scraped_fp = os.path.join(DIR_SCRAPED_DATA, src)
        else:
            self.fp = DIR_SYMBOLS
            self.scraped_fp = None  # There is no master directory of scraped data.

    def get_path(self, filename: str = "master"):
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

    def get(self, filename: str = "master"):
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

    def prioritize_for_scraping(self, *sym, filename: str = "master",
                                additional_scrape_dirs: list = None):
        """Prioritize symbols for scraping in order:

        1) Symbols which have never been scraped.
        2) Symbols which have been scraped before, in from the least recently
           scraped to the most.

        Args:
            sym (str): stock symbols.
            filename (str): the file containing the stock symbols (only used if
                no symbols are passed).
            additional_scrape_dirs (list: str): subdirectores in internal
                database of scraped data to calculate prioritization.
        """
        if not sym:
            sym = self.get(filename=filename)
        if not additional_scrape_dirs:
            additional_scrape_dirs = list()
        scrape_dir = os.path.join(self.scraped_fp, *additional_scrape_dirs)
        scraped = os.listdir(scrape_dir)
        modified = [os.path.getmtime(os.path.join(scrape_dir, f)) for f in scraped]
        order_df = pd.DataFrame(data={"scraped": scraped, "modified": modified})
        order_df.sort_values(by=["modified"], ascending=False, inplace=True)
        last = [i.replace(".csv", "") for i in order_df["scraped"].to_list()]
        first = [s for s in sym if s not in last]
        return first + last


MasterSymbols = Symbols()
