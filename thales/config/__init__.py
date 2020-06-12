
import os
from pathlib import Path
import yaml

from thales.utils import DIR_PACKAGE_DATA, DIR_SCRAPED_DATA, DIR_SYMBOLS, InvalidSource, sp500


SOURCES_PATH = os.path.join(DIR_PACKAGE_DATA, "sources.yaml")
DIR_CREDENTIALS = os.path.join(DIR_PACKAGE_DATA, "credentials")
DIR_FIELDMAPS = os.path.join(DIR_PACKAGE_DATA, "fieldmaps")


default_fieldmap = {
    "datetime": "DATETIME",
    "symbol": "SYMBOL",
    "open": "OPEN",
    "high": "HIGH",
    "low": "LOW",
    "close": "CLOSE",
    "volume": "VOLUME"
}


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
    assert isinstance(src, str) and src, f"Source must be valid str"
    sources = available_sources()
    if src not in sources:
        sources.append(src)
    with open(SOURCES_PATH, "w") as stream:
        yaml.safe_dump({"sources": sources}, stream)

    fieldmap_fp = os.path.join(DIR_FIELDMAPS, f"{src}.yaml")
    if not os.path.exists(fieldmap_fp):
        Path(fieldmap_fp).touch()
        with open(fieldmap_fp, "w") as stream:
            yaml.safe_dump(default_fieldmap, stream)

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


def get_fieldmap(src: str):
    """Load stored fieldmap for the specified API/website source."""
    src = validate_source(src)
    fieldmap_fp = os.path.join(DIR_FIELDMAPS, f"{src}.yaml")
    with open(fieldmap_fp) as stream:
        fieldmap_fp = yaml.safe_load(stream)
    return fieldmap_fp


def set_fieldmap(src: str, **fieldmap):
    """Save new field mapping for the specified API/website sources."""
    saved = get_fieldmap(src)
    assert all([k in saved for k in fieldmap]), "Invalid fieldmap keys"
    fieldmap = {**saved, **fieldmap}
    fieldmap_fp = os.path.join(DIR_FIELDMAPS, f"{src}.yaml")
    with open(fieldmap_fp, "w") as stream:
        yaml.safe_dump(fieldmap, stream)



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


MasterSymbols = Symbols()
