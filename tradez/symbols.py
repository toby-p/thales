
import os
import yaml

from tradez.utils import DIR_PACKAGE_DATA, sp500


DIR_SYMBOLS = os.path.join(DIR_PACKAGE_DATA, "symbols")
SYMBOLS_MASTER_PATH = os.path.join(DIR_SYMBOLS, "master.yaml")


def get_symbols_master():
    """Open the master list of symbols."""
    with open(SYMBOLS_MASTER_PATH, "r") as stream:
        data = yaml.safe_load(stream)
        return list() if not data else data["symbols"]


def add_master_symbol(*sym: str):
    """Add stock symbols to the master list (doesn't validate to check if
    symbols are valid stocks)."""
    sym = [str.upper(s) for s in sym]
    master = get_symbols_master()
    new = sorted(set(sym) - set(master))
    if new:
        new_master = sorted(set(sym) | set(master))
        with open(SYMBOLS_MASTER_PATH, "w") as stream:
            yaml.safe_dump({"symbols": new_master}, stream)
        print(f"Added symbols to target list:\n{', '.join(new)}")


def remove_master_symbol(*sym: str, clear_all: bool = False):
    """Remove stock symbols from the master list."""
    if sym:
        sym = [str.upper(s) for s in sym]
    elif clear_all:
        sym = get_symbols_master()
    else:
        return
    master = get_symbols_master()
    remove = sorted(set(sym) & set(master))
    if remove:
        new_master = sorted(set(master) - set(sym))
        with open(SYMBOLS_MASTER_PATH, "w") as stream:
            yaml.safe_dump({"symbols": new_master}, stream)
        print(f"Removed symbols from target list:\n{', '.join(remove)}")


def add_sp500():
    symbols = sorted(sp500()["Symbol"].unique())
    add_master_symbol(*symbols)
