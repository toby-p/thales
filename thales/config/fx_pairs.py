"""The FXPairs class is used to set target lists of pairs of currency symbols
associated to a registered data source."""

import os
from pathlib import Path
import yaml

from thales.config.paths import io_path
from thales.config.sources import validate_source


class FXPairs:
    """API for controlling the target FX currency pairs associated to a data
    source."""
    def __init__(self, src: str = None):
        if src:
            self.fp = io_path("fx_pairs", validate_source(src))
            self.scraped_fp = io_path("scraped_data", validate_source(src))
        else:
            self.fp = io_path("fx_pairs")
            self.scraped_fp = None  # There is no master directory of scraped data.

    def get_path(self, filename: str = "master") -> str:
        """Construct a filepath to a saved yaml currency pairs list."""
        fp = os.path.join(self.fp, f"{filename}.yaml")
        if os.path.exists(fp):
            return fp
        else:
            print(f"No such currency pairs list: {fp}\nUse `new_fx_list({filename})` to create the list.")

    def new_fx_list(self, filename: str):
        """Create a file to store a new list of FX currencies."""
        fp = os.path.join(self.fp, f"{filename}.yaml")
        if os.path.exists(fp):
            print(f"FX file already exists: {fp}")
        else:
            Path(fp).touch()

    def get(self, filename: str = "master") -> list:
        """Open a list of FX currency pairs associated to this instance's
        source."""
        fp = self.get_path(filename)
        with open(fp, "r") as stream:
            data = yaml.safe_load(stream)
            if not data:
                return list()
            else:
                return [tuple(p) for p in data["fx_pairs"]]

    @staticmethod
    def validate_pairs(*pair: tuple):
        """Validate currency pairs to ensure they are tuples of 2 distinct
        strings (doesn't validate to check if symbols are valid currencies."""
        for p in pair:
            assert isinstance(p, tuple), f"Currency pairs must be passed as tuples, not {type(p)}"
            assert len(p) == 2, "Must be exactly 2 currencies in each pair."
            for c in p:
                assert isinstance(c, str), f"Currency must be str, not {type(c)}"
            assert p[0].lower() != p[1].lower(), f"Each currency in a pair must be unique."
        return [(p[0].upper(), p[1].upper()) for p in pair]

    def add(self, *pair: tuple, filename: str):
        """Add currency pairs to a list."""
        current_pairs = self.get(filename)
        pair = self.validate_pairs(*pair)
        new = sorted(set(pair) - set(current_pairs))
        if new:
            all_pairs = sorted(set(pair) | set(current_pairs))
            fp = self.get_path(filename)
            with open(fp, "w") as stream:
                yaml.safe_dump({"fx_pairs": all_pairs}, stream)
            str_new = [f"({p[0]}, {p[1]})" for p in new]
            print(f"Added pairs to {fp}:\n{', '.join(str_new)}")

    def remove(self, *pair: tuple, filename: str, remove_all: bool = False):
        """Remove currency pairs from a list."""
        if pair:
            pair = self.validate_pairs(*pair)
        elif remove_all:
            pair = self.get(filename)
        else:
            return
        current_pairs = self.get(filename)
        remove = sorted(set(pair) & set(current_pairs))
        if remove:
            all_pairs = sorted(set(current_pairs) - set(pair))
            fp = self.get_path(filename)
            with open(fp, "w") as stream:
                yaml.safe_dump({"fx_pairs": all_pairs}, stream)
            str_removed = [f"({p[0]}, {p[1]})" for p in remove]
            print(f"Removed pairs from {fp}:\n{', '.join(str_removed)}")


MasterFXPairs = FXPairs()
