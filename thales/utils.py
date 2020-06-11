"""Common variables and utility functions required throughout the package."""

import os
import pandas as pd
import requests
import warnings


def custom_format_warning(msg, *args, **kwargs):
    """Monkey-patch warnings to just show relevant info."""
    return str(msg) + "\n"


warnings.formatwarning = custom_format_warning


# Absolute directory paths:
DIR = os.path.dirname(os.path.realpath(__file__))
DIR_PACKAGE_DATA = os.path.join(DIR, "package_data")
DIR_SCRAPED_DATA = os.path.join(DIR_PACKAGE_DATA, "scraped_data")
DIR_SYMBOLS = os.path.join(DIR_PACKAGE_DATA, "symbols")

# Unicode symbols for status messages:
PASS, FAIL = "\u2714", "\u2718"


def sp500():
    """Pandas DataFrame of S&P500 constituents scraped from:

        https://datahub.io/core
    """
    url = "https://pkgstore.datahub.io/core/s-and-p-500-companies/" \
          "constituents_json/data/64dd3e9582b936b0352fdd826ecd3c95/constituents_json.json"
    r = requests.get(url)
    assert r.status_code == 200, f"Unable to get S&P500 from url: {url}"
    return pd.DataFrame(r.json())


# Custom exceptions:
class InvalidApiCall(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class InvalidSource(Exception):
    def __init__(self, src):
        self.src = src

    def __str__(self):
        return f"Invalid data source: {self.src}"