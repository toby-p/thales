"""Common variables and utility functions required throughout the package."""

import os
import pandas as pd
from pathlib import Path
import requests
import warnings
import yaml


def custom_format_warning(msg, *args, **kwargs):
    """Monkey-patch warnings to just show relevant info."""
    return str(msg) + "\n"


warnings.formatwarning = custom_format_warning


# List of the valid sites/apis currently available to get data from:
SOURCES = [
    "alphavantage",
    "test"
]

# Absolute directory paths:
DIR = os.path.dirname(os.path.realpath(__file__))
DIR_PACKAGE_DATA = os.path.join(DIR, "package_data")
DIR_SCRAPED_DATA = os.path.join(DIR_PACKAGE_DATA, "scraped_data")
CREDENTIALS_PATH = os.path.join(DIR_PACKAGE_DATA, "credentials", "credentials.yaml")
if not os.path.exists(CREDENTIALS_PATH):
    Path(CREDENTIALS_PATH).touch()

# Unicode symbols for status messages:
PASS, FAIL = "\u2714", "\u2718"


# Functions for loading data:
def get_credentials():
    """Load all currently stored API/website credentials."""
    with open(CREDENTIALS_PATH) as stream:
        return yaml.safe_load(stream)


def save_credentials(source: str, **credentials):
    """Save new credentials for one of the API/website sources."""
    assert source.lower() in SOURCES, f"Invalid data source: {source}"
    saved = get_credentials()
    if not saved:
        saved = dict()
    new = {source: credentials}
    credentials = {**saved, **new}
    with open(CREDENTIALS_PATH, "w") as stream:
        yaml.safe_dump(credentials, stream)


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
