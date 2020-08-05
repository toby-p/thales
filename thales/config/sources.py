
import os
from pathlib import Path
import yaml

from thales.config.exceptions import InvalidSource
from thales.config.paths import DIR_CREDENTIALS, DIR_FIELDMAPS, DIR_FX, DIR_PACKAGE_DATA, DIR_SCRAPED_DATA, DIR_SYMBOLS
from thales.config.utils import DEFAULT_FIELDMAP


SOURCES_PATH = os.path.join(DIR_PACKAGE_DATA, "sources.yaml")
DEFAULT_SRC = "alphavantage"


def available_sources() -> list:
    """Get the list of currently registered data sources."""
    with open(SOURCES_PATH, "r") as stream:
        data = yaml.safe_load(stream)
        return list() if not data else data["sources"]


SRCS = available_sources()


def validate_source(src: str = None, valid_sources: list = None) -> str:
    """Returns a data source only if it is in the valid list of sources, else
    raises InvalidSource. If `src` isn't passed returns the package default
    source (`alphavantage`). If `valid_sources` isn't passed it checks all
    currently available sources."""
    if not valid_sources:
        valid_sources = SRCS
    if not src:
        src = DEFAULT_SRC
    if src in valid_sources:
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
            yaml.safe_dump(DEFAULT_FIELDMAP, stream)

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

    fx_dir = os.path.join(DIR_FX, src)
    if not os.path.isdir(fx_dir):
        os.mkdir(fx_dir)
    master_fx = os.path.join(fx_dir, "master.yaml")
    if not os.path.exists(master_fx):
        Path(master_fx).touch()