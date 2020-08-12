"""A fieldmap maps the custom names of data fields returned by a data source
(e.g. closing price, date, open price, etc) to the standardized names used for
each field throughout the package."""

import os
import pandas as pd
import yaml

from thales.config.exceptions import MissingRequiredColumns
from thales.config.paths import io_path
from thales.config.sources import validate_source
from thales.config.utils import DEFAULT_FIELDMAP


def get_fieldmap(src: str) -> dict:
    """Load stored fieldmap for the specified API/website source."""
    src = validate_source(src)
    fieldmap_fp = io_path("fieldmaps", filename="{src}.yaml")
    with open(fieldmap_fp) as stream:
        fieldmap_fp = yaml.safe_load(stream)
    return fieldmap_fp


def set_fieldmap(src: str, **fieldmap):
    """Save new field mapping for the specified API/website sources."""
    saved = get_fieldmap(src)
    assert all([k in DEFAULT_FIELDMAP for k in fieldmap]), "Invalid fieldmap keys"
    fieldmap = {**saved, **fieldmap}
    fieldmap_fp = io_path("fieldmaps", filename=f"{src}.yaml")
    with open(fieldmap_fp, "w") as stream:
        yaml.safe_dump(fieldmap, stream)


def apply_fieldmap(df: pd.DataFrame, src: str = None,
                   error_missing: bool = True):
    """Map default fieldnames onto a source's custom field names."""
    fieldmap = get_fieldmap(src)
    rename = {v: k for k, v in fieldmap.items() if v in df.columns}
    if error_missing:
        missing = set(rename) - set(df.columns)
        if missing:
            raise MissingRequiredColumns(*missing)
    return df.rename(columns=rename)
