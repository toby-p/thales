"""Manage credentials for data sources."""

import yaml

from thales.config.paths import io_path
from thales.config.sources import validate_source


def get_credentials(src: str) -> dict:
    """Load stored API/website credentials for the specified source."""
    src = validate_source(src)
    credentials_fp = io_path("credentials", filename=f"{src}.yaml")
    with open(credentials_fp) as stream:
        credentials = yaml.safe_load(stream)
    return dict() if not credentials else credentials


def save_credentials(src: str, **credentials):
    """Save new credentials for the specified API/website sources. Any key-value
    pair can be saved as a credential for a source."""
    saved = get_credentials(src)
    credentials = {**saved, **credentials}
    credentials_fp = io_path("credentials", filename=f"{src}.yaml")
    with open(credentials_fp, "w") as stream:
        yaml.safe_dump(credentials, stream)
