"""Manage credentials for notification systems."""

import os
import yaml

from thales.config.paths import DIR_NOTIFICATIONS


def get_credentials(system: str, username: str) -> dict:
    """Load stored credentials for the specified notification system."""
    credentials_fp = os.path.join(DIR_NOTIFICATIONS, f"{system}.yaml")
    if os.path.exists(credentials_fp):
        with open(credentials_fp) as stream:
            credentials = yaml.safe_load(stream)
    else:
        credentials = None
    return dict() if not credentials else credentials[username]


def save_credentials(system: str, username: str, **credentials):
    """Save new credentials for the specified notification system. Any key-value
    pair can be saved as a credential for a source."""
    saved = get_credentials(system, username)
    credentials = {username: {**saved, **credentials}}
    credentials_fp = os.path.join(DIR_NOTIFICATIONS, f"{system}.yaml")
    with open(credentials_fp, "w") as stream:
        yaml.safe_dump(credentials, stream)
