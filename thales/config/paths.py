"""Paths to directories and files that can be imported."""

import os


DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DIR_PACKAGE_DATA = os.path.join(DIR, "package_data")
DIR_SCRAPED_DATA = os.path.join(DIR_PACKAGE_DATA, "scraped_data")
DIR_SYMBOLS = os.path.join(DIR_PACKAGE_DATA, "symbols")
DIR_CREDENTIALS = os.path.join(DIR_PACKAGE_DATA, "credentials")
DIR_FIELDMAPS = os.path.join(DIR_PACKAGE_DATA, "fieldmaps")
DIR_FX = os.path.join(DIR_PACKAGE_DATA, "fx")
DIR_TOY_DATA = os.path.join(DIR, "data", "toy_datasets")
DIR_TEMP = os.path.join(DIR_PACKAGE_DATA, "temp")
DIR_POSITIONS_OPEN = os.path.join(DIR_PACKAGE_DATA, "positions", "open")
DIR_POSITIONS_CLOSED = os.path.join(DIR_PACKAGE_DATA, "positions", "closed")
DIR_BOT_DATA = os.path.join(DIR_PACKAGE_DATA, "bot_data")
DIR_BOT_CODE = os.path.join(DIR, "bots")
DIR_NOTIFICATIONS = os.path.join(DIR_PACKAGE_DATA, "notifications")
DIR_LOGS = os.path.join(DIR_PACKAGE_DATA, "logs")
