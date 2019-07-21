import os
import yaml


DIR = os.path.dirname(os.path.realpath(__file__))
DIR_DATA = os.path.join(DIR, "datastore")
DIR_META = os.path.join(DIR, "metadata")
DIR_SCRAPERS = os.path.join(DIR, "scrapers")

SYMBOL_PATH = os.path.join(DIR_META, "symbols.yaml")
with open(SYMBOL_PATH, "r") as stream:
    SYMBOLS = yaml.safe_load(stream)["symbols"]
