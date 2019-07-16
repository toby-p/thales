import os


DIR = os.path.abspath(__name__).split(__name__)[0]
DATA = os.path.join(DIR, "data")
DATA_COLLECT = os.path.join(DIR, "data_collection")
SYM = os.path.join(DATA_COLLECT, "symbols.yaml")
