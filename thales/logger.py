
import datetime
import logging
import os
import pytz

from thales.config.paths import DIR_LOGS
from thales.config.utils import get_file_modified_date


utc_now = pytz.utc.localize(datetime.datetime.utcnow())
time_stamp = utc_now.astimezone(pytz.timezone("US/Eastern")).strftime("%Y_%m_%d %H;%M;%S")
logpath = os.path.join(DIR_LOGS, f"thales_log ({time_stamp}).log")
log_format = "[%(asctime)-15s] [%(levelname)08s] [%(funcName)s] %(message)s [line %(lineno)d]"

try:
    logging.basicConfig(level=logging.DEBUG, filename=logpath, format=log_format)
except FileNotFoundError:
    os.mkdir(DIR_LOGS)
logger = logging.getLogger("air")


def wipe_logs(before_year=None, before_month=None):
    """Delete all log files from before the year-month combination indicated. By
    default removes all logs prior to the current month."""
    if not before_year and not before_month:
        now = datetime.datetime.now()
        before_year, before_month = now.year, now.month
    dt = datetime.datetime(year=before_year, month=before_month, day=1)
    log_files = [f for f in os.listdir(DIR_LOGS) if "thales_log" in f and f.endswith(".log")]
    log_files = {f: get_file_modified_date(os.path.join(DIR_LOGS, f)) for f in log_files}
    log_files = [k for k, v in log_files.items() if v < dt]
    i = 0
    for i, f in enumerate(log_files):
        path = os.path.join(DIR_LOGS, f)
        os.remove(path)
    print(f"Deleted {i:,} log files.")
