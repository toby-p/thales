"""Paths to directories and files that can be imported."""

import os
import pandas as pd
from pathlib import Path


DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
IO_DIR = os.path.join(os.path.expanduser("~"), ".thales_IO")


def make_dirs(*subdir, basedir: str = None):
    if not basedir:
        basedir = DIR
    for sd in subdir:
        assert os.path.isdir(basedir), f"basedir doesn't exist: {basedir}"
        basedir = os.path.join(basedir, sd)
        if not os.path.isdir(basedir):
            os.mkdir(basedir)
    return basedir


def make_empty_file(fp):
    if os.path.exists(fp):
        return fp
    filename = os.path.basename(fp)
    filetype = filename.split(".")[-1]
    if filetype in ("yaml", "txt", "py"):
        Path(fp).touch()
    elif filetype == "csv":
        df = pd.DataFrame()
        df.to_csv(fp, encoding="utf-8", index=False)
    else:
        raise NotImplementedError(f"Can't create files with type: {filetype}")
    return fp


def _construct_path(*subdir, filename: str = None, make_subdirs: bool = True,
                    make_file: bool = True, basedir: str = None):
    path = os.path.join(basedir, *subdir)
    if make_subdirs and not os.path.isdir(path):
        make_dirs(*subdir, basedir=basedir)
    if filename:
        path = os.path.join(path, filename)
        if make_file:
            path = make_empty_file(path)
    return path


def io_path(*subdir, filename: str = None, make_subdirs: bool = False,
            make_file: bool = False):
    # Remove empty elements from `subdir` - more flexible for applications:
    subdir = [i for i in subdir if i]
    return _construct_path(*subdir, filename=filename, make_subdirs=make_subdirs, make_file=make_file, basedir=IO_DIR)


def package_path(*subdir, filename: str = None, make_subdirs: bool = False,
                 make_file: bool = False):
    # Remove empty elements from `subdir` - more flexible for applications:
    subdir = [i for i in subdir if i]
    return _construct_path(*subdir, filename=filename, make_subdirs=make_subdirs, make_file=make_file, basedir=DIR)
