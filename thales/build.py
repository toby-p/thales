"""Script to build out directories for storing IO data."""

import os
from pathlib import Path


def create_structure(structure: object, base_dir=None):
    if isinstance(structure, dict):
        for key, value in structure.items():
            new_dir = os.path.join(base_dir, key)
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)
            next_base_dir = os.path.join(base_dir, key)
            create_structure(structure=value, base_dir=next_base_dir)
    elif isinstance(structure, str):
        if ("." in structure) and (not structure.startswith(".")):
            filetype = structure.split(".")[-1]
            if filetype == "yaml":
                fp = os.path.join(base_dir, structure)
                if not os.path.exists(fp):
                    Path(fp).touch()
            else:
                raise NotImplementedError("Only yaml files implemented")
        else:
            new_dir = os.path.join(base_dir, structure)
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)
    elif isinstance(structure, list):
        for obj in structure:
            create_structure(obj, base_dir=base_dir)


io_structure = {
    ".thales_IO": [
        "bot_data",
        "credentials",
        "fieldmaps",
        "fx_pairs",
        "logs",
        {"notifications": [
            "gmail.yaml",
            "telegram.yaml"
        ]},
        {"positions": [
            "open",
            "closed"
        ]},
        "scraped_data",
        {"stocks": [
            "master.yaml"]
        },
        "temp",
        "bots.yaml",
        "sources.yaml"
    ]
}
io_dir = os.path.expanduser("~")
create_structure(structure=io_structure, base_dir=io_dir)
