
import pandas as pd

from thales.config.paths import package_path
from thales.data.csv_loader import CSVLoader


def save_toy_dataset(df: pd.DataFrame, name: str, **kwargs):
    """Save a CSV file to the toy_datasets directory."""
    fp = package_path("data", "toy_datasets", filename=f"{name}.csv")
    df.to_csv(fp, encoding=kwargs.get("encoding", "utf-8"), index=kwargs.get("index", False))
    print(f"Dataset saved: {fp}")


def load_toy_dataset(name: str, **kwargs):
    name = f"{name}.csv" if "." not in name else name
    fp = package_path("data", "toy_datasets", filename=f"{name}")
    df = pd.read_csv(fp, encoding=kwargs.get("encoding", "utf-8"), **kwargs)
    for c in df.columns:
        if "date" in c:
            df[c] = pd.to_datetime(df[c])
    return df
