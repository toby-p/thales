"""All the currently implemented endpoints with the required/optional parameters
and default values where possible."""

endpoints = {
    "TIME_SERIES_DAILY_ADJUSTED": {
        "required": {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": None,
            "apikey": None,
        },
        "optional": {
            "outputsize": ["full", "compact"],
            "datatype": ["json", "csv"],
        }
    },
    "CURRENCY_EXCHANGE_RATE": {
        "required": {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": None,
            "to_currency": None,
            "apikey": None,
        },
        "optional": {
        }
    },
    "FX_INTRADAY": {
        "required": {
            "function": "FX_INTRADAY",
            "from_symbol": None,
            "to_symbol": None,
            "interval": ["1min", "5min", "15min", "30min", "60min"],
            "apikey": None,
        },
        "optional": {
            "outputsize": ["full", "compact"],
            "datatype": ["json", "csv"],
        }
    },
    "FX_DAILY": {
        "required": {
            "function": "FX_DAILY",
            "from_symbol": None,
            "to_symbol": None,
            "apikey": None,
        },
        "optional": {
            "outputsize": ["full", "compact"],
            "datatype": ["json", "csv"],
        }
    },
    "FX_WEEKLY": {
        "required": {
            "function": "FX_WEEKLY",
            "from_symbol": None,
            "to_symbol": None,
            "apikey": None,
        },
        "optional": {
            "datatype": ["json", "csv"],
        }
    },
    "FX_MONTHLY": {
        "required": {
            "function": "FX_MONTHLY",
            "from_symbol": None,
            "to_symbol": None,
            "apikey": None,
        },
        "optional": {
            "datatype": ["json", "csv"],
        }
    },
}
