
import os
import requests

from thales.config.credentials import get_credentials
from thales.config.exceptions import InvalidApiCall, RateLimitExceeded
from thales.config.paths import io_path
from thales.scrapers.alphavantage.endpoints import endpoints


class AlphaVantageScraper:
    """Generic scraper to get data from any implemented endpoint."""

    name = "alphavantage"
    endpoints = endpoints
    base_url = "https://www.alphavantage.co/"
    data_dir = io_path("scraped_data", name)
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    rate_limit_msg = {
        "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 "
                "calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a "
                "higher API call frequency."
    }

    @staticmethod
    def construct_query(endpoint: str, api_key: str = None, **kwargs):
        if not api_key:
            api_key = get_credentials(AlphaVantageScraper.name)["key"]
        query = f"{AlphaVantageScraper.base_url}query?function={endpoint}&apikey={api_key}"
        try:
            keywords = endpoints[endpoint]
        except KeyError:
            raise NotImplementedError(f"Endpoint has not yet been implemented: {endpoint}")
        required = {k: v for k, v in keywords["required"].items() if k not in ("function", "apikey")}
        for k, v in required.items():
            v = kwargs.get(k, v)  # Override defaults with passed kwargs.
            if not v:
                raise ValueError(f"Missing required parameter: {k}")
            elif isinstance(v, list):
                v = v[0]
            assert isinstance(v, str), f"Parameter {k} must be type str not type({v})"
            query += f"&{k}={v}"
        optional = keywords["optional"]
        for k, v in optional.items():
            v = kwargs.get(k, v)  # Override defaults with passed kwargs.
            if isinstance(v, list):
                v = v[0]
            assert isinstance(v, str), f"Parameter {k} must be type str not type({v})"
            query += f"&{k}={v}"
        return query

    @staticmethod
    def get(endpoint: str, api_key: str = None, **kwargs):
        """Submit a get request to the api. Valid parameters can be passed as
        kwargs and will be added to the API request. See documentation here:

            https://www.alphavantage.co/documentation/
        """
        if not api_key:
            api_key = get_credentials(AlphaVantageScraper.name)["key"]
        query = AlphaVantageScraper.construct_query(endpoint=endpoint, api_key=api_key, **kwargs)
        r = requests.get(query)
        json_data = r.json()
        if len(json_data) == 1:
            if json_data == AlphaVantageScraper.rate_limit_msg:
                raise RateLimitExceeded
            elif list(json_data.keys())[0] == "Error Message":
                error = json_data["Error Message"]
                details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                raise InvalidApiCall(f"{error}\n\nendpoint={endpoint}\napi_key={api_key}\nkwargs={details}")
        else:
            return r
