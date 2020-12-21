
import os
import requests

from thales.config.credentials import get_credentials
from thales.config.exceptions import InvalidApiCall, RateLimitExceeded
from thales.config.paths import io_path
from thales.scrapers.endpoints import ENDPOINTS


# Rate limit message for AlphaVantage:
_av_rl_msg = \
    "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day. "\
    "Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency."


class _BaseScraper:
    """Generic scraper to get data from any implemented endpoint."""

    def __init__(self, name: str = "alphavantage",
                 base_url: str = "https://www.alphavantage.co/",
                 default_endpoint: str = "TIME_SERIES_DAILY_ADJUSTED",
                 **kwargs):
        self.name = name
        self.base_url = base_url
        self.endpoints = ENDPOINTS[self.name]
        self.data_dir = io_path("scraped_data", self.name)
        if not os.path.isdir(self.data_dir):
            os.mkdir(self.data_dir)
        assert default_endpoint in self.endpoints, f"Invalid endpoint: {default_endpoint}"
        self.default_endpoint = default_endpoint
        self.endpoint_dirs = dict()
        self.rate_limit_msg = kwargs.get("rate_limit_msg", dict(Note=_av_rl_msg))
        self.api_key = kwargs.get("api_key", get_credentials(self.name)["key"])

    def endpoint_data_dir(self, endpoint: str = None):
        """Get full filepath to the directory where scraped data is stored for
        the given endpoint."""
        if endpoint is None:
            endpoint = self.default_endpoint
        try:
            return self.endpoint_dirs[endpoint]
        except KeyError:
            assert endpoint in self.endpoints, f"Invalid enpoint: {endpoint}"
            endpoint_dir = os.path.join(self.data_dir, endpoint)
            if not os.path.isdir(endpoint_dir):
                os.mkdir(endpoint_dir)
            self.endpoint_dirs[endpoint] = endpoint_dir
            return endpoint_dir

    def construct_query(self, endpoint: str, **kwargs):
        assert endpoint in self.endpoints, f"Invalid enpoint: {endpoint}"
        query = f"{self.base_url}query?function={endpoint}&apikey={self.api_key}"
        keywords = self.endpoints[endpoint]
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

    def get(self, endpoint: str, **kwargs):
        """Submit a get request to the api. Valid parameters can be passed as
        kwargs and will be added to the API request. See documentation here:

            https://www.alphavantage.co/documentation/
        """
        query = self.construct_query(endpoint=endpoint, **kwargs)
        r = requests.get(query)
        json_data = r.json()
        if len(json_data) == 1:
            if json_data == self.rate_limit_msg:
                raise RateLimitExceeded
            elif list(json_data.keys())[0] == "Error Message":
                error = json_data["Error Message"]
                details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                raise InvalidApiCall(f"{error}\n\nendpoint={endpoint}\napi_key={self.api_key}\nkwargs={details}")
        else:
            return r
