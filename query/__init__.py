from .alphavantage import AlphaVantage


class Query:

    @staticmethod
    def load(company_src: str = "alphavantage", function: str = "TIME_SERIES_DAILY_ADJUSTED", *symbol: str):
        if company_src == "alphavantage":
            return AlphaVantage.load(function=function, *symbol)
