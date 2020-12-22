"""Custom exceptions to raise errors with descriptive messages."""

import warnings


def custom_format_warning(msg, *args, **kwargs):
    """Monkey-patch warnings to just show relevant info."""
    return str(msg) + "\n"


warnings.formatwarning = custom_format_warning


class ThalesException(Exception):
    pass


class InvalidApiCall(ThalesException):
    pass


class RateLimitExceeded(ThalesException):
    pass


class InvalidSource(ThalesException):
    def __init__(self, src):
        self.src = src

    def __str__(self):
        return f"Invalid data source: {self.src}"


class InvalidPriceColumn(ThalesException):
    def __init__(self, col):
        self.col = col

    def __str__(self):
        return f"Invalid price column: {self.col}"


class MissingRequiredColumns(ThalesException):
    def __init__(self, *col):
        self.col = col

    def __str__(self):
        return f"Missing required columns: {', '.join([str(c) for c in sorted(self.col)])}"


class InvalidIndicator(ThalesException):
    def __init__(self, indicator):
        self.indicator = indicator

    def __str__(self):
        return f"Invalid indicator: {self.indicator}"
