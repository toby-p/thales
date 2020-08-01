

class Position:

    def __init__(self, timestamp: str, price: float, amount: float):
        self.open_timestamp = timestamp
        self.buy_price = price
        self.amount = amount

        self.close_timestamp = None
        self.sell_price = None
        self.sell_buy_ratio = None
        self.delta = None

    @property
    def open(self):
        return not (any([self.close_timestamp, self.sell_price, self.sell_buy_ratio, self.delta]))

    def sell(self, timestamp: str, price: float):
        assert self.open and (not self.sell_price), f"Position already closed."
        self.close_timestamp = timestamp
        self.sell_price = price
        self.sell_buy_ratio = self.sell_price / self.buy_price
        self.delta = (self.amount * self.sell_buy_ratio) - self.amount
