
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class GrowthModeller:

    def __init__(self, initial: float = 1000, n: int = 400,
                 flat_percents: list = (0.005, 0.01)):
        self.initial = initial
        self.n = n
        self.df = pd.DataFrame([self.initial for _ in range(self.n)], columns=["initial"])
        for pc in flat_percents:
            pc_series = pd.Series(data=[pc for _ in self.df.index], index=self.df.index)
            self.df[f"flat_pc_{pc}_return"] = self.compound(pc_series)
            self.df[f"flat_pc_-{pc}_return"] = self.compound(-pc_series)

    def compound(self, pc: pd.Series):
        initial = self.df["initial"]
        return (initial * pc.shift(1).add(1).cumprod()).fillna(initial)

    def normal(self, mu=0.01, sigma=0.02):
        col = f"normal_mu{mu}_sigma_{sigma}"
        self.df[f"{col}_pc"] = np.random.normal(mu, sigma, self.n)
        self.df[f"{col}_return"] = self.compound(self.df[f"{col}_pc"])

    def plot(self, *columns):
        if not columns:
            columns = ["initial"] + [c for c in self.df.columns if c.endswith("_return")]
        columns = list(columns)
        fig, ax = plt.subplots(figsize=(10, 8))
        for col in columns:
            ax.plot(self.df[col], label=col)
        ax.legend()


if __name__ == "__main__":
    gm = GrowthModeller(initial=1000, n=400, flat_percents=[0.01])
    gm.normal(mu=0.01, sigma=0.02)
    gm.normal(mu=0.005, sigma=0.02)
    gm.plot()
