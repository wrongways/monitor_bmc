import matplotlib.pyplot as plt


class RedfishPlotter:
    def __init__(self, hostname, dataframes):
        self._dataframes = dataframes
        self._hostname = hostname

    def plot_power(self, savefilename=f"{self._hostname}_power.png"):
        df = self._dataframes["Power"]
        df.plot(
            title=f"Power Draws {self._hostname}",
            ylabel="Power (Watts)",
            fontsize=9,
        )
        plt.savefig(savefilename, dpi=140)
