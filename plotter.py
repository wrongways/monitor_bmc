import matplotlib.pyplot as plt


class RedfishPlotter:
    def __init__(self, dataframes):
        self._dataframes = dataframes

    def plot_power(self, savefilename):
        df = self._dataframes["Power"]
        df.plot(
            title=f"Power Draws {self.bmc_hostname}",
            ylabel="Power (Watts)",
            fontsize=9,
        )
        plt.savefig(savefilename, dpi=140)
