import matplotlib.pyplot as plt


class RedfishPlotter:
    def __init__(self, hostname, dataframes):
        self._dataframes = dataframes
        self._hostname = hostname

    def plot_power(self, savefilename=""):
        """Plot the power graphs

        If the savefilename is empty, use hostname_power.png - can't use
        "self" here as a default value
        """

        # Setup default savefilename - can't use self in the default
        if savefilename == "":
            savefilename = f"{self._hostname}_power.png"

        df = self._dataframes["Power"]
        df.plot(
            title=f"Power Draws {self._hostname}",
            ylabel="Power (Watts)",
            fontsize=9,
        )
        plt.savefig(savefilename, dpi=140)
