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
        plt.savefig(savefilename, dpi=144)

    def plot_power_sensors(self, savefilename=""):
        """Plot the power sensor graphs"""

        if savefilename == "":
            savefilename = f"{self._hostname}_power_sensors.png"

        df = self._dataframes["Sensors"]
        power_columns = [
            col for col in df.columns if "pwr" in col.lower() or "power" in col.lower()
        ]
        df = df[power_columns]
        print(df.head())

        df.plot(
            title=f"Power Draws {self._hostname}",
            ylabel="Power (Watts)",
            fontsize=9,
        )
        plt.savefig(savefilename, dpi=144)


    def plot_temperature_sensors(self, savefilename=""):
        """Plot the power graphs"""

        # Setup default savefilename
        if savefilename == "":
            savefilename = f"{self._hostname}_temperature_sensors.png"

        df = self._dataframes["Sensors"]
        temperature_columns = [
            col for col in df.columns if "temp" in col.lower()
        ]
        df = df[temperature_columns]

        df.plot(
            title=f"Temperature Sensors {self._hostname}",
            ylabel="Temp (ºC)",
            fontsize=8,
        )
        plt.savefig(savefilename, dpi=144)

    def plot_temperatues(self, savefilename=""):
        if savefilename == "":
            savefilename = f"{self._hostname}_temperatures.png"

        self._dataframes["Temperatures"].plot(
            title=f"Temperatures {self._hostname}", ylabel="Temp (ºC)", fontsize=8
        )
