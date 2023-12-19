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

    def plot_power_sensors(self, savefilename="", filter=True):
        """Plot the power sensor graphs"""

        if savefilename == "":
            savefilename = f"{self._hostname}_power_sensors.png"

        df = self._dataframes["Sensors"]

        if filter:
            df = self.filter(df)

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
        temperature_columns = [col for col in df.columns if "temp" in col.lower()]
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
        plt.savefig(savefilename, dpi=144)

    def filter(self, df):
        """
        If # columns <= 4 return df as is, otherwise
        retain only the columns that have values that reach at least 60% of
        the column with the second highest max value
        """

        if len(df.columns) <= 4:
            return df

        max_values = df.max()
        penultimate_max = sorted(max_values)[-2]
        threshold = 0.6 * penultimate_max

        # I suspect that there's a more elegant approach to this, but I can't find it...

        # Get a series of bools where the index is the column name
        pass_max = max_values > threshold

        # Get a list of the column names
        idx = pass_max.index

        # Filter the list of column names if pass_max is true for column
        pass_cols = [idx[i] for i, v in enumerate(pass_max)]

        # return dataframe with the passing columns
        return df[pass_cols]
