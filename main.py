import pandas as pd
from cli_parser import parse_cli
from collector import Collector
from plotter import RedfishPlotter

args = parse_cli()
collector = Collector(
    args.bmc_hostname,
    args.bmc_username,
    args.bmc_password,
)
host = args.bmc_hostname.replace("bmc", "").replace("-", "")

collector.collect_samples(args.collect_duration)
dataframes = collector.as_dataframes()
plotter = RedfishPlotter(host, dataframes)
plotter.plot_power()
plotter.plot_power_sensors()
plotter.plot_temperature_sensors()
plotter.plot_temperatues()


with pd.ExcelWriter(f"{host}.xlsx", engine="xlsxwriter") as writer:
    for name, df in dataframes.items():
        if len(df) > 0:
            print(f">>>>> Processing sheet: {name} <<<<<")
            print(df.head())
            df.to_csv(f"{host}_{name.lower()}.csv", encoding="utf-8")
            df.to_excel(writer, sheet_name=name.lower())
