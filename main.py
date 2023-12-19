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

for i, df in dataframes.items():
    if len(df) > 0:
        print(df.head())

        df.to_csv(f"{host}_sensors.csv", encoding="utf-8")
        df.to_excel(f"{host}_sensors.xlsx")
