import json
from pathlib import Path
from time import monotonic
import concurrent.futures
from redfish import redfish_client
import pandas as pd
import matplotlib.pyplot as plt
from cli_parser import parse_cli


REDFISH_BASE = "/redfish/v1"
HTTP_OK_200 = 200


class Collector:
    def __init__(self, bmc_hostname, bmc_username, bmc_password):
        """Sets up the bmc client"""

        self.bmc_hostname = bmc_hostname
        bmc_url = f"https://{bmc_hostname}"
        print(f"Connecting to {bmc_url} ...")
        self._bmc = redfish_client(bmc_url, bmc_username, bmc_password)
        self._boards = {}
        self._sensors = {}
        self._power = {}
        self._power_power_supplies = {}
        self._thermal_temps = {}
        self._thermal_fans = {}

        self._bmc.login(auth="session")

        self.identify_boards()
        self.identify_sensors()
        self.add_power()
        self.add_thermal()

    def identify_boards(self):
        self.motherboard_path = None
        chassis_path = REDFISH_BASE + "/Chassis"
        response = self._bmc.get(chassis_path)
        if response.status == HTTP_OK_200:
            response_data = json.loads(response.text)
            paths = [member["@odata.id"] for member in response_data["Members"]]
            for path in paths:
                name = Path(path).name
                if name.lower() in {"motherboard", "self", "gpu_board"}:
                    self._boards[name] = {"power": {}}

    def identify_sensors(self):
        sensors = []
        for board in self._boards:
            response = self._redfish_get(f"{REDFISH_BASE}/Chassis/{board}/Sensors")
            sensors += [s.get("@odata.id", "") for s in response.get("Members", {})]

        for sensor in [s for s in sensors if s]:
            name = Path(sensor).name.lower()
            if "temp" in name:
                self._sensors[sensor] = {
                    "name": name,
                    "kind": "THERMAL",
                    "readings": {},
                    "units": None,
                }
            elif ("pwr" in name or "power" in name) and not name.startswith("vr_"):
                self._sensors[sensor] = {
                    "name": name,
                    "kind": "POWER",
                    "readings": {},
                    "units": None,
                }

    def add_power(self):
        for board in self.boards:
            self._power[board] = {
                "name": f"{board} power",
                "kind": "POWER",
                "readings": {},
                "units": "Watts",
            }

        for path in self.power_urls:
            power_resp = self._redfish_get(path)
            if (power_supplies := power_resp.get("PowerSupplies")) is not None:
                for psu in power_supplies:
                    name = psu.get("Name") or psu.get("@odata.id").split("/")[-2:]
                    self._power_power_supplies[name] = {
                        "name": name,
                        "kind": "POWER_SUPPLY",
                        "readings": {},
                        "units": psu.get("Units"),
                    }

    def add_thermal(self):
        pass

    @property
    def sensors(self):
        return list(self._sensors)

    @property
    def power_sensors(self):
        return [s for s, a in self._sensors.items() if a["kind"] == "POWER"]

    @property
    def boards(self):
        return list(self._boards)

    @property
    def power_urls(self):
        return [f"{REDFISH_BASE}/Chassis/{board}/Power" for board in self.boards]

    @property
    def thermal_urls(self):
        return [f"{REDFISH_BASE}/Chassis/{board}/Thermal" for board in self.boards]

    @property
    def collection_urls(self):
        collection_urls = self.sensors + self.power_urls + self.thermal_urls
        return collection_urls

    def collect_samples(self, collect_duration):
        start_time = monotonic()
        while monotonic() < start_time + collect_duration:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.collection_urls)
            ) as executor:
                future_to_sensor = {
                    executor.submit(self._redfish_get(path)): path
                    for path in self.collection_urls
                }
                time_delta = monotonic() - start_time  # All samples have same timestamp
                for future in concurrent.futures.as_completed(future_to_sensor):
                    path = future_to_sensor[future]
                    boardname = path.split("/")[4]
                    try:
                        response = future.result()
                    except Exception as e:
                        print(f"Sensor {path} generated an exception: {e}")
                    else:
                        if "Sensors" in path:
                            self.save_sensor_data(response, time_delta, path)
                        elif path.endswith("Power"):
                            self.save_power_data(response, time_delta, boardname)
                        elif path.endswith("Thermal"):
                            self.save_thermal_data(response, time_delta, boardname)
                        else:
                            print(f"Unexpected path: {path}")



    def save_sensor_data(self, response, time_delta, path):
        reading = response["Reading"] # Standardized ?
        print(f'{time_delta:8.1f}  {sensor:<25}: {reading:6.1f} Watts')
        self._sensors[path]["readings"][time_delta] = reading

    def save_power_data(self, response, time_delta, boardname):
        power = response.get("PowerControl", [{}])[0].get("PowerConsumedWatts")
        self._power[boardname]["readings"][time_delta] = power
        if (power_supplies := response.get("PowerSupplies")) is not None:
            for psu in power_supplies:
                name = psu.get("Name") or psu.get("@odata.id").split("/")[-2:]
                self._power_power_supplies[name]["readings"]["time_delta"] = psu.get(
                    "PowerInputWatts"
                )

    def save_thermal_data(response, time_delta, boardname):
        pass

    def _redfish_get(self, path):
        # print(f'GETing: {path}')
        response = self._bmc.get(path)
        if response.status == HTTP_OK_200:
            return json.loads(response.text)
        return None

    def sensor_readings_to_df(self):
        # Merge the readings
        readings = {
            sensor: self._sensors[sensor]["readings"] for sensor in self._sensors
        }
        print(readings)

        df = pd.DataFrame(readings)
        df.index.name = "Timestamp"
        return df

    def plot_sensors(self, save_file="plot.png"):
        df = self.sensor_readings_to_df()[self.power_sensors]

        df.plot(
            title=f"Power Draws {self.bmc_hostname}",
            ylabel="Power (Watts)",
            fontsize=9,
        )
        plt.savefig(save_file, dpi=140)

    def save_to_excel(self, filename="sensors.xlsx"):
        self.sensor_readings_to_df()[self.power_sensors].to_excel(filename)

    def max_power_values(self):
        df = self.sensor_readings_to_df()[self.power_sensors]
        print("Max readings per sensor")
        for column in df:
            print(f"{column:>25}: {df[column].max():.1f} Watts")

        print(f"\nMax power drawn: {df.max().max():,.1f} Watts\n")


if __name__ == "__main__":
    args = parse_cli()
    collector = Collector(
        args.bmc_hostname,
        args.bmc_username,
        args.bmc_password,
    )

    # collector.sample_power(10, 1)
    # for board in collector.boards:
    #     boardname = Path(board).name
    #     print(boardname)
    #     print('\t', collector.boards[board]['power'])

    # print("Sensors:")
    # for sensor in collector.sensors:
    #     print(f"\t{sensor}")

    collector.collect_samples(args.collect_duration)

    print(collector.sensor_readings_to_df())
    host = args.bmc_hostname.replace("bmc", "").replace("-", "")
    collector.plot_sensors(f"{host}_plot.png")
    collector.save_to_excel(f"{host}_sensors.xlsx")
    collector.max_power_values()
