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
        self._bmc.login(auth="session")

        self.init_boards()
        self.init_sensors()

    def init_boards(self):
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

    def init_sensors(self):
        sensors = []
        for board in self._boards:
            response = self._redfish_get(f"{REDFISH_BASE}/Chassis/{board}/Sensors")
            sensors += [s.get("@odata.id", "") for s in response.get("Members", {})]

        for sensor in sensors:
            name = Path(sensor).name.lower()
            if "temp" in name:
                self._sensors[name] = {
                    "path": sensor,
                    "kind": "THERMAL",
                    "readings": {},
                }
            elif ("pwr" in name or "power" in name) and not name.startswith("cpu"):
                self._sensors[name] = {"path": sensor, "kind": "POWER", "readings": {}}

    @property
    def sensors(self):
        return list(self._sensors)

    @property
    def power_sensors(self):
        return [s for s, a in self._sensors.items() if a["kind"] == "POWER"]

    def sample_power(self):
        start_time = monotonic()
        while monotonic() < start_time + self.collect_duration:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Start the load operations and mark each future with its board_path
                future_to_board = {
                    executor.submit(
                        self.get_power, f"{REDFISH_BASE}/Chassis/{board}"
                    ): board
                    for board in self._boards
                }
                for future in concurrent.futures.as_completed(future_to_board):
                    board = future_to_board[future]
                    try:
                        power = future.result()
                    except Exception as e:
                        print(f"{board} generated an exception: {e}")
                    else:
                        time_delta = monotonic() - start_time
                        self._boards[board]["power"][time_delta] = power
                        # print(f'{time_delta:8.1f}  {board:<10}: {power:6.1f} Watts')

    def sample_sensors(self, collect_duration):
        start_time = monotonic()
        while monotonic() < start_time + collect_duration:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self._sensors)
            ) as executor:
                future_to_sensor = {
                    executor.submit(self.read_sensor, sensor): sensor
                    for sensor in self.power_sensors
                }
                time_delta = monotonic() - start_time  # All samples have same timestamp
                for future in concurrent.futures.as_completed(future_to_sensor):
                    sensor = future_to_sensor[future]
                    try:
                        reading = future.result()
                    except Exception as e:
                        print(f"Sensor {sensor} generated an exception: {e}")
                    else:
                        self._sensors[sensor]["readings"][time_delta] = reading
                        # print(f'{time_delta:8.1f}  {sensor:<25}: {reading:6.1f} Watts')

    def read_sensor(self, sensor):
        sensor_path = self._sensors[sensor]["path"]
        response = self._redfish_get(sensor_path)
        return response["Reading"]  # Need to ensure that this is standardized

    def get_power(self, board_path):
        data = self._redfish_get(f"{board_path}/Power")
        return data.get("PowerControl", [{}])[0].get("PowerConsumedWatts")

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
        df = pd.DataFrame(readings)
        df.index.name = "Timestamp"
        return df

    def plot_sensors(self, save_file="plot.png"):
        df = self.sensor_readings_to_df()
        df.plot(title=f"Power Draws {self.bmc_hostname}")
        plt.savefig(save_file, dpi=140)

    def save_to_excel(self, filename="sensors.xlsx"):
        self.sensor_readings_to_df().to_excel(filename)

    def max_power_values(self):
        power_cols = [s for s in self.sensors if 'power' in s or 'pwr' in s]
        df = self.sensor_readings_to_df()[power_cols]
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

    print("Sensors:")
    for sensor in collector.sensors:
        print(f"\t{sensor}")

    collector.sample_sensors(args.collect_duration)

    print(collector.sensor_readings_to_df())
    host = args.bmc_hostname.replace("bmc", "")
    collector.plot_sensors(f"{host}_plot.png")
    collector.save_to_excel(f"{host}_sensors.xlsx")
    collector.max_power_values()
