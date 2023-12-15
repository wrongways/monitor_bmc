import json
from pathlib import Path
from time import monotonic
import concurrent.futures
from redfish import redfish_client
import pandas as pd
import matplotlib.pyplot as plt
from cli_parser import parse_cli


REDFISH_BASE = '/redfish/v1'
HTTP_OK_200 = 200

class Collector:
    def __init__(self, bmc_hostname, bmc_username, bmc_password):
        '''Sets up the bmc client - DOES NOT save the credentials'''
        bmc_url = f'https://{bmc_hostname}'
        print(f'Connecting to {bmc_url} ...')
        self.bmc = redfish_client(bmc_url, bmc_username, bmc_password)
        print('... connected')
        self.boards = {}
        self.sensors = {}
        self.bmc.login(auth='session')
        print('Logged in')

        self.init_boards()
        self.find_power_sensors()

    def init_boards(self):
        self.motherboard_path = None
        chassis_path = REDFISH_BASE + '/Chassis'
        response = self.bmc.get(chassis_path)
        if response.status == HTTP_OK_200:
            response_data = json.loads(response.text)
            paths = [member['@odata.id'] for member in response_data['Members']]
            for path in paths:
                name = Path(path).name
                if name.lower() in {'motherboard', 'self', 'gpu_board'}:
                    self.boards[name] = {
                        'power': {}
                    }

    def find_power_sensors(self):
        power_sensors = []
        for board in self.boards:
            response = self._redfish_get(f'{REDFISH_BASE}/Chassis/{board}/Sensors')
            sensors = [s.get('@odata.id', '') for s in response.get('Members', {})]
            power_sensors += [s for s in sensors if 'pwr' in s.lower() or 'power' in s.lower()]


        for sensor in power_sensors:
            sensor_name = Path(sensor).name
            self.sensors[sensor_name] = {
                'path': sensor,
                'readings': {},
                # TODO: can get this from ReadingType returned by sensor
                'kind': 'POWER'
            }

    def sample_power(self, runtime_secs=300):
        start_time = monotonic()
        while monotonic() < start_time + runtime_secs:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Start the load operations and mark each future with its board_path
                future_to_board = {
                    executor.submit(self.get_power, f'{REDFISH_BASE}/Chassis/{board}'): board
                    for board in self.boards
                }
                for future in concurrent.futures.as_completed(future_to_board):
                    board = future_to_board[future]
                    try:
                        power = future.result()
                    except Exception as e:
                        print(f'{board} generated an exception: {e}')
                    else:
                        time_delta = monotonic() - start_time
                        self.boards[board]['power'][time_delta] = power
                        # print(f'{time_delta:8.1f}  {board:<10}: {power:6.1f} Watts')


    def sample_sensors(self, runtime_secs=300):
        start_time = monotonic()
        while monotonic() < start_time + runtime_secs:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.sensors)) as executor:
                future_to_sensor = {
                    executor.submit(self.read_sensor, sensor): sensor for sensor in self.sensors
                }
                time_delta = monotonic() - start_time # All samples have same timestamp
                for future in concurrent.futures.as_completed(future_to_sensor):
                    sensor = future_to_sensor[future]
                    try:
                        reading = future.result()
                    except Exception as e:
                        print(f'Sensor {sensor} generated an exception: {e}')
                    else:
                        self.sensors[sensor]['readings'][time_delta] = reading
                        # print(f'{time_delta:8.1f}  {sensor:<25}: {reading:6.1f} Watts')

    def read_sensor(self, sensor):
        sensor_path = self.sensors[sensor]['path']
        response = self._redfish_get(sensor_path)
        return response["Reading"] # Need to ensure that this is standardized

    def get_power(self, board_path):
        data = self._redfish_get(f'{board_path}/Power')
        return data.get('PowerControl', [{}])[0].get('PowerConsumedWatts')

    def _redfish_get(self, path):
        # print(f'GETing: {path}')
        response = self.bmc.get(path)
        if response.status == HTTP_OK_200:
            return json.loads(response.text)
        return None


    def sensor_readings_to_df(self):
        # Merge the readings
        readings = {sensor: self.sensors[sensor]['readings'] for sensor in self.sensors}
        return pd.DataFrame(readings)


    def plot_sensors(self, save_file=None):
        df = self.sensor_readings_to_df()
        df.plot()
        plt.save('plot.png')


if __name__ == '__main__':
    args = parse_cli()
    collector = Collector(
        args.bmc_hostname,
        args.bmc_username,
        args.bmc_password
    )

    # collector.sample_power(10, 1)
    # for board in collector.boards:
    #     boardname = Path(board).name
    #     print(boardname)
    #     print('\t', collector.boards[board]['power'])

    for sensor, info in collector.sensors.items():
        print(f"{sensor:>25} {info['path']}")

    collector.sample_sensors(20)
    for sensor in collector.sensors:
        print(sensor)
        print("=" * len(sensor), end='\n\n')

        for timestamp, reading in collector.sensors[sensor]['readings'].items():
            print(f"{timestamp:6.1f} {reading: 5.1f}")

    print(collector.sensor_readings_to_df())
