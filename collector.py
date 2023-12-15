import argparse
import json
from pathlib import Path
from time import time, sleep
import concurrent.futures
from redfish import redfish_client


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
            print(sensor)
            sensor_name = Path(sensor).name
            self.sensors[sensor_name] = {
                'path': sensor,
                'readings': {},
                # TODO: can get this from ReadingType returned by sensor
                'kind': 'POWER'
            }

    def sample_power(self, runtime_secs=300, sample_hz=1):
        start_time = time()
        while time() < start_time + runtime_secs:
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
                        time_delta = time() - start_time
                        self.boards[board]['power'][time_delta] = power
                        print(f'{time_delta:8.1f}  {board:<10}: {power:6.1f} Watts')

            sleep(1/sample_hz)

    def sample_sensors(self, runtime_secs=300, sample_hz=1):
        start_time = time()
        sample_interval = 1/sample_hz
        while sample_start := time() < start_time + runtime_secs:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.sensors)) as executor:
                future_to_sensor = {
                    executor.submit(self.read_sensor, sensor): sensor for sensor in self.sensors
                }
                for future in concurrent.futures.as_completed(future_to_sensor):
                    sensor = future_to_sensor[future]
                    try:
                        reading = future.result()
                    except Exception as e:
                        print(f'Sensor {sensor} generated an exception: {e}')
                    else:
                        time_delta = time() - start_time
                        self.sensors[sensor]['readings'][time_delta] = reading
                        print(f'{time_delta:8.1f}  {sensor:<25}: {reading:6.1f} Watts')

                sleep_time = time() - (sample_start + sample_interval)
                if sleep_time > 0:
                    sleep(sleep_time)

    def get_power(self, board_path):
        data = self._redfish_get(f'{board_path}/Power')
        return data.get('PowerControl', [{}])[0].get('PowerConsumedWatts')

    def _redfish_get(self, path):
        print(f'GETing: {path}')
        response = self.bmc.get(path)
        if response.status == HTTP_OK_200:
            return json.loads(response.text)
        return None

    def plot_power(self, save_file=None):
        pass

    # def __del__(self):
    #     try:
    #         self.bmc.logout()
    #     except Exception:
    #         pass


if __name__ == '__main__':

    def parse_cli():
        parser = argparse.ArgumentParser(
            description='Tool to collect power data from Redfish BMC'
        )

        parser.add_argument('--bmc_hostname', type=str,
                            help='The hostname of the bmc')

        parser.add_argument('--bmc_username', type=str,
                            help='The bmc user/login name')

        parser.add_argument('--bmc_password', type=str,
                            help='Password for the bmc user')

        return parser.parse_args()

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
        for timestamp, reading in collector.sensors[sensor]['readings']:
            print(f"{timestamp:6.1f} {reading: 5.1f}")
