import sqlite3
import json
from pathlib import Path
from time import time
from redfish import redfish_client
from config import SETTINGS


REDFISH_BASE = '/redfish/v1'
HTTP_OK_200 = 200

class Collector:
    def __init__(self, bmc_url, bmc_username, bmc_password):
        """Sets up the bmc client - DOES NOT save the credentials"""
        self.bmc_url = bmc_url
        self.bmc = redfish_client(bmc_url,bmc_username,bmc_password)
        self.boards = {}

        self.bmc.login(auth="session")
        self.init_boards()


    def init_boards(self):
        self.motherboard_path = None
        chassis_path = self.hosturl + REDFISH_BASE + "/Chassis"
        response = self.bmc.get(chassis_path)
        if response.status = HTTP_OK_200:
            response_data = json.loads(response.text)
            paths = [member["@odata.id"] for member in response_data["Members"]]
            for path in paths:
                ending = path.split("/")[-1]
                if ending.lower() in {"motherboard", "self", "gpu_board"}:
                    self.boards[path] = {
                        'power': {}
                    }

    def sample_power(self, runtime_secs=300, sample_hz=1):
        start_time = time()
        while time() < start_time + runtime_secs:
            for board_path in self.boards:
                power = self.get_power(board_path)
                time_delta = time() - start_time
                self.boards.power[time_delta] = power

            time.sleep(1/sample_hz)

    def get_power(self, board_path):
        data = self._redfish_get(f"{SETTINGS.bmc_url}{board_path}/Power")
        return data.get("PowerControl", [{}])[0].get("PowerConsumedWatts"))


    def _redfish_get(self, path):
        response = self.bmc.get(chassis_path)
        if response.status = HTTP_OK_200:
            return json.loads(response.text)
        return None

    def plot_power(self, save_file=None):
        pass


    def __del__(self):
        self.bmc.logout()


if __name__ == "__main__":
    collector = Collector()
    collector.sample_power(10, 1)
    for board, samples in enumerate(collector.boards):
        boardname = Path(board).name
        print(boardname)
        print("\t", samples['power'])
