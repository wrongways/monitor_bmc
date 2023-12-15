import argparse

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
