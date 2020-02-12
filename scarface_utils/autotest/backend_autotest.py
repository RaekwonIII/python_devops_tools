import configparser
import json
import logging
import sys
from argparse import ArgumentParser

from autotest.grpc.grpc_client import GRPCClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("Autotest")


def main(config_file, campaign_token):
    cfp = configparser.ConfigParser()
    with open(config_file) as cfg:
        cfp.read_file(cfg)
        auth_base_url = cfp.get('CONFIG', 'auth_base_url')
        # reading from config file always yields a string, parsing it as a list through JSON
        testable_services = json.loads(cfp.get('CONFIG', 'testable_services'))

    grpc_clients = {service: GRPCClient.from_auth_config(
        auth_base_url=auth_base_url, campaign_token=campaign_token, credentials={},
        grpc_host=cfp.get(service, 'service'), proto_name=service,
        stub_name=cfp.get(service, 'host')
    ) for service in testable_services}

    # TODO this is just preliminary code to show how it would work. We still need params and args for each call.
    for service, grpc_client in grpc_clients.items():
        # reading from config file always yields a string, parsing it as a list through JSON
        for call in json.loads(cfp.get(service, 'calls')):
            response = grpc_client.make_call(call)


if __name__ == "__main__":
    parser = ArgumentParser()

    args = parser.parse_args()
    sys.exit(main(args.config_file, args.campaign_token))
