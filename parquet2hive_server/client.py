import boto3
import os

from settings import server_dns, server_port, secret_key, secret_value_bucket, secret_value_key
from requests import post
from tempfile import NamedTemporaryFile
from parquet2hive_modules import parquet2hivelib as lib

class Parquet2HiveClient():

    arg_key = 'args'

    def __init__(self, server=None):
        """Create client. Can load a dataset.

        :param server - The DNS entry for the associated server, with the port.
                        e.g. 0.0.0.0:5129
                        If None, checks the environment for $PRESTO_DNS. If that
                        is missing as well, uses the settings values for server and port.

        >>> from parquet2hive_server.client import Parquet2HiveClient
        >>> client = Parquet2HiveClient()
        >>> client.load(['s3://telemetry-parquet/longitudinal', '--dataset-version', 'v1'])
        """
        self.s3 = boto3.client('s3')

        env_server = os.environ.get("PRESTO_DNS")
        settings_server = (server_dns + ':' + str(server_port))

        _server_dns = server or env_server or settings_server
        self.server = 'http://' + _server_dns

    def load(self, args):
        """Load a dataset.

        :param args - List of arguments for parquet2hive
        """
        lib.parse_args(args)
        str_args = ' '.join(args)

        kwargs = {
            secret_key: self.get_secret(),
            Parquet2HiveClient.arg_key: str_args
        }

        res = post(self.server, data=kwargs)
        return res.status_code, res.text

    def get_secret(self):
        obj_file = NamedTemporaryFile()
        self.s3.download_file(secret_value_bucket, secret_value_key, obj_file.name)

        with open(obj_file.name, 'r+') as f:
            res = f.readlines()

        return ''.join([r.strip() for r in res])
