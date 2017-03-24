from settings import server_dns, server_port, secret_key, secret_value_bucket, secret_value_key
import boto3
from requests import post
from tempfile import NamedTemporaryFile

class Parquet2HiveClient():

    def __init__(self, server=None):
        self.s3 = boto3.client('s3')
        _server_dns = server or (server_dns + ':' + server_port)
        self.server = 'http://' + _server_dns

    def load(self, **kwargs):
        kwargs[secret_key] = self._get_secret()
        res = post(self.server, data=kwargs)
        return res.status_code, res.text

    def _get_secret(self):
        obj_file = NamedTemporaryFile()
        self.s3.download_file(secret_value_bucket, secret_value_key, obj_file.name)

        with open(obj_file.name, 'r+') as f:
            res = f.readlines()

        return ''.join([r.strip() for r in res])
