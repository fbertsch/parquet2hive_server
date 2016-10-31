from flask_restful import Resource, reqparse
from parquet2hive_modules import parquet2hivelib as lib
from parquet2hive_server.client import Parquet2HiveClient
from subprocess import Popen
from settings import secret_key, debug

class Parquet2HiveServer(Resource):

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataset', type=str, help='Location in s3 of dataset location. Must begin with s3://')
        self.parser.add_argument('success-only', type=bool, default=False, required=False, store_missing=True,
                                 help='Only process partitions that contain a _SUCCESS file')
        self.parser.add_argument('dataset-version', type=str, required=False, store_missing=True,
                                 help='Specify version of the dataset to use with format vyyyymmdd, e.g. v20160514. Cannot be used with --use-last-versions')
        self.parser.add_argument('use-last-versions', type=int, required=False, store_missing=True,
                                 help='Load only the most recent version of the dataset, cannot be used with --dataset-version. Defaults to 1')

        #could not successfuly identify alias with flask_restful argparse, hence this
        self.parser.add_argument('so', type=bool, default=False, required=False, store_missing=True,
                                 help='Only process partitions that contain a _SUCCESS file')
        self.parser.add_argument('dv', type=str, required=False, store_missing=True,
                                 help='Specify version of the dataset to use with format vyyyymmdd, e.g. v20160514. Cannot be used with --use-last-versions')
        self.parser.add_argument('ulv', type=int, required=False, store_missing=True,
                                 help='Load only the most recent version of the dataset, cannot be used with --dataset-version. Defaults to 1')

        self.parser.add_argument(secret_key, type=str, required=True,
                                 help='Secret to authenticate client')

        self.client = Parquet2HiveClient()

    def post(self):
        allowed, args, msg = self.parse_args()

        if allowed: 
            try:
                res = lib.get_bash_cmd(dataset=args['dataset'], success_only=args['success-only'], recent_versions=args['use-last-versions'], version=args['dataset-version'])
                if not debug:
                    process = Popen(res)
                    res = process.communicate()
            except Exception:
                res = "Failed"
        else:
            res = msg 
        return {"Result": res}

    def parse_args(self):
        args = self.parser.parse_args(strict=True)
        allowed, msg = True, '' 
        secret = self.client.get_secret()

        if args[secret_key] != secret:
            allowed = False
            msg = 'Incorrect secret'
        if args['dataset-version'] is not None and args['use-last-versions'] is not None:
            allowed = False
            msg = 'Cannot use both dataset-version and use-last-versions'

        return allowed, args, msg
