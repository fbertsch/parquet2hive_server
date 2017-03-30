from flask_restful import Resource, reqparse
from flask import current_app
from parquet2hive_modules import parquet2hivelib as lib
from parquet2hive_server.client import Parquet2HiveClient
from subprocess import Popen
from settings import secret_key

class Parquet2HiveServer(Resource):

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument(Parquet2HiveClient.arg_key, type=str, required=True)
        self.parser.add_argument(secret_key, type=str, required=True)

        self.client = Parquet2HiveClient()

    def post(self):
        allowed, args, msg = self.parse_args()

        if allowed: 
            try:
                print "Args: " + str(args)
                errored, res = lib.run(args)
                if not current_app.debug and errored == 0:
                    process = Popen(res, shell=True)
                    res = process.communicate()
            except Exception as e:
                res = "ServerException: {}".format(str(e))
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

        return allowed, args[Parquet2HiveClient.arg_key].split(' '), msg
