# Parquet2Hive Server
A server and client that can remotly import parquet data into Hive. Request to import after any dataset creation, minimizing time between creation and loading.

## Running the Server
To start the server, run
```bash
start_parquet2hive_server
```

## Importing a dataset
To remotely import a dataset, run 
```bash
export PRESTO_DNS=www.presto.dns
load_dataset s3://bucket/path/to/dataset
```

load_dataset supports any arguments supported by parquet2hive.

## Module Settings
Settings are found in parquet2hive_server/settings.py. When DEBUG mode is enabled, the hive command is returned and not run. Otherwise, the command runs and the result is returned.

