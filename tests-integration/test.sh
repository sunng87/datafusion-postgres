#!/usr/bin/env bash

set -e

cargo build
./target/debug/datafusion-postgres-cli --csv delhi:tests-integration/delhiclimate.csv &
PID=$!
sleep 3
python tests-integration/test.py
kill -9 $PID 2>/dev/null

cd tests-integration && python create_arrow_testfile.py && cd .. 
./target/debug/datafusion-postgres-cli --parquet all_types:tests-integration/all_types.parquet &
PID=$!
sleep 3
python tests-integration/test_all_types.py
kill -9 $PID 2>/dev/null