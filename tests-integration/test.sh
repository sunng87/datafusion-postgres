#!/usr/bin/env bash

set -e

cargo build
./target/debug/datafusion-postgres-cli --csv delhi:tests-integration/delhiclimate.csv &
PID=$!
sleep 3
python tests-integration/test.py
kill -9 $PID 2>/dev/null
