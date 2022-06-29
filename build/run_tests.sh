#!/bin/env bash

set -ex

./build/download_dynamodb_local.sh
./build/run_dynamodb_local.sh &


go vet ./...
go test -v ./...
go build ./...
