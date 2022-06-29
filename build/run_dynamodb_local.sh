#!/bin/bash
set -e

./build/download_dynamodb_local.sh
DDBPATH=./vendor/packages/dynamodb/
DDBJARPATH="${DDBPATH}/DynamoDBLocal.jar"
echo "Running Local DynamoDB on http://localhost:8844"
java "-Djava.library.path=${DDBPATH}/DynamoDBLocal_lib" -jar "${DDBJARPATH}" -inMemory -port 8844
