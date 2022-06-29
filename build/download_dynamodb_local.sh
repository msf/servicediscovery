#!/bin/bash
set -e

DDBPATH=./vendor/packages/dynamodb/
if [ ! -f ${DDBPATH}/dynamodb_local_latest.tar.gz ]; then
    echo 'Downloading DynamoDB...'
    mkdir -p ${DDBPATH}
    curl https://from-where-exactly/dynamodb_local_latest.tar.gz ${DDBPATH}
    tar zxpf ${DDBPATH}/dynamodb_local_latest.tar.gz -C ${DDBPATH}
else
    echo "DynamoDB found in $DDBPATH"
fi
