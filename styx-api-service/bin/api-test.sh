#!/usr/bin/env bash

PROJECT="$(dirname $0)/../.."
PORT="8080"
WAIT_CONNECT="nc -z localhost $PORT"

$PROJECT/styx-api-service/bin/run-service.sh -DtestMode & pid=$!
trap "kill $pid; wait" EXIT

$WAIT_CONNECT
while [[ $? -ne 0 ]]; do $WAIT_CONNECT; done

sleep 3
set -e
dredd $PROJECT/doc/api.apib http://localhost:$PORT/api
