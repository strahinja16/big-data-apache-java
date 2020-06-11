#!/bin/bash

export SPARK_APPLICATION_ARGS="${APP_ARGS_CSV_FILE_PATH}"
export SPARK_SUBMIT_ARGS="--driver-memory 4096m"

sh /template.sh