#!/bin/sh

# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

BIN_DIR=gearpump/bin

if [ ! -d $BIN_DIR ]; then
  echo "FATAL: Directory '$BIN_DIR' is missing. Please re-install."
  exit 1
fi
if [ -z "$CLUSTER" ]; then
  echo "FATAL: Environment variable 'CLUSTER' is NOT set."
  exit 1
fi

export JAVA_OPTS
JAVA_OPTS="$CLUSTER"

case "$1" in
  master)
    shift
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    nohup $BIN_DIR/master "$@" &
    if [ -z "$(jps | grep Services)" ]; then
      JAVA_OPTS="$JAVA_OPTS -Dgearpump.services.host=$(hostname)"
      nohup $BIN_DIR/services &
    fi
    ;;
  worker)
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname -i)"
    nohup $BIN_DIR/worker &
    ;;
  *)
    echo "Usage: master|worker [ARGS]"
    ;;
esac

if [ "$(ps aux | grep top -c)" = "1" ]; then
  # A trick to prevent leaving the bash
  top -d 65535
fi