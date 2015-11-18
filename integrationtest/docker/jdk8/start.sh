#!/bin/sh

# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

if [ ! -d "$SUT_HOME/bin" ]; then
  echo "FATAL: Directory '$SUT_HOME' is incomplete. Please re-install."
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
    # Will launch REST service as daemon and then launch master in foreground
    shift
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    if [ -z "$(jps | grep Services)" ]; then
      JAVA_OPTS="$JAVA_OPTS -Dgearpump.services.host=$(hostname)"
      nohup sh ${SUT_HOME}/bin/services &
    fi
    nohup sh ${SUT_HOME}/bin/master "$@"
    ;;
  worker)
    # Will launch a worker instance in foreground
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname -i)"
    nohup sh ${SUT_HOME}/bin/worker
    ;;
  gear)
    # Will execute command `gear [ARGS]` and wait for response 
    shift
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    sh ${SUT_HOME}/bin/gear "$@"
    ;;
  gear)
    shift
    #JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    nohup sh ${SUT_HOME}/bin/gear "$@" &
    ;;
  *)
    echo "Usage:"
    echo "  master -ip [HOST] -port [PORT]"
    echo "  worker"
    echo "  gear [ARGS]"
    exit 1
    ;;
esac

