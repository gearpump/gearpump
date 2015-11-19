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

COMMAND=$1
shift

case "$COMMAND" in
  master)
    # Will launch REST service as daemon and then launch master in foreground
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname) -Dgearpump.services.host=$(hostname)"
    nohup sh "$SUT_HOME"/bin/services &
    nohup sh "$SUT_HOME"/bin/master "$@"
    ;;
  worker)
    # Will launch a worker instance in foreground
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname -i)"
    nohup sh "$SUT_HOME"/bin/worker
    ;;
  gear|storm)
    # Will execute command `gear` or `storm` with any number of arguments and wait for response
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    sh "$SUT_HOME"/bin/"$COMMAND" "$@"
    ;;
  *)
    echo "Usage:"
    echo "  master -ip [HOST] -port [PORT]"
    echo "  worker"
    echo "  gear [ARGS]"
    echo "  storm [ARGS]"
    exit 1
    ;;
esac

