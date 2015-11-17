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
    shift
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    nohup sh ${SUT_HOME}/bin/master "$@" &
    if [ -z "$(jps | grep Services)" ]; then
      JAVA_OPTS="$JAVA_OPTS -Dgearpump.services.host=$(hostname)"
      nohup sh ${SUT_HOME}/bin/services &
    fi
    ;;
  worker)
    JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname -i)"
    nohup sh ${SUT_HOME}/bin/worker &
    ;;
  gear)
    shift
    #JAVA_OPTS="$JAVA_OPTS -Dgearpump.hostname=$(hostname)"
    nohup sh ${SUT_HOME}/bin/gear "$@" &
    ;;
  *)
    echo "Usage: master|worker [ARGS]"
    ;;
esac

if [ "$(ps aux | grep top -c)" = "1" ]; then
  # A trick to prevent leaving the bash
  top -d 65535
fi
