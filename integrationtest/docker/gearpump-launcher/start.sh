#!/bin/sh

# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

if [ ! -d "$SUT_HOME/bin" ]; then
  echo "FATAL: Directory '$SUT_HOME' is incomplete. Please re-install."
  exit 1
fi

if [ -z "JAVA_OPTS" ]; then
  echo "FATAL: Environment variable 'JAVA_OPTS' is NOT set."
  exit 1
fi
export JAVA_OPTS

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
  storm-drpc)
    # Will launch a Storm DRPC daemon
    LIB_HOME="$SUT_HOME"/lib
    cat > "$SUT_HOME"/storm.yaml <<- YAML
drpc.servers:
  - `ip route | awk '/default/ {print $3}'`
YAML
    java -server -Xmx768m -cp "$LIB_HOME"/*:"$LIB_HOME"/storm/* backtype.storm.daemon.drpc
    ;;
  *)
    cat <<- USAGE
Gearpump Commands:
  master -ip [HOST] -port [PORT]
  worker
  gear (app|info|kill) [ARGS]
  storm [ARGS]

Storm Commands:
  storm-drpc
USAGE
    exit 1
    ;;
esac
