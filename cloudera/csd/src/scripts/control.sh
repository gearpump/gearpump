#!/bin/bash

# based on https://github.com/cloudera/cm_csds/blob/master/SPARK/src/scripts/control.sh

# Time marker for both stderr and stdout
date 1>&2

CMD=$1
export CONF_DIR=${CONF_DIR:-.}
MASTER_FILE=$CONF_DIR/$2
shift 2

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function get_default_fs {
  hdfs --config $1 getconf -confKey fs.defaultFS 2>/dev/null
}

function join {
  local IFS=","
  echo "$*"
}

log "Detected CDH_VERSION of [$CDH_VERSION]"

DEFAULT_GEARPUMP_HOME=/usr/lib/gearpump

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

export HADOOP_HOME=${HADOOP_HOME:-$CDH_HADOOP_HOME}
export HADOOP_CONF_DIR=$CONF_DIR/hadoop-conf

# If GEARPUMP_HOME is not set, make it the default
export GEARPUMP_HOME=${GEARPUMP_HOME:-$DEFAULT_GEARPUMP_HOME}

export GEARPUMP_LIBRARY_PATH=${GEARPUMP_HOME}/lib
export SCALA_LIBRARY_PATH=${GEARPUMP_HOME}/lib

if [ -n "$HADOOP_HOME" ]; then
  export GEARPUMP_LIBRARY_PATH=$GEARPUMP_LIBRARY_PATH:${HADOOP_HOME}/lib/native
fi

MASTER_ARG=""
MASTER_LIST=()
index=0
if [ -f $MASTER_FILE ]; then
  MASTER_IP=
  MASTER_PORT=
  for line in $(sort $MASTER_FILE)
  do
    readconf "$line"
    case $key in
      master.port)
         MASTER_IP="$host"
        MASTER_PORT="$value"
        ;;
    esac

    if [ -n "$MASTER_IP " ] && [ -n "$MASTER_PORT" ]; then
       if [ z"$MASTER_IP" = z"$GEARPUMP_MASTER" ]; then
          THIS_PORT=$MASTER_PORT
       fi
       i=$((index++))
       MASTER_ARG="$MASTER_ARG -Dgearpump.cluster.masters.$i=${MASTER_IP}:$MASTER_PORT"
       MASTER_LIST+=("\"$MASTER_IP:$MASTER_PORT\"")
       MASTER_IP=
       MASTER_PORT=
    fi
  done
  log "Found masters: $MASTER_ARG"
fi

# We want to use a local conf dir
export GEARPUMP_CONF_DIR=$CONF_DIR/config
if [ ! -d "$GEARPUMP_CONF_DIR" ]; then
  mkdir $GEARPUMP_CONF_DIR
fi

# Set JAVA_OPTS for the daemons
# sets preference to IPV4
export GEARPUMP_DAEMON_JAVA_OPTS="$GEARPUMP_DAEMON_JAVA_OPTS -Djava.net.preferIPv4Stack=true $MASTER_ARG"
export JVM_OPT="$JVM_OPT $GEARPUMP_DAEMON_JAVA_OPTS"

ARGS=()
case $CMD in
  (start_master)
    log "Starting Gearpump master on $GEARPUMP_MASTER"
    ARGS+=("-ip" $GEARPUMP_MASTER "-port" $THIS_PORT)
    ARGS+=($ADDITIONAL_ARGS)

    export JVM_OPT="$JVM_OPT -Dgearpump.master.log.file=gearpump-master-${GEARPUMP_MASTER}.log -Dgearpump.log.daemon.dir=/var/log/gearpump"
    cmd="$GEARPUMP_HOME/bin/master ${ARGS[@]}"
    echo "Running [$cmd]"
    echo "JVM_OPT as [$JVM_OPT]"
    exec $cmd
    ;;

  (start_webui)
    log "Starting Gearpump webui"
    ARGS+=($ADDITIONAL_ARGS)

    export JVM_OPT="$JVM_OPT -Dgearpump.ui.log.file=gearpump-webui-${GEARPUMP_WEBUI_NODE}.log -Dgearpump.log.daemon.dir=/var/log/gearpump"
    cmd="$GEARPUMP_HOME/bin/services ${ARGS[@]}"
    echo "Running [$cmd]"
    exec $cmd
    ;;

  (start_worker)
    log "Starting Gearpump worker on $GEARPUMP_WORKER"
    ARGS+=($ADDITIONAL_ARGS)

    export JVM_OPT="$JVM_OPT -Dgearpump.worker.log.file=gearpump-worker-${GEARPUMP_WORKER}.log -Dgearpump.log.daemon.dir=/var/log/gearpump"
    cmd="$GEARPUMP_HOME/bin/worker ${ARGS[@]}"
    echo "Running [$cmd]"
    exec $cmd
    ;;

  (client)
    log "Deploying client configuration"
    MASTERS=$(join ${MASTER_LIST[@]})
    echo "master list is '$MASTERS'."

    CLIENT_CONF_DIR=$CONF_DIR/gearpump-conf

    perl -pi -e "s#{{MASTER_LIST}}#$MASTERS#g" $CLIENT_CONF_DIR/application.conf

    ENV_FILENAME="gearpump-env.sh"
    perl -pi -e "s#{{HADOOP_HOME}}#$HADOOP_HOME#g" $CLIENT_CONF_DIR/$ENV_FILENAME
    perl -pi -e "s#{{GEARPUMP_HOME}}#$GEARPUMP_HOME#g" $CLIENT_CONF_DIR/$ENV_FILENAME

    exit 0
    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;

esac
ARGS+=($ADDITIONAL_ARGS)

cmd="$GEARPUMP_HOME/bin/gearpump-class ${ARGS[@]}"
echo "Running [$cmd]"
exec $cmd
