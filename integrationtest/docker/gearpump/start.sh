#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [ ! -d "$SUT_HOME/bin" ]; then
  echo "FATAL: The Gearpump distribution seems to be incomplete. Please build gearpump with 'sbt clean assembly' first, so that the test driver is able to mount the distribution directory to '/opt/gearpump'."
  exit 1
fi

if [ -z "$JAVA_OPTS" ]; then
  echo "FATAL: Environment variable 'JAVA_OPTS' is NOT set."
  exit 1
fi

update_config_file() {
  CONF_FILE="$SUT_HOME"/conf/gear.conf
  mkdir /var/log/gearpump
  sed -i 's/log\.daemon\.dir\s*=.*$/log.daemon.dir = "\/var\/log\/gearpump\/daemon"/g' $CONF_FILE
  sed -i 's/log\.application\.dir\s*=.*$/log.application.dir = "\/var\/log\/gearpump\/app"/g' $CONF_FILE
  sed -i 's/#\s*jarstore\.rootpath\s*=.*$/jarstore.rootpath = "\/tmp"/g' $CONF_FILE
}

set_and_export_java_opts() {
  JAVA_OPTS="$JAVA_OPTS $*"
  export JAVA_OPTS
}

COMMAND=$1
shift

case "$COMMAND" in
  master|local)
    # Launch a container with Gearpump cluster and REST interface (in foreground)
    HOSTNAME=$(hostname)
    update_config_file
    set_and_export_java_opts \
      "-Dgearpump.hostname=$HOSTNAME" \
      "-Dgearpump.services.host=$HOSTNAME"
    nohup sh "$SUT_HOME"/bin/services &
    nohup sh "$SUT_HOME"/bin/"$COMMAND" "$@"
    ;;
  worker)
    # Launch a container with a Gearpump worker (in foreground)
    update_config_file
    set_and_export_java_opts \
      "-Dgearpump.hostname=$(hostname -i)"
    nohup sh "$SUT_HOME"/bin/worker
    ;;
  gear|storm)
    # Launch a container and execute command `gear` or `storm`
    # Container will be killed, when command is executed. 
    update_config_file
    set_and_export_java_opts \
      "-Dgearpump.hostname=$(hostname -i)"
    sh "$SUT_HOME"/bin/"$COMMAND" "$@"
    ;;
  storm-drpc)
    # Launch a container with a Storm DRPC daemon
    # Note that this command has nothing to do with Gearpump, it only uses storm related jar libs.
    LIB_HOME="$SUT_HOME"/lib
    cat > "$SUT_HOME"/storm.yaml <<- EOF
drpc.servers:
  - $(ip route|awk '/default/ {print $3}')
EOF
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
