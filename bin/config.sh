#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GEARPUMP_PID_DIR=""                                 # Directory to store *.pid files to
DEFAULT_ENV_PID_DIR="/tmp"                          # Default directory to store *.pid files to

GEARPUMP_LOG_DIR=""
DEFAULT_ENV_LOG_DIR="/tmp/gear-logs"

if [ "$GEARPUMP_PID_DIR" = "" ]; then
    GEARPUMP_PID_DIR=${DEFAULT_ENV_PID_DIR}
fi

if [ "$GEARPUMP_LOG_DIR" = "" ]; then
    GEARPUMP_LOG_DIR=${DEFAULT_ENV_LOG_DIR}
fi

bin=`dirname "$0"`
SYMLINK_RESOLVED=`cd "$bin"; pwd -P`

# Define the main directory of the gearpump installation
export GEAR_ROOT_DIR=`dirname "$SYMLINK_RESOLVED"`
export GEAR_BIN_DIR="$GEAR_ROOT_DIR/bin"
export GEAR_CONF_DIR="$GEAR_ROOT_DIR/conf"

readMasters() {
    MASTERS_FILE="${GEAR_CONF_DIR}/masters"

    if [[ ! -f "${MASTERS_FILE}" ]]; then
        echo "No masters file. Please specify masters in 'conf/masters'."
        exit 1
    fi

    MASTERS=()
    MASTERPORTS=()

    while IFS='' read -r line || [[ -n "$line" ]]; do
        HOSTPORT=$( extractHostName $line)

        if [ -n "$HOSTPORT" ]; then
            HOST=$(echo $HOSTPORT | cut -f1 -d:)
            PORT=$(echo $HOSTPORT | cut -s -f2 -d:)
            MASTERS+=(${HOST})

            if [ -z "$PORT" ]; then
                MASTERPORTS+=(3000)
            else
                MASTERPORTS+=(${PORT})
            fi
        fi
    done < "$MASTERS_FILE"
}

readWorkers() {
    WORKERS_FILE="${GEAR_CONF_DIR}/workers"

    if [[ ! -f "$WORKERS_FILE" ]]; then
        echo "No workers file. Please specify workers in 'conf/workers'."
        exit 1
    fi

    WORKERS=()

    while IFS='' read -r line || [[ -n "$line" ]];
    do
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ]; then
            WORKERS+=(${HOST})
        fi
    done < "$WORKERS_FILE"
}

readDashboard() {
    DASHBOARD_FILE="${GEAR_CONF_DIR}/dashboard"

    if [[ ! -f "$DASHBOARD_FILE" ]]; then
        echo "No dashboard file. Please specify dashboard in 'conf/dashboard'."
        return
    fi

    DASHBOARD=""

    while IFS='' read -r line || [[ -n "$line" ]];
    do
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ]; then
            DASHBOARD=${HOST}
            break
        fi
    done < "$DASHBOARD_FILE"
}

# Auxilliary function which extracts the name of host from a line which
# also potentially includes topology information and the taskManager type
extractHostName() {
    # handle comments: extract first part of string (before first # character)
    HOST=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$HOST" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            HOST=${BASH_REMATCH[1]}
    fi

    echo $HOST
}
