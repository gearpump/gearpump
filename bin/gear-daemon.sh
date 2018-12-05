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

USAGE="Usage: gear-daemon.sh (start|stop|stop-all) (local|master|worker|services) [args]"

OPERATION=$1
DAEMON=$2
ARGS=("${@:3}") # get remaining arguments as array

case $DAEMON in
    (master)
        BASH_TO_RUN=master
    ;;

    (worker)
        BASH_TO_RUN=worker
    ;;

    (local)
        BASH_TO_RUN=local
    ;;

    (services)
        BASH_TO_RUN=services
    ;;

    (*)
        echo "Unknown daemon '${DAEMON}'. $USAGE."
        exit 1
    ;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$GEARPUMP_IDENT_STRING" = "" ]; then
    GEARPUMP_IDENT_STRING="$USER"
fi

pid=$GEARPUMP_PID_DIR/gear-$GEARPUMP_IDENT_STRING-$DAEMON.pid

mkdir -p "$GEARPUMP_PID_DIR"
mkdir -p "$GEARPUMP_LOG_DIR"

# Ascending ID depending on number of lines in pid file.
# This allows us to start multiple daemon of each type.
id=$([ -f "$pid" ] && echo $(wc -l < $pid) || echo "0")

out="${GEARPUMP_LOG_DIR}/gearpump-${GEARPUMP_IDENT_STRING}-${DAEMON}-${id}-${HOSTNAME}.out"

case $OPERATION in

    (start)
        # Print a warning if daemons are already running on host
        if [ -f $pid ]; then
          active=()
          while IFS='' read -r p || [[ -n "$p" ]]; do
            kill -0 $p >/dev/null 2>&1
            if [ $? -eq 0 ]; then
              active+=($p)
            fi
          done < "${pid}"

          count="${#active[@]}"

          if [ ${count} -gt 0 ]; then
            echo "[INFO] $count instance(s) of $DAEMON are already running on $HOSTNAME."
          fi
        fi

        echo "Starting $DAEMON daemon on host $HOSTNAME."
        "$bin"/${BASH_TO_RUN} "${ARGS[@]}" > "$out" 2>&1 < /dev/null &

        mypid=$!

        # Add to pid file if successful start
        if [[ ${mypid} =~ ${IS_NUMBER} ]] && kill -0 $mypid > /dev/null 2>&1 ; then
            echo $mypid >> $pid
        else
            echo "Error starting $DAEMON daemon."
            exit 1
        fi
    ;;

    (stop)
        if [ -f $pid ]; then
            # Remove last in pid file
            to_stop=$(tail -n 1 $pid)

            if [ -z $to_stop ]; then
                rm $pid # If all stopped, clean up pid file
                echo "No $DAEMON daemon to stop on host $HOSTNAME."
            else
                sed \$d $pid > $pid.tmp # all but last line

                # If all stopped, clean up pid file
                [ $(wc -l < $pid.tmp) -eq 0 ] && rm $pid $pid.tmp || mv $pid.tmp $pid

                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    kill $to_stop
                else
                    echo "No $DAEMON daemon (pid: $to_stop) is running anymore on $HOSTNAME."
                fi
            fi
        else
            echo "No $DAEMON daemon to stop on host $HOSTNAME."
        fi
    ;;

    (stop-all)
        if [ -f $pid ]; then
            mv $pid ${pid}.tmp

            while read to_stop; do
                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    kill $to_stop
                else
                    echo "Skipping $DAEMON daemon (pid: $to_stop), because it is not running anymore on $HOSTNAME."
                fi
            done < ${pid}.tmp
            rm ${pid}.tmp
        fi
    ;;

    (*)
        echo "Unexpected argument '$STARTSTOP'. $USAGE."
        exit 1
    ;;
esac
