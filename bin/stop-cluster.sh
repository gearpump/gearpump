#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Stop workers
readWorkers

for worker in ${WORKERS[@]}; do
    ssh $worker "$GEAR_BIN_DIR/gear-daemon.sh stop-all worker"
done

# Stop dashboard
readDashboard

if [ -n "$DASHBOARD" ]; then
    HOST=$(echo $DASHBOARD | cut -f1 -d:)
    ssh $HOST "$GEAR_BIN_DIR/gear-daemon.sh stop-all services"
fi

# Stop Masters
readMasters

for master in ${MASTERS[@]}; do
    ssh $master "$GEAR_BIN_DIR/gear-daemon.sh stop-all master"
done