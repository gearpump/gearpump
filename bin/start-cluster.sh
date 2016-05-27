#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Start Masters
readMasters

MASTER_CONFIG=""

# Init master configuration
for((i=0;i<${#MASTERS[@]};++i)); do
    MASTER_CONFIG+="-Dgearpump.cluster.masters.${i}=${MASTERS[i]}:${MASTERPORTS[i]} "
done

# Start masters
for((i=0;i<${#MASTERS[@]};++i)); do
    master=${MASTERS[i]}
    port=${MASTERPORTS[i]}
    ssh $master "export JAVA_OPTS='$JAVA_OPTS ${MASTER_CONFIG} -Dgearpump.hostname=${master}';\\
                 cd $GEAR_ROOT_DIR; $GEAR_BIN_DIR/gear-daemon.sh start master -ip $master -port $port"
done

# Wait for all Master start
sleep 2

# Start dashboard

readDashboard

if [ -n "$DASHBOARD" ]; then
    HOST=$(echo $DASHBOARD | cut -f1 -d:)
    PORT=$(echo $DASHBOARD | cut -s -f2 -d:)
    ssh $HOST "export JAVA_OPTS='$JAVA_OPTS ${MASTER_CONFIG} -Dgearpump.hostname=${HOST} -Dgearpump.services.host=${HOST} -Dgearpump.services.port=${PORT}';\\
                 cd $GEAR_ROOT_DIR; $GEAR_BIN_DIR/gear-daemon.sh start services"
else
    echo "No dashboard specified, please check file conf/dashboard"
fi


# Start workers

readWorkers

for worker in ${WORKERS[@]}; do
    ssh $worker "export JAVA_OPTS='$JAVA_OPTS ${MASTER_CONFIG} -Dgearpump.hostname=${worker}';\\
                cd $GEAR_ROOT_DIR; $GEAR_BIN_DIR/gear-daemon.sh start worker"
done
