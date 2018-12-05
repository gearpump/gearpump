#!/bin/bash
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
