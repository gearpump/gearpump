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
