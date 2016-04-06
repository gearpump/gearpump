/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.integrationtest.storm

import scala.util.Random

import backtype.storm.utils.{DRPCClient, Utils}

import io.gearpump.integrationtest.minicluster.{BaseContainer, MiniCluster, RestClient}
import io.gearpump.integrationtest.{Docker, Util}

class StormClient(cluster: MiniCluster, restClient: RestClient) {

  private val masterAddrs: List[(String, Int)] = cluster.getMastersAddresses
  private val CONFIG_FILE = s"/opt/gearpump/storm${new Random().nextInt()}.yaml"
  private val DRPC_HOST = "storm0"
  private val DRPC_PORT = 3772
  private val DRPC_INVOCATIONS_PORT = 3773
  private val STORM_DRPC = "storm-drpc"
  private val NIMBUS_HOST = "storm1"
  private val STORM_NIMBUS = "storm nimbus"
  private val STORM_APP = "/opt/start storm app"

  private val drpcContainer = new BaseContainer(DRPC_HOST, STORM_DRPC, masterAddrs,
    tunnelPorts = Set(DRPC_PORT, DRPC_INVOCATIONS_PORT))

  private val nimbusContainer =
    new BaseContainer(NIMBUS_HOST, s"$STORM_NIMBUS -output $CONFIG_FILE", masterAddrs)

  def start(): Unit = {
    nimbusContainer.createAndStart()
    ensureNimbusRunning()

    drpcContainer.createAndStart()
    ensureDrpcServerRunning()
  }

  private def ensureNimbusRunning(): Unit = {
    Util.retryUntil(() => {
      val response = Docker.execute(NIMBUS_HOST, "grep \"port\" " + CONFIG_FILE)
      // Parse format nimbus.thrift.port: '39322'
      val thriftPort = response.split(" ")(1).replace("'", "").toInt

      Docker.executeSilently(NIMBUS_HOST, s"""sh -c "netstat -na | grep $thriftPort" """)
    }, "Nimbus running")
  }

  private def ensureDrpcServerRunning(): Unit = {
    Util.retryUntil(() => {
      Docker.executeSilently(DRPC_HOST, s"""sh -c "netstat -na | grep $DRPC_PORT " """)
    }, "DRPC running")
  }

  def submitStormApp(jar: String, mainClass: String, args: String, appName: String): Int = {
    Util.retryUntil(() => {
      Docker.executeSilently(NIMBUS_HOST, s"$STORM_APP -config $CONFIG_FILE " +
        s"-jar $jar $mainClass $args")
      restClient.listRunningApps().exists(_.appName == appName)
    }, "app running")
    restClient.listRunningApps().filter(_.appName == appName).head.appId
  }

  def getDRPCClient(drpcServerIp: String): DRPCClient = {
    val config = Utils.readDefaultConfig()
    new DRPCClient(config, drpcServerIp, DRPC_PORT)
  }

  def shutDown(): Unit = {

    // Cleans up the storm.yaml config file
    Docker.executeSilently(NIMBUS_HOST, s"rm $CONFIG_FILE ")
    drpcContainer.killAndRemove()
    nimbusContainer.killAndRemove()
  }
}
