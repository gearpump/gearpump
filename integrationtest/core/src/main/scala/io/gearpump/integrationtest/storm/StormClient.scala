/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.integrationtest.storm

import backtype.storm.utils.{Utils, DRPCClient}
import io.gearpump.integrationtest.{Util, Docker}
import io.gearpump.integrationtest.minicluster.{RestClient, BaseContainer}

class StormClient(masterAddrs: List[(String, Int)], restClient: RestClient) {

  private val CONFIG_FILE = "/opt/gearpump/storm.yaml"
  private val DRPC_HOST = "storm0"
  private val DRPC_PORT = 3772
  private val DRPC_INVOCATIONS_PORT = 3773
  private val STORM_DRPC = "storm-drpc"
  private val NIMBUS_HOST = "storm1"
  private val STORM_NIMBUS = "storm nimbus"
  private val STORM_APP = "/opt/start storm app"

  private val drpcContainer = new BaseContainer(DRPC_HOST, STORM_DRPC, masterAddrs,
    tunnelPorts = Set(DRPC_PORT, DRPC_INVOCATIONS_PORT))
  private val nimbusContainer = new BaseContainer(NIMBUS_HOST, s"$STORM_NIMBUS -output $CONFIG_FILE", masterAddrs)

  def start(): Unit = {
    drpcContainer.createAndStart()
    nimbusContainer.createAndStart()
  }

  def submitStormApp(jar: String, mainClass: String, args: String, appName: String): Int = {
    Util.retryUntil({
      Docker.exec(NIMBUS_HOST, s"$STORM_APP -config $CONFIG_FILE " +
        s"-jar $jar $mainClass $args")
      restClient.listRunningApps().exists(_.appName == appName)
    })
    restClient.listRunningApps().filter(_.appName == appName).head.appId
  }

  def getDRPCClient(drpcServerIp: String): DRPCClient = {
    val config = Utils.readDefaultConfig()
    new DRPCClient(config, drpcServerIp, DRPC_PORT)
  }

  def shutDown(): Unit = {
    drpcContainer.killAndRemove()
    nimbusContainer.killAndRemove()
  }

}
