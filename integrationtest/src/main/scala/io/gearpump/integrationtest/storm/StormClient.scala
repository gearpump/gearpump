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


import backtype.storm.utils.DRPCClient
import io.gearpump.integrationtest.Docker
import io.gearpump.integrationtest.minicluster.BaseContainer
import org.apache.log4j.Logger

class StormClient(masterAddrs: Seq[(String, Int)]) {

  private val LOG = Logger.getLogger(getClass)
  private val STORM_HOST = "storm0"
  private val STORM_CMD = "/opt/start storm"
  private val STORM_DRPC = "storm-drpc"
  private val CONFIG_FILE = "storm.yaml"
  private val NIMBUS_THRIFT_PORT = 6627
  private val DRPC_PORT = 3772
  private val DRPC_INVOCATIONS_PORT = 3773

  private val container = new BaseContainer(STORM_HOST, STORM_DRPC, masterAddrs,
    tunnelPorts = Set(NIMBUS_THRIFT_PORT, DRPC_PORT, DRPC_INVOCATIONS_PORT))

  def start(): Unit = {
    container.createAndStart()
  }

  def submitStormApp(jar: String, mainClass: String, args: String = ""): Int = {
    try {
      Docker.execAndCaptureOutput(STORM_HOST, s"$STORM_CMD -config $CONFIG_FILE " +
        s"-jar $jar $mainClass $args").split("\n")
        .filter(_.contains("The application id is ")).head.split(" ").last.toInt
    } catch {
      case ex: Throwable =>
        LOG.warn(s"swallowed an exception: $ex")
        -1
    }
  }

  def getDRPCClient(drpcServerIp: String): DRPCClient = {
    new DRPCClient(drpcServerIp, DRPC_PORT)
  }

  def shutDown(): Unit = {
    container.killAndRemove()
  }

}
