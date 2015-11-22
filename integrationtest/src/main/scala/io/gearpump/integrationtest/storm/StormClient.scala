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
import io.gearpump.integrationtest.Constants._
import io.gearpump.integrationtest.{Shell, Docker}
import io.gearpump.integrationtest.minicluster.{CommandLineClient, MiniCluster}

class StormClient(cluster: MiniCluster, commandLineClient: CommandLineClient) {

  private val STORM_HOST = "storm0"
  private val STORM_CMD = "/opt/start storm"
  private val STORM_STARTER_JAR = "/opt/gearpump/lib/storm/storm-starter-0.9.5.jar"
  private val IMAGE = "stanleyxu2005/gpct-jdk8"
  private val CONFIG_FILE = "storm.yaml"
  private var drpcServerIp: String = null

  def start(): Unit = {
    Docker.createAndStartContainer(STORM_HOST,
      s"-d -h $STORM_HOST -p 6627:6627 -p 3772:3772 -p 3773:3773 ${cluster.LINK_TO_MASTER_OPTS} " +
          s"-e CLUSTER=${cluster.CLUSTER_OPTS} $MOUNT_PATH_OPTS $MOUNT_PATH_OPTS",
      "drpc", IMAGE)
    Shell.shellExec(s"cp $getConfigFilePath $MOUNT_PATH", "host")
    drpcServerIp = getDRPCServerIp
    Docker.exec(STORM_HOST, s"sed -i -e s/%drpc%/$drpcServerIp/g $SUT_HOME/storm.yaml")
  }

  def submitStormApp(mainClass: String = "",  args: String = ""): Int = {
    try {
      Docker.execAndCaptureOutput(STORM_HOST, s"$STORM_CMD -config $CONFIG_FILE " +
          s"-jar $STORM_STARTER_JAR $mainClass $args")
          .split("\n").last
          .replace("Submit application succeed. The application id is ", "")
          .toInt
    } catch {
      case ex: Throwable => -1
    }
  }

  def getDRPCClient: DRPCClient = {
    if (drpcServerIp == null) {
      throw new Exception(s"storm client is not started")
    }
    new DRPCClient(drpcServerIp, 3772)
  }

  def shutDown(): Unit = {
    Docker.killAndRemoveContainer(STORM_HOST)
  }

  private def getConfigFilePath: String = {
    this.getClass.getClassLoader.getResource(CONFIG_FILE).getPath
  }

  private def getDRPCServerIp: String = {
    Docker.execAndCaptureOutput(STORM_HOST, "ip route").split("\\s+")(2)
  }
}
