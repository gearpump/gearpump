/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.integrationtest.minicluster

import io.gearpump.integrationtest.{Util, Docker}
import org.apache.log4j.Logger

import scala.sys.process._

/**
 * This class is a test driver for end-to-end integration test.
 */
class MiniCluster(
                   workerNum: Int = 2, // Each worker will launch a new Docker container
                   tunnelPorts: Set[Int] = Set.empty, // Ports that need to be forwarded
                   dockerImage: String = "stanleyxu2005/gpct-jdk8", // The Docker image to be tested with
                   masterPort: Int = 3000, // The port of master endpoint
                   restServicePort: Int = 8090 // The port of REST service (Yarn integration might modify the value)
                   ) {

  private val LOG = Logger.getLogger(getClass)
  private val SUT_HOME = "/opt/gearpump"
  private val MOUNT_PATH = "pwd".!!.trim + "/output/target/pack"
  private val MOUNT_PATH_OPTS = s"-v $MOUNT_PATH:$SUT_HOME"

  private val MASTER_ADDRS = {
    (0 to 0).map(index =>
      ("master" + index, masterPort)
    )
  }

  lazy val commandLineClient = new CommandLineClient(getMasterHosts.head)

  lazy val restClient = new RestClient(getMasterHosts.head, restServicePort)

  private val CLUSTER_OPTS = {
    MASTER_ADDRS.zipWithIndex.map { case (hostPort, index) =>
      s"-Dgearpump.cluster.masters.$index=${hostPort._1}:${hostPort._2}"
    }.mkString(" ")
  }

  private var workers: List[String] = List.empty

  def start(): Unit = {
    cleanupDockerEnv()

    // Masters' membership cannot be modified at runtime
    MASTER_ADDRS.foreach({ case (host, port) =>
      newMasterNode(host, port)
    })
    // We consider the cluster is started, when the REST services are ready.
    expectRestServiceAvailable()

    // Workers' membership can be modified at runtime
    (0 to workerNum - 1).foreach(index => {
      val host = "worker" + index
      newWorkerNode(host)
    })
  }

  private def cleanupDockerEnv(): Unit = {
    val containers = Docker.listContainers()
    if (containers.nonEmpty) {
      Docker.killAndRemoveContainer(containers.mkString(" "))
    }
  }

  private def newMasterNode(host: String, port: Int): Unit = {
    // Setup port forwarding from master node to host machine for testing
    val tunnelPortsOpt = tunnelPorts.map(port =>
      s"-p $port:$port").mkString(" ")

    Docker.createAndStartContainer(host, s"-d -h $host -e CLUSTER=$CLUSTER_OPTS $MOUNT_PATH_OPTS $tunnelPortsOpt",
      s"master -ip $host -port $port", dockerImage)
  }

  def newWorkerNode(host: String): Unit = {
    // Masters's hostname will be exposed to worker node
    val hostLinksOpt = getMasterHosts.map(master =>
      "--link " + master).mkString(" ")

    Docker.createAndStartContainer(host, s"-d $hostLinksOpt -e CLUSTER=$CLUSTER_OPTS $MOUNT_PATH_OPTS",
      "worker", dockerImage)
    workers :+= host
  }

  /**
   * @throws RuntimeException if service is not available for N retries
   */
  private def expectRestServiceAvailable(): Unit = {
    Util.retryUntil({
      val response = restClient.queryVersion()
      LOG.debug(s"Finish waiting for RestService available with response: $response.")
      response
    } != "")
  }

  def getDockerMachineIp(): String = {
    Docker.execAndCaptureOutput(MASTER_ADDRS.head._1, "ip route").split("\\s+")(2)
  }

  def shutDown(): Unit = {
    getWorkerHosts.foreach(removeWorkerNode)
    getMasterHosts.foreach(removeMasterNode)
  }

  private def removeMasterNode(host: String): Unit = {
    Docker.killAndRemoveContainer(host)
  }

  def removeWorkerNode(host: String): Unit = {
    if (Docker.killAndRemoveContainer(host)) {
      workers = workers.filter(_ != host)
    }
  }

  def getMasterHosts = {
    MASTER_ADDRS.map({ case (host, port) => host })
  }

  def getWorkerHosts = {
    workers
  }

  def nodeIsOnline(host: String): Boolean = {
    Docker.containerIsRunning(host)
  }

  def queryBuiltInExampleJars(subtext: String): Seq[String] = {
    Docker.execAndCaptureOutput(getMasterHosts.head, s"find $SUT_HOME/examples")
      .split("\n").filter(_.contains(subtext))
  }

}
