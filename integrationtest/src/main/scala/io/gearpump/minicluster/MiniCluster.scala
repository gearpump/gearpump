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
package io.gearpump.minicluster

import scala.sys.process._

/**
 * This class is a test driver for end-to-end integration test.
 */
class MiniCluster(
                   workerNum: Int = 2, // Each worker will launch a new Docker container
                   tunnelPorts: Set[Int] = Set.empty, // Ports that need to be forwarded
                   dockerImage: String = "stanleyxu2005/gpct-jdk8" // The Docker image to be tested with
                   ) {

  private val SUT_HOME = "/opt/gearpump"
  private val MOUNT_PATH = "pwd".!!.trim + "/output/target/pack"
  private val MOUNT_PATH_OPTS = s"-v $MOUNT_PATH:$SUT_HOME"

  private val MASTERS = {
    (0 to 0).map("master" + _)
  }

  private val MASTER_PORT = 3000
  private val REST_PORT = 8090

  def client = new RestClient(MASTERS.head, REST_PORT)

  private val CLUSTER_OPTS = {
    MASTERS.zipWithIndex.map { case (host, index) =>
      s"-Dgearpump.cluster.masters.$index=$host:$MASTER_PORT"
    }.mkString(" ")
  }

  private var workers: List[String] = List.empty

  def start(): Unit = {
    // Masters' membership cannot be modified at runtime
    MASTERS.foreach(newMasterNode)

    // Workers' membership can be modified at runtime
    (0 to workerNum - 1).foreach(index => {
      val host = "worker" + index
      newWorkerNode(host)
    })

    // We consider the cluster is started, when the REST services are ready.
    expectRestServiceAvailable()
  }

  private def newMasterNode(host: String): Unit = {
    // Setup port forwarding from master node to host machine for testing
    val tunnelPortsOpt = tunnelPorts.map(port =>
      s"-p $port:$port").mkString(" ")

    Docker.run(host, s"-d -h $host -e CLUSTER=$CLUSTER_OPTS $MOUNT_PATH_OPTS $tunnelPortsOpt",
      s"master -ip $host -port $MASTER_PORT", dockerImage)
  }

  def newWorkerNode(host: String): Unit = {
    // Masters's hostname will be exposed to worker node
    val hostLinksOpt = MASTERS.map(master =>
      "--link " + master).mkString(" ")

    Docker.run(host, s"-d $hostLinksOpt -e CLUSTER=$CLUSTER_OPTS $MOUNT_PATH_OPTS",
      "worker", dockerImage)
    workers :+= host
  }

  /**
   * @throws RuntimeException if service is not available for N retries
   */
  private def expectRestServiceAvailable(retry: Int = 10, retryDelay: Int = 200): Unit = {
    try {
      client.queryVersion
    } catch {
      case ex if retry > 0 =>
        Thread.sleep(retryDelay)
        expectRestServiceAvailable(retry - 1, retryDelay)
    }
  }

  def shutDown(): Unit = {
    workers.foreach(removeWorkerNode)
    MASTERS.foreach(removeMasterNode)
  }

  private def removeMasterNode(host: String): Unit = {
    Docker.killAndRemove(host)
  }

  def removeWorkerNode(host: String): Unit = {
    if (Docker.killAndRemove(host)) {
      workers = workers.filter(_ != host)
    }
  }

  def nodeOnline(host: String): Boolean = {
    Docker.running(host)
  }

  def queryBuiltInExampleJars(subtext: String): Seq[String] = {
    Docker.execAndCaptureOutput(MASTERS.head, s"find $SUT_HOME/examples")
      .split("\n").filter(_.contains(subtext))
  }

}
