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

/**
 * This class is a test driver for end-to-end integration test.
 */
class MiniCluster(masterNum: Int = 1, workerNum: Int = 2) {

  // todo: run all tests with different test beds ("jdk8", "jdk7")
  private val DOCKER_IMAGE_NAME = "gearpump/gearpump"

  private val MASTERS = {
    if (masterNum != 1) {
      throw new NotImplementedError(
        "Either specify a free IP range or run all masters on same host at different ports.")
    }
    (0 to masterNum - 1).map("master" + _)
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
    // Expose REST service port to localhost for debugging
    val debugOpts = s"-p $REST_PORT:$REST_PORT"

    Docker.run(host, s"-d -h $host -e CLUSTER=$CLUSTER_OPTS $debugOpts",
      s"master -ip $host -port $MASTER_PORT", DOCKER_IMAGE_NAME)
  }

  def newWorkerNode(host: String): Unit = {
    // Masters's hostname will be exposed to worker node
    val hostLinksOpt = MASTERS.map(master =>
      "--link " + master).mkString(" ")

    Docker.run(host, s"-d $hostLinksOpt -e CLUSTER=$CLUSTER_OPTS",
      "worker", DOCKER_IMAGE_NAME)
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
    Docker.execAndCaptureOutput(MASTERS.head, "find /gearpump/examples")
      .split("\n").filter(_.contains(subtext))
  }

}