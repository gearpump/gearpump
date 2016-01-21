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

import io.gearpump.integrationtest.{Docker, Util}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
 * This class is a test driver for end-to-end integration test.
 */
class MiniCluster {

  private val LOG = Logger.getLogger(getClass)
  private val SUT_HOME = "/opt/gearpump"

  private val REST_SERVICE_PORT = 8090
  private val MASTER_PORT = 3000
  private val MASTER_ADDRS = {
    (0 to 0).map(index =>
      ("master" + index, MASTER_PORT)
    )
  }

  lazy val commandLineClient = new CommandLineClient(getMasterHosts.head)

  lazy val restClient = {
    val client = new RestClient(getMasterHosts.head, REST_SERVICE_PORT)
    client
  }

  private var workers: ListBuffer[String] = ListBuffer.empty

  def start(workerNum: Int = 2): Unit = {
    // Masters' membership cannot be modified at runtime
    MASTER_ADDRS.foreach({ case (host, port) =>
      addMasterNode(host, port)
    })
    expectRestClientAuthenticated()
    expectClusterAvailable()

    // Workers' membership can be modified at runtime
    (0 to workerNum - 1).foreach(index => {
      val host = "worker" + index
      addWorkerNode(host)
    })
  }

  private def addMasterNode(host: String, port: Int): Unit = {
    val container = new BaseContainer(host, s"master -ip $host -port $port", MASTER_ADDRS)
    container.createAndStart()
  }

  def addWorkerNode(host: String): Unit = {
    val container = new BaseContainer(host, "worker", MASTER_ADDRS)
    container.createAndStart()
    workers += host
  }

  /**
   * @throws RuntimeException if rest client is not authenticated after N attempts
   */
  private def expectRestClientAuthenticated(): Unit = {
    Util.retryUntil({
      restClient.login()
      LOG.info("rest client has been authenticated")
      true
    })
  }

  /**
   * @throws RuntimeException if service is not available after N attempts
   */
  private def expectClusterAvailable(): Unit = {
    Util.retryUntil({
      val response = restClient.queryMaster()
      LOG.info(s"cluster is now available with response: $response.")
      response.aliveFor > 0
    })
  }

  def isAlive: Boolean = {
    getMasterHosts.exists(nodeIsOnline)
  }

  def getNetworkGateway: String = {
    Docker.execAndCaptureOutput(MASTER_ADDRS.head._1, "ip route").split("\\s+")(2)
  }

  def shutDown(): Unit = {
    val removalHosts = (getMasterHosts ++ getWorkerHosts).toSet
      .filter(nodeIsOnline).toArray
    if (removalHosts.length > 0) {
      Docker.killAndRemoveContainer(removalHosts)
    }
    workers.clear()
  }

  def removeMasterNode(host: String): Unit = {
    Docker.killAndRemoveContainer(host)
  }

  def removeWorkerNode(host: String): Unit = {
    workers -= host
    Docker.killAndRemoveContainer(host)
  }

  def restart(): Unit = {
    shutDown()
    Util.retryUntil(
      !(getMasterHosts ++ getWorkerHosts).exists(Docker.containerExists))
    LOG.info("all containers have been killed. restarting...")
    start()
  }

  def getMastersAddresses = {
    MASTER_ADDRS
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

  private def builtInJarsUnder(folder: String) = {
    Docker.execAndCaptureOutput(getMasterHosts.head, s"find $SUT_HOME/$folder")
      .split("\n").filter(_.endsWith(".jar"))
  }

  private def queryBuiltInJars(folder: String, subtext: String): Seq[String] = {
    builtInJarsUnder(folder).filter(_.contains(subtext))
  }

  def queryBuiltInExampleJars(subtext: String): Seq[String] = {
    queryBuiltInJars("examples", subtext)
  }

  def queryBuiltInITJars(subtext: String): Seq[String] = {
    queryBuiltInJars("integrationtest", subtext)
  }

}
