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
package org.apache.gearpump.integrationtest.minicluster

import java.io.IOException
import scala.collection.mutable.ListBuffer

import org.apache.log4j.Logger

import org.apache.gearpump.integrationtest.{Docker, Util}

/**
 * This class is a test driver for end-to-end integration test.
 */
class MiniCluster {

  private val LOG = Logger.getLogger(getClass)
  private val SUT_HOME = "/opt/gearpump"

  private val REST_SERVICE_PORT = 8090
  private val MASTER_PORT = 3000
  private val MASTER_ADDRS: List[(String, Int)] = {
    (0 to 0).map(index =>
      ("master" + index, MASTER_PORT)
    ).toList
  }

  lazy val commandLineClient: CommandLineClient = new CommandLineClient(getMasterHosts.head)

  lazy val restClient: RestClient = {
    val client = new RestClient(getMasterHosts.head, REST_SERVICE_PORT)
    client
  }

  private var workers: ListBuffer[String] = ListBuffer.empty

  def start(workerNum: Int = 2): Unit = {

    // Kill master
    MASTER_ADDRS.foreach { case (host, _) =>
      if (Docker.containerExists(host)) {
        Docker.killAndRemoveContainer(host)
      }
    }

    // Kill existing workers
    workers ++= (0 until workerNum).map("worker" + _)
    workers.foreach { worker =>
      if (Docker.containerExists(worker)) {
        Docker.killAndRemoveContainer(worker)
      }
    }

    // Start Masters
    MASTER_ADDRS.foreach({ case (host, port) =>
      addMasterNode(host, port)
    })

    // Start Workers
    workers.foreach { worker =>
      val container = new BaseContainer(worker, "worker", MASTER_ADDRS)
      container.createAndStart()
    }

    // Check cluster status
    expectRestClientAuthenticated()
    expectClusterAvailable()
  }

  private def addMasterNode(host: String, port: Int): Unit = {
    val container = new BaseContainer(host, s"master -ip $host -port $port", MASTER_ADDRS)
    container.createAndStart()
  }

  def addWorkerNode(host: String): Unit = {
    if (workers.find(_ == host).isEmpty) {
      val container = new BaseContainer(host, "worker", MASTER_ADDRS)
      container.createAndStart()
      workers += host
    } else {
      throw new IOException(s"Cannot add new worker $host, " +
        s"as worker with same hostname already exists")
    }
  }

  /**
   * @throws RuntimeException if rest client is not authenticated after N attempts
   */
  private def expectRestClientAuthenticated(): Unit = {
    Util.retryUntil(() => {
      restClient.login()
      LOG.info("rest client has been authenticated")
      true
    }, "login successfully")
  }

  /**
   * @throws RuntimeException if service is not available after N attempts
   */
  private def expectClusterAvailable(): Unit = {
    Util.retryUntil(() => {
      val response = restClient.queryMaster()
      LOG.info(s"cluster is now available with response: $response.")
      response.aliveFor > 0
    }, "cluster running")
  }

  def isAlive: Boolean = {
    getMasterHosts.exists(nodeIsOnline)
  }

  def getNetworkGateway: String = {
    Docker.getNetworkGateway(MASTER_ADDRS.head._1)
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
    Util.retryUntil(() => {
      !(getMasterHosts ++ getWorkerHosts).exists(Docker.containerExists)
    }, "all docker containers killed")
    LOG.info("all docker containers have been killed. restarting...")
    start()
  }

  def getMastersAddresses: List[(String, Int)] = {
    MASTER_ADDRS
  }

  def getMasterHosts: List[String] = {
    MASTER_ADDRS.map({ case (host, port) => host })
  }

  def getWorkerHosts: List[String] = {
    workers.toList
  }

  def nodeIsOnline(host: String): Boolean = {
    Docker.containerIsRunning(host)
  }

  private def builtInJarsUnder(folder: String): Array[String] = {
    Docker.findJars(getMasterHosts.head, s"$SUT_HOME/$folder")
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
