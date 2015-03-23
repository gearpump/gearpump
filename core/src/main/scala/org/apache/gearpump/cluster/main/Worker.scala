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

package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.main.Master._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.worker.{Worker=>WorkerActor}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.util.LogUtil.ProcessType
import org.slf4j.Logger

import scala.collection.JavaConverters._

object
Worker extends App with ArgumentsParser {

  val options: Array[(String, CLIOption[Any])] =
    Array("ip"->CLIOption[String]("<master ip address>",required = true))

  var workerConfig = ClusterConfig.load.worker
  val LOG : Logger = {
    LogUtil.loadConfiguration(workerConfig, ProcessType.WORKER)
    //delay creation of LOG instance to avoid creating an empty log file as we reset the log file name here
    LogUtil.getLogger(getClass)
  }

  def uuid = java.util.UUID.randomUUID.toString

  def start(): Unit = {
    val config = parse(args)
    worker(config.getString("ip"))
  }

  def worker(ip: String): Unit = {
    val id = uuid

    workerConfig = workerConfig.
      withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(ip))

    val system = ActorSystem(id, workerConfig)

    val masterAddress = workerConfig.getStringList(GEARPUMP_CLUSTER_MASTERS).asScala.map { address =>
      val hostAndPort = address.split(":")
      HostPort(hostAndPort(0), hostAndPort(1).toInt)
    }

    LOG.info(s"Trying to connect to masters " + masterAddress.mkString(",") + "...")
    val masterProxy = system.actorOf(MasterProxy.props(masterAddress), MASTER)

    system.actorOf(Props(classOf[WorkerActor], masterProxy),
      classOf[WorkerActor].getSimpleName + id)

    system.awaitTermination()
  }

  start()
}