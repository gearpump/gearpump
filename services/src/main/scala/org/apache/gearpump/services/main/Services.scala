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

package org.apache.gearpump.services.main

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.services.RestServices
import org.apache.gearpump.services.websocket.WebSocketServices
import org.apache.gearpump.util.LogUtil.ProcessType
import org.apache.gearpump.util.{Constants, LogUtil, Util}
import org.slf4j.Logger

object Services extends App {
  val systemConfig = ClusterConfig.load.master.withFallback(ClusterConfig.load.worker)

  private val LOG: Logger = {
    LogUtil.loadConfiguration(systemConfig, ProcessType.UI)
    LogUtil.getLogger(getClass)
  }

  def start(): Unit = {

    val config = ClusterConfig.load.ui

    import scala.collection.JavaConversions._
    val masterCluster = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)

    implicit val system = ActorSystem("services" , config)
    import scala.concurrent.duration._
    val master = system.actorOf(MasterProxy.props(masterCluster, 1 day), s"masterproxy${system.name}")

    WebSocketServices(master)
    RestServices(master)

    system.awaitTermination()
  }
  start ()
}
