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

import akka.actor.{PoisonPill, ActorSystem, Props}
import akka.contrib.pattern.{ClusterSingletonProxy, ClusterSingletonManager}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.{Configs, Master => MasterClass}
import org.apache.gearpump.util.ActorUtil
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object Master extends App with ArgumentsParser {
  private val LOG: Logger = LoggerFactory.getLogger(Master.getClass)

  val options = Array("ip"->"master ip address", "port"->"master port")

  val config = parse(args)

  def start() = {
    master(config.getString("ip"), config.getInt("port"))
  }

  def verifyMaster(master : String, port: Int, masters : Iterable[String])  = {
    masters.exists{ hostPort =>
      hostPort == s"$master:$port"
    }
  }

  def master(ip:String, port : Int): Unit = {

    var masterConfig = Configs.MASTER_CONFIG
    val masters = masterConfig.getStringList("gearpump.cluster.masters").asScala

    if (!verifyMaster(ip, port, masters)) {
      LOG.error(s"The provided ip $ip and port $port doesn't conform with config at gearpump.cluster.masters")
      System.exit(-1)
    }

    masterConfig = masterConfig.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(ip)).
      withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromAnyRef(masters.map(master => s"akka.tcp://${Configs.MASTER}@$master").toList))

    val system = ActorSystem(Configs.MASTER, masterConfig)

    //start singleton manager
    system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(classOf[MasterClass]),
      singletonName = Configs.MASTER,
      terminationMessage = PoisonPill,
      role = Some(Configs.MASTER)),
      name = Configs.SINGLETON_MANAGER)

    //start master proxy
    system.actorOf(ClusterSingletonProxy.props(
      singletonPath = s"/user/${Configs.SINGLETON_MANAGER}/${Configs.MASTER}",
      role = Some(Configs.MASTER)),
      name = Configs.MASTER_PROXY)

    val masterPath = ActorUtil.getSystemPath(system) + s"/user/${Configs.MASTER_PROXY}"
    LOG.info(s"master proxy is started at $masterPath")
    system.awaitTermination()
  }

  start()
}