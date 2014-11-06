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
package org.apache.gearpump.cluster

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestActorRef
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.gearpump.util.Constants._

import scala.collection.JavaConverters._

object TestUtil{
  private val MASTER_CONFIG = {
    val config = ConfigFactory.parseURL(getClass.getResource("/test.conf"))
    if (config.hasPath(MASTER)) {
      config.getConfig(MASTER).withFallback(config)
    } else {
      config
    }
  }
  
  def startMiniCluster = new MiniCluster

  class MiniCluster{
    private val mockMasterIP = "127.0.0.1"
    private val mockMasterPort = 4000

    private implicit val system = ActorSystem(MASTER, MASTER_CONFIG.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(mockMasterPort)).
      withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(mockMasterIP)).
      withValue("gearpump.cluster.masters",  ConfigValueFactory.fromAnyRef(List(s"$mockMasterIP:$mockMasterPort").asJava)))

    val mockMaster: ActorRef = {
      system.actorOf(Props(classOf[Master]))
    }

    val worker: ActorRef = {
      system.actorOf(Props(classOf[org.apache.gearpump.cluster.Worker], mockMaster))
    }

    def launchActor(props: Props): TestActorRef[Actor] = {
      TestActorRef(props)
    }

    def shutDown = system.shutdown()
  }
}
