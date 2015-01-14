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

import akka.actor._
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.master.Master
import org.apache.gearpump.cluster.worker.Worker

object TestUtil {
  val rawConfig = ClusterConfig.load("test.conf")
  val DEFAULT_CONFIG = rawConfig.application
  val MASTER_CONFIG = rawConfig.master
  val WORKER_CONFIG = rawConfig.worker

  def startMiniCluster = new MiniCluster

  class MiniCluster {
    private val mockMasterIP = "127.0.0.1"

    private implicit val system = ActorSystem("system", MASTER_CONFIG.
      withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(mockMasterIP)))

    val mockMaster: ActorRef = {
      system.actorOf(Props(classOf[Master]), "master")
    }

    val worker: ActorRef = {
      system.actorOf(Props(classOf[Worker], mockMaster), "worker")
    }

    def launchActor(props: Props): TestActorRef[Actor] = {
      TestActorRef(props)
    }

    def shutDown() = system.shutdown()
  }

  class DummyAppMaster(context: AppMasterContext, app: Application) extends ApplicationMaster {
    context.master ! (context, app)

    def receive : Receive = null
  }

  val dummyApp = Application("dummy", classOf[DummyAppMaster].getName, UserConfig.empty)
}