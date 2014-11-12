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

package org.apache.gearpump.streaming.client

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import org.apache.gearpump.cluster.{Application, MasterClient, MasterProxy}
import org.apache.gearpump.streaming.{AppDescription, AppMaster, JarsForTasks, TaskConfiguration}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Configs, Util}
import org.slf4j.{LoggerFactory, Logger}

class ClientContext(masters: Iterable[HostPort]) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ClientContext])
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  val system = ActorSystem("client", Configs.SYSTEM_DEFAULT_CONFIG)

  val master = system.actorOf(Props(classOf[MasterProxy], masters), MASTER)

  def submit(app : Application) : Int = {
    val appDescription = app.asInstanceOf[AppDescription]
    appDescription.dag.vertex.map(taskDescription => {
      LOG.info(s"in dag taskDescription ${taskDescription.taskClass}")
      JarsForTasks.jars.get(taskDescription.taskClass).map(jar => {
        LOG.info(s"Setting taskDescription ${taskDescription.taskClass}")
        taskDescription.config = Option(TaskConfiguration(None,Option(jar)))
      })

    })
    val client = new MasterClient(master)
    client.submitApplication(classOf[AppMaster], Configs.empty, app)
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  //TODO: add interface to query master here

  def destroy() : Unit = {
    system.shutdown()
  }
}

object ClientContext {
  /**
   * masterList is a list of master node address
   * host1:port,host2:port2,host3:port3
   */
  def apply(masterList : String) = {
    new ClientContext(Util.parseHostList(masterList))
  }

  def apply(masters: Iterable[HostPort]) = new ClientContext(masters)
}