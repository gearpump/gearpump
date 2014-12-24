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

package org.apache.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.gearpump.cluster._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Configs, Util}
import org.slf4j.{LoggerFactory, Logger}

class ClientContext(masters: Iterable[HostPort], systemConfig : Config) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ClientContext])
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  val system = ActorSystem("client", systemConfig)

  val master = system.actorOf(Props(classOf[MasterProxy], masters), MASTER)

  def submit(app : Application, jar: Option[AppJar]) : Int = {
    val client = new MasterClient(master)
    client.submitApplication(app, jar)
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
  def apply(masterList : String, config : Config) = {
    new ClientContext(Util.parseHostList(masterList), config)
  }

  def apply(masters: Iterable[HostPort], config : Config) = new ClientContext(masters, config)
}
