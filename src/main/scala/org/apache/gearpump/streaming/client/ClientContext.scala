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

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import org.apache.gearpump.cluster.{Application, Configs, MasterClient}
import org.apache.gearpump.streaming.AppMaster

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClientContext(masterActorPath : String) {

  private var system : ActorSystem = null
  private var master : ActorRef = null

  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  this.system = ActorSystem("client", Configs.SYSTEM_DEFAULT_CONFIG)
  this.master = Await.result(system.actorSelection(masterActorPath).resolveOne(), Duration.Inf)

  def submit(app : Application) : Int = {
    val client = new MasterClient(master)
    client.submitApplication(classOf[AppMaster], Configs.empty, app)
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  def destroy() : Unit = {
    system.shutdown()
  }
}

object ClientContext {
  def apply(masterActorPath : String) = new ClientContext(masterActorPath)
}