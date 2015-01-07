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

package org.apache.gearpump.cluster.appmaster

import akka.actor.{Props, Actor}
import org.apache.gearpump.cluster.{MasterProxy, AppMasterContext, Application}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter.{CreateActorFailed, ActorCreated}

import scala.util.{Failure, Success, Try}

class AppMasterDaemon(masters: Iterable[HostPort], app : Application, appContextInput : AppMasterContext) extends Actor {
  val masterProxy = context.actorOf(Props(classOf[MasterProxy], masters), "masterproxy")
  val appContext = appContextInput.copy(masterProxy = masterProxy)

  context.actorOf(Props(Class.forName(app.appMaster), appContext, app), "appmaster")

  def receive : Receive = {
    case _ =>
  }
}