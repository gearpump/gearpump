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
package org.apache.gearpump.jarstore

import akka.actor.{Actor, Stash}
import akka.pattern.pipe

import org.apache.gearpump.cluster.ClientToMaster.{GetJarStoreServer, JarStoreServerAddress}
import org.apache.gearpump.util._

class JarStoreServer(jarStoreRootPath: String) extends Actor with Stash {
  private val host = context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)
  private val jarStore = JarStore.get(jarStoreRootPath)
  jarStore.init(context.system.settings.config)
  private val server = new FileServer(context.system, host, 0, jarStore)
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  server.start pipeTo self

  def receive: Receive = {
    case FileServer.Port(port) =>
      context.become(listen(port))
      unstashAll()
    case _ =>
      stash()
  }

  def listen(port: Int): Receive = {
    case GetJarStoreServer =>
      sender ! JarStoreServerAddress(s"http://$host:$port/")
  }

  override def postStop(): Unit = {
    server.stop
  }
}
