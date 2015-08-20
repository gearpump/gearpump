/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.jarstore.local

import java.io.File

import akka.actor.{Actor, Props, Stash}
import akka.pattern.{ask, pipe}
import io.gearpump.cluster.ClientToMaster.GetJarStoreServer
import io.gearpump.util.{FileServer, Constants}
import io.gearpump.util.FileUtils
import io.gearpump.cluster.ClientToMaster.{JarStoreServerAddress, GetJarStoreServer}
import io.gearpump.util.{Constants, FileServer, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future

class LocalJarStore(rootDirPath : String) extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val host = context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)
  val rootDirectory = new File(rootDirPath)

  FileUtils.forceMkdir(rootDirectory)

  val server = context.actorOf(Props(classOf[FileServer], rootDirectory, host , 0))

  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  (server ? FileServer.GetPort).asInstanceOf[Future[FileServer.Port]] pipeTo self

  def receive : Receive = {
    case FileServer.Port(port) =>
      context.become(listen(port))
      unstashAll()
    case _ =>
      stash()
  }

  def listen(port : Int) : Receive = {
    case GetJarStoreServer =>
      sender ! JarStoreServerAddress(s"http://$host:$port/")
  }
}