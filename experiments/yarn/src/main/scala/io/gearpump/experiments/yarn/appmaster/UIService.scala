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

package io.gearpump.experiments.yarn.appmaster

import akka.actor._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.services.main.Services
import io.gearpump.transport.HostPort
import io.gearpump.util.{ActorUtil, Constants, LogUtil}

import scala.concurrent.Future

trait UIFactory {
  def props(masters: List[HostPort], host: String, port: Int): Props
}

class UIService(masters: List[HostPort], host: String, port: Int) extends Actor {
  private val LOG = LogUtil.getLogger(getClass)

  private val supervisor = ActorUtil.getFullPath(context.system, context.parent.path)
  private var configFile: java.io.File = null

  implicit val dispatcher = context.dispatcher

  override def postStop: Unit = {
    if (configFile != null) {
      configFile.delete()
      configFile = null
    }

    // TODO: fix this
    // Hack around to Kill the UI server
    Services.kill()
  }

  override def preStart(): Unit = {
    Future(start)
  }

  def start: Unit = {
    val mastersArg = masters.mkString(",")
    LOG.info(s"Launching services -master $mastersArg")

    configFile = java.io.File.createTempFile("uiserver", ".conf")


    val config = context.system.settings.config.
      withValue(Constants.GEARPUMP_SERVICE_HOST, ConfigValueFactory.fromAnyRef(host)).
      withValue(Constants.GEARPUMP_SERVICE_HTTP, ConfigValueFactory.fromAnyRef(port.toString)).
      withValue(Constants.NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(host))


    ClusterConfig.saveConfig(config, configFile)

    val master = masters.head

    ConfigFactory.invalidateCaches()
    launch(supervisor, master.host, master.port, configFile.toString)
  }

  def launch(supervisor: String, masterHost: String, masterPort: Int, configFile: String): Unit = {
    Services.main(Array("-supervisor", supervisor, "-master", s"$masterHost:$masterPort"
     , "-conf", configFile))
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }
}

object UIService extends UIFactory{
  override def props(masters: List[HostPort], host: String, port: Int): Props = {
    Props(new UIService(masters, host, port))
  }
}