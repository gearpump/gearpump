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
package org.apache.gearpump.experiments.distributeservice

import java.io.File

import akka.actor.{Deploy, Props}
import akka.remote.RemoteScope
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, StartExecutorSystemTimeout, ExecutorSystemStarted}
import org.apache.gearpump.cluster.{ExecutorContext, ApplicationMaster, AppDescription, AppMasterContext}
import org.apache.gearpump.experiments.distributeservice.DistServiceAppMaster.{InstallService, FileContainer, GetFileContainer}
import org.apache.gearpump.util._
import org.slf4j.Logger

import akka.pattern.{ask, pipe}
import scala.concurrent.Future

class DistServiceAppMaster(appContext : AppMasterContext, app : AppDescription) extends ApplicationMaster {
  import appContext._
  import context.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  private var currentExecutorId = 0
  private var fileServerPort = -1

  val rootDirectory = new File("/")
  val host = context.system.settings.config.getString(Constants.NETTY_TCP_HOSTNAME)
  val server = context.actorOf(Props(classOf[FileServer], rootDirectory, host , 0))

  override def preStart(): Unit = {
    LOG.info(s"Distribute Service AppMaster started")
    ActorUtil.launchExecutorOnEachWorker(masterProxy, getExecutorJvmConfig, self)
  }

  (server ? FileServer.GetPort).asInstanceOf[Future[FileServer.Port]] pipeTo self

  override def receive: Receive = {
    case ExecutorSystemStarted(executorSystem) =>
      import executorSystem.{address, worker, resource => executorResource}
      val executorContext = ExecutorContext(currentExecutorId, worker.workerId, appId, self, executorResource)
      //start executor
      val executor = context.actorOf(Props(classOf[DistServiceExecutor], executorContext, app.userConfig)
        .withDeploy(Deploy(scope = RemoteScope(address))), currentExecutorId.toString)
      executorSystem.bindLifeCycleWith(executor)
      currentExecutorId += 1
    case StartExecutorSystemTimeout =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
    case FileServer.Port(port) =>
      this.fileServerPort = port
    case GetFileContainer =>
      val name = Math.abs(new java.util.Random().nextLong()).toString
      sender ! new FileContainer(s"http://$host:$fileServerPort/$name")
    case installService: InstallService =>
      context.children.foreach(_ ! installService)
  }

  private def getExecutorJvmConfig: ExecutorSystemJvmConfig = {
    val config: Config = Option(app.clusterConfig).map(_.getConfig).getOrElse(ConfigFactory.empty())
    val jvmSetting = Util.resolveJvmSetting(config.withFallback(context.system.settings.config)).executor
    ExecutorSystemJvmConfig(jvmSetting.classPath, jvmSetting.vmargs,
      appJar, username, config)
  }
}

object DistServiceAppMaster {
  case object GetFileContainer

  case class FileContainer(url: String)

  case class InstallService(
    url: String,
    zipFileName: String,
    targetPath: String,
    script : Array[Byte],
    serviceName: String,
    serviceSettings: Map[String, Any])
}
