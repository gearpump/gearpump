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
package org.apache.gearpump.experiments.distributeservice

import java.io.File
import scala.concurrent.Future

import akka.actor.{Deploy, Props}
import akka.pattern.{ask, pipe}
import akka.remote.RemoteScope
import com.typesafe.config.Config
import org.slf4j.Logger

import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, ExecutorSystemStarted, StartExecutorSystemTimeout}
import org.apache.gearpump.cluster.{AppDescription, AppMasterContext, ApplicationMaster, ExecutorContext}
import org.apache.gearpump.experiments.distributeservice.DistServiceAppMaster.{FileContainer, GetFileContainer, InstallService}
import org.apache.gearpump.util._

class DistServiceAppMaster(appContext: AppMasterContext, app: AppDescription)
  extends ApplicationMaster {
  import appContext._
  import context.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  private var currentExecutorId = 0
  private var fileServerPort = -1

  val rootDirectory = new File("/")
  val host = context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)
  val server = context.actorOf(Props(classOf[FileServer], rootDirectory, host, 0))

  override def preStart(): Unit = {
    LOG.info(s"Distribute Service AppMaster started")
    ActorUtil.launchExecutorOnEachWorker(masterProxy, getExecutorJvmConfig, self)
  }

  (server ? FileServer.GetPort).asInstanceOf[Future[FileServer.Port]] pipeTo self

  override def receive: Receive = {
    case ExecutorSystemStarted(executorSystem, _) =>
      import executorSystem.{address, resource => executorResource, worker}
      val executorContext = ExecutorContext(currentExecutorId, worker,
        appId, app.name, self, executorResource)
      // start executor
      val executor = context.actorOf(Props(classOf[DistServiceExecutor],
        executorContext, app.userConfig).withDeploy(
        Deploy(scope = RemoteScope(address))), currentExecutorId.toString)
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
    val config: Config = app.clusterConfig
    val jvmSetting = Util.resolveJvmSetting(
      config.withFallback(context.system.settings.config)).executor
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
      script: Array[Byte],
      serviceName: String,
      serviceSettings: Map[String, Any])
}
