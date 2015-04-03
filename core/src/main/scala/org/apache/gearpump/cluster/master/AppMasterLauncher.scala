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

package org.apache.gearpump.cluster.master

import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.actor.{Actor, ActorRef, Props, _}
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.AppMasterToWorker.{LaunchExecutor, ShutdownExecutor}
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.appmaster.AppMasterRuntimeEnvironment
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter._
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil, LogUtil, Util}
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

class AppMasterLauncher(
    appId : Int, executorId: Int, app : AppDescription,
    jar: Option[AppJar], username : String, master : ActorRef, client: Option[ActorRef])
  extends Actor {
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  import app._

  val scheduler = context.system.scheduler
  val systemConfig = context.system.settings.config
  val TIMEOUT = Duration(15, TimeUnit.SECONDS)

  val appMasterAkkaConfig: Config = Option(clusterConfig).map(_.getConfig).getOrElse(ConfigFactory.empty())
  LOG.info(s"AppManager asking Master for resource for app $appId...")
  master ! RequestResource(appId, ResourceRequest(Resource(1)))

  def receive : Receive = waitForResourceAllocation

  def waitForResourceAllocation : Receive = {
    case ResourceAllocated(allocations) =>
      LOG.info(s"Resource allocated for appMaster $appId")
      val ResourceAllocation(resource, worker, workerId) = allocations(0)

      val submissionTime = System.currentTimeMillis()

      val appMasterInfo = AppMasterRuntimeInfo(appId, app.name, worker, username,
        submissionTime, config = appMasterAkkaConfig.withFallback(systemConfig).withOnlyPath(GEARPUMP))
      val appMasterContext = AppMasterContext(appId, username, resource, jar, null, appMasterInfo)

      LOG.info(s"Try to launch a executor for app Master on ${worker} for app $appId")
      val name = ActorUtil.actorNameForExecutor(appId, executorId)
      val selfPath = ActorUtil.getFullPath(context.system, self.path)

      val jvmSetting = Util.resolveJvmSetting(appMasterAkkaConfig.withFallback(systemConfig)).appMater
      val executorJVM = ExecutorJVMConfig(jvmSetting.classPath ,jvmSetting.vmargs,
        classOf[ActorSystemBooter].getName, Array(name, selfPath), jar, username, appMasterAkkaConfig)

      worker ! LaunchExecutor(appId, executorId, resource, executorJVM)
      context.become(waitForActorSystemToStart(worker, appMasterContext, app.userConfig, resource))
  }

  def waitForActorSystemToStart(worker : ActorRef, appContext : AppMasterContext, user : UserConfig, resource: Resource) : Receive = {
    case ExecutorLaunchRejected(reason, ex) =>
      LOG.error(s"Executor Launch failed reason：$reason", ex)
      LOG.info(s"reallocate resource $resource to start appmaster")
      master ! RequestResource(appId, ResourceRequest(resource))
      context.become(waitForResourceAllocation)
    case RegisterActorSystem(systemPath) =>
      LOG.info(s"Received RegisterActorSystem $systemPath for app master")
      sender ! ActorSystemRegistered(worker)

      val masterAddress = systemConfig.getStringList(GEARPUMP_CLUSTER_MASTERS)
        .asScala.map(HostPort(_)).map(ActorUtil.getMasterActorPath(_))
      sender ! CreateActor(AppMasterRuntimeEnvironment.props(masterAddress, app, appContext), s"appdaemon$appId")

      import context.dispatcher
      val appMasterTimeout = scheduler.scheduleOnce(TIMEOUT, self,
        CreateActorFailed(app.appMaster, new TimeoutException))
      context.become(waitForAppMasterToStart(worker, appMasterTimeout))
  }

  def waitForAppMasterToStart(worker : ActorRef, cancel: Cancellable) : Receive = {
    case ActorCreated(appMaster, _) =>
      cancel.cancel()
      sender ! BindLifeCycle(appMaster)
      LOG.info(s"AppMaster is created, stopping myself...")
      replyToClient(SubmitApplicationResult(Success(appId)))
      context.stop(self)
    case CreateActorFailed(name, reason) =>
      cancel.cancel()
      worker ! ShutdownExecutor(appId, executorId, reason.getMessage)
      replyToClient(SubmitApplicationResult(Failure(reason)))
      context.stop(self)
  }

  def replyToClient(result : SubmitApplicationResult) : Unit = {
    if (client.isDefined) {
      client.get.tell(result, master)
    }
  }
}

object AppMasterLauncher extends AppMasterLauncherFactory{
  def props(appId : Int, executorId: Int, app : AppDescription, jar: Option[AppJar], username : String, master : ActorRef, client: Option[ActorRef]) = {
    Props(new AppMasterLauncher(appId, executorId, app, jar, username, master, client))
  }
}

trait AppMasterLauncherFactory {
  def props(appId : Int, executorId: Int, app : AppDescription, jar: Option[AppJar], username : String, master : ActorRef, client: Option[ActorRef]) : Props
}