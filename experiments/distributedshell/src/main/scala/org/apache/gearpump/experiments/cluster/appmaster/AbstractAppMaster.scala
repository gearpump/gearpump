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
package org.apache.gearpump.experiments.cluster.appmaster

import akka.actor._
import akka.remote.RemoteScope
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.AppMasterToMaster.GetAllWorkers
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.WorkerList
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{StartExecutorSystemTimeout, StartExecutorSystems, ExecutorSystemStarted, ExecutorSystemJvmConfig}
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.MsgToExecutor
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.{ResponsesFromExecutor, RegisterExecutor}
import org.apache.gearpump.experiments.cluster.util.ResponseBuilder
import org.apache.gearpump.util.{Constants, Util, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import akka.pattern.{ask, pipe}

abstract class AbstractAppMaster(appContext : AppMasterContext, app : Application) extends ApplicationMaster {
  import appContext._
  import context.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT

  protected val userConfig = app.userConfig
  protected val systemConfig = context.system.settings.config
  protected var master: ActorRef = appContext.masterProxy
  protected var currentExecutorId = 0
  protected val defaultMsgHandler = masterMsgHandler orElse workerMsgHandler orElse clientMsgHandler
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  protected def executorClass: Class[_ <: Actor]

  def onStart(): Unit

  override def receive: Receive = null

  override def preStart(): Unit = {
    context.become(defaultMsgHandler)
    onStart()
  }

  def masterMsgHandler: Receive = {
    case ExecutorSystemStarted(executorSystem) =>
      import executorSystem.{address, worker, resource => executorResource}
      val executorContext = ExecutorContext(currentExecutorId, worker.workerId, appId, self, executorResource)

      //start executor
      val executor = context.actorOf(
        Props(executorClass, executorContext, userConfig)
          .withDeploy(Deploy(scope = RemoteScope(address))), currentExecutorId.toString)
      executorSystem.bindLifeCycleWith(executor)
      currentExecutorId += 1
    case StartExecutorSystemTimeout =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
  }

  def workerMsgHandler: Receive = {
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      LOG.info(s"executor $executorId has been launched")
      //watch for executor termination
      context.watch(executor)
      onExecutorStarted(executor, executorId, workerId)
  }

  def clientMsgHandler: Receive = {
    case msg: MsgToExecutor =>
      Future.fold(context.children.map(_ ? msg.msg))(new ResponseBuilder) { (builder, response) =>
        builder.aggregate(response.asInstanceOf[ResponsesFromExecutor])
      }.map(_.toString()) pipeTo sender
  }

  def onExecutorStarted(executor: ActorRef, executorId: Int, workerId: Int): Unit

  def getExecutorJvmConfig: ExecutorSystemJvmConfig = {
    val config: Config = Option(app.clusterConfig).map(_.getConfig).getOrElse(ConfigFactory.empty())
    val jvmSetting = Util.resolveJvmSetting(config.withFallback(systemConfig)).executor
    ExecutorSystemJvmConfig(jvmSetting.classPath, jvmSetting.vmargs, appJar, username, config)
  }

  def launchExecutorOnEachWorker() = {
    (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]].map { list =>
      val resources = list.workers.map {
        workerId => ResourceRequest(Resource(1), workerId, relaxation = Relaxation.SPECIFICWORKER)
      }.toArray

      master ! StartExecutorSystems(resources, getExecutorJvmConfig)
    }
  }
}


