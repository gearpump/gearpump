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

package org.apache.gearpump.streaming.appmaster

import akka.actor._
import akka.remote.RemoteScope
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, ExecutorSystemStarted, StartExecutorSystemTimeout, StartExecutorSystems}
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import org.apache.gearpump.cluster.{AppDescription, AppMasterContext, ClusterConfigSource, ExecutorContext, UserConfig}
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.{ExecutorId, StreamApplication}
import org.apache.gearpump.streaming.executor.Executor
import org.apache.gearpump.util.{Constants, LogUtil, Util}

import scala.util.Try

/**
 * This will launch Executor when asked. It hide the details like requesting ExecutorSystem
 * From user.
 *
 * Please use ExecutorManager.props() to construct this actor
 *
 * @param userConfig
 * @param appContext
 * @param executorFactory
 */
private[appmaster] class ExecutorManager (
    userConfig: UserConfig,
    appContext: AppMasterContext,
    executorFactory: (ExecutorContext, UserConfig, Address, ExecutorId) => Props,
    clusterConfig: Config)
  extends Actor {

  private val LOG = LogUtil.getLogger(getClass)

  import appContext.{appId, appJar, masterProxy, username}

  private var taskManager: ActorRef = null
  implicit val actorSystem = context.system
  private val systemConfig = context.system.settings.config

  def receive: Receive = waitForTaskManager

  def waitForTaskManager: Receive = {
    case SetTaskManager(taskManager) =>
      this.taskManager = taskManager
      context.become(service orElse terminationWatch)
  }

  def service: Receive = {
    case StartExecutors(resources) =>
      masterProxy ! StartExecutorSystems(resources, getExecutorJvmConfig)
    case ExecutorSystemStarted(executorSystem) =>
      import executorSystem.{executorSystemId, address, worker, resource => executorResource}

      val executorId = executorSystemId
      val executorContext = ExecutorContext(executorId, worker.workerId, appId, appMaster =  context.parent, executorResource)

      //start executor
      val executor = context.actorOf(executorFactory(executorContext, userConfig, address, executorId),
        executorId.toString)
      executorSystem.bindLifeCycleWith(executor)

    case StartExecutorSystemTimeout =>
      taskManager ! StartExecutorsTimeOut

    case RegisterExecutor(executor, executorId, resource, workerId) =>
      LOG.info(s"executor $executorId has been launched")
      //watch for executor termination
      context.watch(executor)
      taskManager ! ExecutorStarted(executor, executorId, resource, workerId)

    case BroadCast(msg) =>
      LOG.info(s"broadcasting ${msg.getClass.getName}")
      context.children.foreach(_ ! msg)

    case GetExecutorPathList =>
      sender ! context.children.map(_.path).toList
    }

  def terminationWatch : Receive = {
    case Terminated(actor) =>
      val executorId = Try(actor.path.name.toInt)
      LOG.error(s"Executor is down ${actor.path.name}, executorId: $executorId")
      executorId match {
        case scala.util.Success(id) => taskManager ! ExecutorStopped(id)
        case scala.util.Failure(ex) => LOG.error(s"failed to get the executor Id from path string ${actor.path}" , ex)
      }
  }

  import Constants._
  private def getExecutorJvmConfig: ExecutorSystemJvmConfig = {
    val executorAkkaConfig = clusterConfig
    val jvmSetting = Util.resolveJvmSetting(executorAkkaConfig.withFallback(systemConfig)).executor
    ExecutorSystemJvmConfig(jvmSetting.classPath, jvmSetting.vmargs, appJar, username, executorAkkaConfig)
  }
}

private[appmaster] object ExecutorManager {
  case class StartExecutors(resources: Array[ResourceRequest])
  case class BroadCast(msg: Any)

  case object GetExecutorPathList

  case class ExecutorStarted(executor: ActorRef, executorId: Int, resource: Resource, workerId: Int)
  case class ExecutorStopped(executorId: Int)

  case class SetTaskManager(taskManager: ActorRef)

  case object StartExecutorsTimeOut

  def props(userConfig: UserConfig, appContext: AppMasterContext, clusterConfig: Config): Props = {
    val executorFactory =
        (executorContext: ExecutorContext,
         userConfig: UserConfig,
         address: Address,
         executorId: ExecutorId) =>
      Props(classOf[Executor], executorContext, userConfig)
        .withDeploy(Deploy(scope = RemoteScope(address)))

    Props(new ExecutorManager(userConfig, appContext, executorFactory, clusterConfig))
  }
}
