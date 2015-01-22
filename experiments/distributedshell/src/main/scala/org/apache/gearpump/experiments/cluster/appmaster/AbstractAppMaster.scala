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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterRegistered, ResourceAllocated}
import org.apache.gearpump.cluster.appmaster.ExecutorSystem
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster._
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.LaunchTask
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.experiments.cluster.executor.{TaskLaunchData, DefaultExecutor}
import org.apache.gearpump.experiments.cluster.task.TaskContext
import org.apache.gearpump.util.ActorSystemBooter.BindLifeCycle
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil, Util, LogUtil}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}

abstract class AbstractAppMaster(appContext : AppMasterContext, app : Application) extends ApplicationMaster {
  import context.dispatcher
  import appContext._

  protected val userConfig = app.userConfig
  protected val systemConfig = context.system.settings.config
  protected var master: ActorRef = appContext.masterProxy
  protected var currentExecutorId = 0
  protected val executorClass: Class[_ <: DefaultExecutor]
  protected val defaultMsgHandler = masterMsgHandler orElse workerMsgHandler

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  def onStart(): Unit

  override def receive: Receive = null

  override def preStart(): Unit = {
    onStart()
  }

  def masterMsgHandler: Receive = {
    case executorSystem @ ExecutorSystem(_, address, _, executorResource, worker) =>
      val executorContext = ExecutorContext(currentExecutorId, worker.workerId, appId, self, resource)
      val executor = context.actorOf(
        Props(executorClass, executorContext, userConfig)
          .withDeploy(Deploy(scope = RemoteScope(address))), currentExecutorId.toString)

      currentExecutorId += 1
      executorSystem.bindLifeCycleWith(executor)
  }

  def workerMsgHandler : Receive = {
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      //watch for executor termination
      context.watch(executor)
      def launchTask(remainResources: Resource): Unit = {
        if (remainResources > Resource.empty) {
          val TaskLaunchData(taskClass, userConfig) = scheduleTaskOnWorker(workerId)

          executor ! LaunchTask(TaskContext(executorId, appId, self), ActorUtil.loadClass(taskClass), userConfig)

          //Todo: subtract the actual resource used by task
          val usedResource = Resource(1)
          launchTask(remainResources - usedResource)
        }
      }
      launchTask(resource)
  }

  def scheduleTaskOnWorker(workerId: Int): TaskLaunchData
}
