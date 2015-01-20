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
import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterRegistered, ResourceAllocated}
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
  protected var master: ActorRef = null
  protected var currentExecutorId = 0
  protected val executorClass: Class[_ <: DefaultExecutor]
  protected val defaultMsgHandler = masterMsgHandler orElse workerMsgHandler orElse selfMsgHandler

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  def onStart(): Unit

  override def receive: Receive = null

  override def preStart(): Unit = {
    registerToMaster()
  }

  def registerToMaster(): Unit = {
    context.become(waitForMasterToConfirmRegistration(repeatActionUtil(30)(masterProxy ! RegisterAppMaster(self, registerData))))
  }

  def waitForMasterToConfirmRegistration(killSelf : Cancellable) : Receive = {
    case AppMasterRegistered(appId, master) =>
      LOG.info(s"Application $appId registered to Master")
      killSelf.cancel()
      val notInitiated = this.master == null
      this.master = master
      context.watch(master)
      if(notInitiated){
        onStart()
      }
  }

  def masterMsgHandler: Receive = {
    case ResourceAllocated(allocations) =>
      LOG.info(s"Application $appId get resource $allocations")
      val actorToWorkerId = mutable.HashMap.empty[ActorRef, Int]
      val groupedResource = allocations.groupBy(_.worker).mapValues(_.foldLeft(Resource.empty)((totalResource, request) => totalResource add request.resource)).toArray
      allocations.foreach(allocation => actorToWorkerId.put(allocation.worker, allocation.workerId))

      groupedResource.map((workerAndResources) => {
        val (worker, resource) = workerAndResources
        val executorContext = ExecutorContext(currentExecutorId, actorToWorkerId.get(worker).get, appId, self, resource)
        val name = ActorUtil.actorNameForExecutor(appId, currentExecutorId)
        val launcherName = s"launcher${currentExecutorId}"
        val launcherPath = ActorUtil.getFullPath(context.system, self.path.child(launcherName))

        val jvmSetting = Util.resolveJvmSetting(systemConfig).executor
        val launchJVM = ExecutorJVMConfig(jvmSetting.classPath, jvmSetting.vmargs,
          classOf[ActorSystemBooter].getName, Array(name, launcherPath), appJar, username)

        val launch = LaunchExecutor(appId, currentExecutorId, resource, launchJVM)

        context.actorOf(Props(classOf[ExecutorLauncher], executorClass, worker, launch, executorContext, userConfig), launcherName)
        currentExecutorId += 1
      })
  }

  def workerMsgHandler : Receive = {
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      //watch for executor termination
      context.watch(executor)
      def launchTask(remainResources: Resource): Unit = {
        if (remainResources.greaterThan(Resource.empty)) {
          val TaskLaunchData(taskClass, userConfig) = scheduleTaskOnWorker(workerId)

          executor ! LaunchTask(TaskContext(executorId, appId, self), ActorUtil.loadClass(taskClass), userConfig)

          //Todo: subtract the actual resource used by task
          val usedResource = Resource(1)
          launchTask(remainResources subtract usedResource)
        }
      }
      launchTask(resource)
  }

  def selfMsgHandler : Receive = {
    case LaunchExecutorActor(conf : Props, executorId : Int, daemon : ActorRef) =>
      val executor = context.actorOf(conf, executorId.toString)
      daemon ! BindLifeCycle(executor)
  }

  def scheduleTaskOnWorker(workerId: Int): TaskLaunchData

  def repeatActionUtil(seconds: Int)(action : => Unit) : Cancellable = {
    val cancelSend = context.system.scheduler.schedule(Duration.Zero, Duration(2, TimeUnit.SECONDS))(action)
    val cancelSuicide = context.system.scheduler.scheduleOnce(FiniteDuration(seconds, TimeUnit.SECONDS), self, PoisonPill)
    new Cancellable {
      def cancel(): Boolean = {
        val result1 = cancelSend.cancel()
        val result2 = cancelSuicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        cancelSend.isCancelled && cancelSuicide.isCancelled
      }
    }
  }
}
