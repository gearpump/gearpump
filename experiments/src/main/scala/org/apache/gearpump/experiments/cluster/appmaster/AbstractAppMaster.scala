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

import java.io.{File, FileInputStream}
import java.util.concurrent.TimeUnit

import akka.actor._
import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterRegistered, ResourceAllocated}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppJar, ApplicationMaster}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.LaunchTask
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.experiments.cluster.executor.{TaskLaunchData, DefaultExecutor}
import org.apache.gearpump.util.ActorSystemBooter.BindLifeCycle
import org.apache.gearpump.util.{ActorUtil, Configs, LogUtil}
import org.slf4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}

abstract class AbstractAppMaster(config: Configs) extends ApplicationMaster {
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  import context.dispatcher
  protected val appJar = loadJar
  protected val appId = config.appId
  protected var master: ActorRef = null
  protected val username = config.username
  protected val resource = config.resource
  protected val masterProxy = config.masterProxy
  protected val masterExecutorId = config.executorId
  protected val appDescription = config.appDescription
  protected var currentExecutorId = masterExecutorId + 1
  protected val registerData = config.appMasterRegisterData
  protected val executorClass: Class[_ <: DefaultExecutor]

  protected val defaultMsgHandler = masterMsgHandler orElse workerMsgHandler orElse selfMsgHandler

  def onStart(): Unit

  override def receive: Receive = null

  override def preStart(): Unit = {
    registerToMaster()
  }

  def registerToMaster(): Unit = {
    context.become(waitForMasterToConfirmRegistration(repeatActionUtil(30)(masterProxy ! RegisterAppMaster(self, appId, masterExecutorId, resource, registerData))))
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
        val executorConfig = appDescription.conf.withAppId(appId).withUserName(username).withAppMaster(self).withExecutorId(currentExecutorId).withResource(resource).withWorkerId(actorToWorkerId.get(worker).get)
        context.actorOf(Props(classOf[ExecutorLauncher], executorClass, worker, appId, currentExecutorId, resource, executorConfig, appJar), s"launcher${currentExecutorId}")
        currentExecutorId += 1
      })
  }

  def workerMsgHandler : Receive = {
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      //watch for executor termination
      context.watch(executor)
      def launchTask(remainResources: Resource): Unit = {
        if (remainResources.greaterThan(Resource.empty)) {
          val TaskLaunchData(taskClass, config) = scheduleTaskOnWorker(workerId)
          val launchConfig = appDescription.conf.withAppId(appId).withUserName(username).withExecutorId(executorId).
            withAppMaster(self).withConfig(config)
          executor ! LaunchTask(launchConfig, ActorUtil.loadClass(taskClass))

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

  private def loadJar: Option[AppJar] = {
    val fileName: Option[String] = Option[String](System.getProperty("app.jar"))
    fileName match {
      case Some(name) =>
        LOG.info(s"APPMASTER app.jar name = $name")
        val file = new File(name)
        if(file.exists) {
          val fis = new FileInputStream(file)
          val buf = ListBuffer[Byte]()
          var b = fis.read()
          while (b != -1) {
            buf.append(b.byteValue)
            b = fis.read()
          }
          return Option(AppJar(name, buf.toArray))
        }
        LOG.info("APPMASTER app.jar is NULL")
        None
      case None =>
        LOG.info("APPMASTER app.jar is NULL")
        None
    }
  }
}
