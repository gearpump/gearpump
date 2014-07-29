package org.apache.gears.cluster

/**
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

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump._
import org.apache.gearpump.util.ActorSystemBooter.BindLifeCycle
import org.apache.gearpump.util.ExecutorLauncher
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.ExecutorToWorker._
import org.apache.gears.cluster.MasterToWorker._
import org.apache.gears.cluster.WorkerToAppMaster._
import org.apache.gears.cluster.WorkerToMaster._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global

class Worker(id : Int, master : ActorRef) extends Actor{
  import org.apache.gears.cluster.Worker._

  private var resource = 100
  override def receive : Receive = waitForMasterConfirm

  LOG.info(s"Worker $id is starting...")

  def waitForMasterConfirm : Receive = {
    case WorkerRegistered =>
      LOG.info(s"Worker $id Registered ....")
      sender ! ResourceUpdate(id, resource)
      context.become(appMasterMsgHandler orElse executorMsgHandler orElse  ActorUtil.defaultMsgHandler(self))
  }

  def appMasterMsgHandler : Receive = {
    case ShutdownExecutor(appId, executorId) => {
      val actorName = actorNameFor(appId, executorId)
      LOG.info(s"Worker shutting down executor: $actorName")
      if (context.child(actorName).isDefined) {
        LOG.info(s"Shuttting down child: ${context.child(actorName).get.path.toString}")
        context.stop(context.child(actorName).get)
      } else {
        LOG.info(s"There is no child $actorName, ignore this message")
      }
    }
    case launch : LaunchExecutor => {
      LOG.info(s"Worker[$id] LaunchExecutor ....$launch")
      if (resource < launch.slots) {
        sender ! ExecutorLaunchFailed(launch, "There is no free resource on this machine")
      } else {
        resource = resource - launch.slots
        val appMaster = sender
        launchExecutor(context, self, appMaster, launch.copy(executorConfig = launch.executorConfig.
          withSlots(launch.slots).
          withExecutorId(launch.executorId)))
      }
    }
    case LaunchExecutorOnSystem(appMaster, launch, system) => {
      LOG.info(s"Worker[$id] LaunchExecutorOnSystem ...$system")
      val executorProps = Props(launch.executorClass, launch.executorConfig).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(system.path))))
      val executor = context.actorOf(executorProps, actorNameFor(launch.appId, launch.executorId))
      system.daemonActor ! BindLifeCycle(executor)
    }
  }

  def executorMsgHandler : Receive = {
    case RegisterExecutor(appMaster, appId, executorId, slots) => {
      LOG.info(s"Register Executor for $appId, $executorId to ${appMaster.path.toString}...")
      appMaster ! ExecutorLaunched(sender, executorId, slots)
    }
  }

  def actorNameFor(appId : Int, executorId : Int) = "app_" + appId + "_executor_" + executorId

  override def preStart() : Unit = {
    master ! RegisterWorker(id)
    LOG.info(s"Worker[$id] Sending master RegisterWorker")
  }
}

object Worker {
  val LOG : Logger = LoggerFactory.getLogger(classOf[Worker])

  def launchExecutor(context : ActorRefFactory, self : ActorRef, appMaster : ActorRef, launch : LaunchExecutor) : Unit = {
    val newSystemPath = ExecutorLauncher.launch(context, launch).map(system => {
      self ! LaunchExecutorOnSystem(appMaster, launch, system)
    }).onFailure {
      case ex => self ! ExecutorLaunchFailed(launch, "failed to new system path", ex)
    }
  }
}