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
import org.apache.gears.cluster.AppMasterToMaster._
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.ClientToMaster._
import org.apache.gears.cluster.MasterToAppMaster._
import org.apache.gears.cluster.MasterToClient.{ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gears.cluster.WorkerToAppMaster._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Failure}


class AppManager() extends Actor {
  import org.apache.gears.cluster.AppManager._

  private var master : ActorRef = null
  private var executorCount : Int = 0;
  private var appId : Int = 0;
  def receive : Receive = clientMsgHandler

  def clientMsgHandler : Receive = {
    case SubmitApplication(appMasterClass, config, app) =>
      LOG.info(s"AppManager Submiting Application $appId...")
      val appWatcher = context.actorOf(Props(classOf[AppMasterWatcher], appId, appMasterClass, config, app), appId.toString)
      sender.tell(SubmitApplicationResult(Success(appId)), context.parent)
      appId += 1
    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val child = context.child(appId.toString)
      if (child.isEmpty) {
        sender.tell(ShutdownApplicationResult(Failure(new Exception(s"App $appId not found"))), context.parent)
      } else {
        LOG.info(s"Shutting down  ${child.get.path}")
        child.get.forward(ShutdownAppMaster)
      }
  }
}

object AppManager {

  //app master will always use executor id -1 to avoid conflict with executor
  private val masterExecutorId = -1
  private val LOG: Logger = LoggerFactory.getLogger(classOf[AppManager])

  class AppMasterWatcher(appId : Int, appMasterClass : Class[_ <: Actor], appConfig : Configs, app : Application) extends Actor {
    def receive : Receive = waitForResourceAllocation

    def waitForResourceAllocation : Receive = {
      case ResourceAllocated(resource) => {
        LOG.info(s"Resource allocated for appMaster $appId")
        val Resource(worker, slots) = resource(0)
        val appMasterConfig = appConfig.withAppId(appId).withAppDescription(app).withMaster(sender).withAppManager(self).withExecutorId(masterExecutorId).withSlots(slots)
        LOG.info(s"Try to launch a executor for app Master on $worker for app $appId")
        worker ! LaunchExecutor(appId, masterExecutorId, slots, appMasterClass, appMasterConfig, new DefaultExecutorContext)
        context.become(waitForAppMasterToStart)
      }
    }

    def waitForAppMasterToStart : Receive = {
      case ExecutorLaunched(executor, executorId, slots) => {
        LOG.info(s"Executor $executorId has been launched...")
        context.watch(executor)
        context.become(waitForShutdownCommand(sender, executorId) orElse terminationWatch(executor))
      }
      case ExecutorLaunchFailed(launch, reason, ex) => {
        LOG.error(s"Executor Launch failed $launch, reason：$reason", ex)
      }
    }

    def waitForShutdownCommand(worker : ActorRef, executorId : Int) : Receive = {
      case ShutdownAppMaster => {

        LOG.info(s"Shuttdown app master at ${worker.path.toString}, appId: $appId, executorId: $executorId")

        worker ! ShutdownExecutor(appId, executorId, s"AppMaster $appId shutdown requested by master...")
        sender ! ShutdownApplicationResult(Success(appId))
        //self myself
        self ! PoisonPill
      }
    }

    def terminationWatch(appMaster : ActorRef) : Receive = {
      case terminate : Terminated => {
        terminate.getAddressTerminated()
        if (terminate.actor.compareTo(appMaster) == 0) {
          LOG.info(s"App Master is terminiated, network down: ${terminate.getAddressTerminated()}")
          context.stop(self)
        }
      }
    }

    override def preStart : Unit = {
      val master = context.actorSelection("../../")
      master ! RequestResource(appId, 1)
      LOG.info(s"AppManager asking Master for resource for app $appId...")
    }
  }
}