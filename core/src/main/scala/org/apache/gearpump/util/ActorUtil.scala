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

package org.apache.gearpump.util

import org.apache.gearpump.cluster.{ApplicationStatus, AppMasterContext}
import org.apache.gearpump.cluster.worker.WorkerId

import scala.concurrent.{Await, ExecutionContext, Future}
import akka.actor.Actor.Receive
import akka.actor._
import akka.pattern.ask
import org.slf4j.Logger
import akka.util.Timeout
import org.apache.gearpump.cluster.AppMasterToMaster.{ApplicationStatusChanged, GetAllWorkers}
import org.apache.gearpump.cluster.ClientToMaster.{ResolveAppId, ResolveWorkerId}
import org.apache.gearpump.cluster.MasterToAppMaster.WorkerList
import org.apache.gearpump.cluster.MasterToClient.{ResolveAppIdResult, ResolveWorkerIdResult}
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, StartExecutorSystems}
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.transport.HostPort

import scala.concurrent.duration.Duration

object ActorUtil {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def getSystemAddress(system: ActorSystem): Address = {
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  }

  def getFullPath(system: ActorSystem, actorRef: ActorRef): String = {
    if (actorRef != ActorRef.noSender) {
      getFullPath(system, actorRef.path)
    } else {
      ""
    }
  }

  def getFullPath(system: ActorSystem, path: ActorPath): String = {
    path.toStringWithAddress(getSystemAddress(system))
  }

  def getHostname(actor: ActorRef): String = {
    val path = actor.path
    path.address.host.getOrElse("localhost")
  }

  def defaultMsgHandler(actor: ActorRef): Receive = {
    case msg: Any =>
      LOG.error(s"Cannot find a matching message, $msg, forwarded from $actor")
  }

  def printActorSystemTree(system: ActorSystem): Unit = {
    val clazz = system.getClass
    val m = clazz.getDeclaredMethod("printTree")
    m.setAccessible(true)
    LOG.info(m.invoke(system).asInstanceOf[String])
  }

  /** Checks whether a actor is child actor by simply examining name */
  // TODO: fix this, we should also check the path to root besides name
  def isChildActorPath(parent: ActorRef, child: ActorRef): Boolean = {
    if (null != child) {
      parent.path.name == child.path.parent.name
    } else {
      false
    }
  }

  def actorNameForExecutor(appId: Int, executorId: Int): String = "app" + appId + "-executor" +
    executorId

  // TODO: Currently we explicitly require the master contacts to be started with this path pattern
  // akka.tcp://$MASTER@${master.host}:${master.port}/user/$MASTER
  def getMasterActorPath(master: HostPort): ActorPath = {
    import org.apache.gearpump.util.Constants.MASTER
    ActorPath.fromString(s"akka.tcp://$MASTER@${master.host}:${master.port}/user/$MASTER")
  }

  def launchExecutorOnEachWorker(master: ActorRef, executorJvmConfig: ExecutorSystemJvmConfig,
      sender: ActorRef)(implicit executor: scala.concurrent.ExecutionContext): Unit = {
    implicit val timeout = Constants.FUTURE_TIMEOUT

    (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]].map { list =>
      val resources = list.workers.map {
        workerId => ResourceRequest(Resource(1), workerId, relaxation = Relaxation.SPECIFICWORKER)
      }.toArray

      sender ! list
      master.tell(StartExecutorSystems(resources, executorJvmConfig), sender)
    }
  }

  def tellMasterIfApplicationReady(workerNum: Option[Int], executorSystemNum: Int,
      appContext: AppMasterContext): Unit = {
    if (workerNum.contains(executorSystemNum)) {
      appContext.masterProxy ! ApplicationStatusChanged(appContext.appId, ApplicationStatus.ACTIVE,
        System.currentTimeMillis(), null)
    }
  }

  def askAppMaster[T](master: ActorRef, appId: Int, msg: Any)(implicit ex: ExecutionContext)
    : Future[T] = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val appmaster = askActor[ResolveAppIdResult](master, ResolveAppId(appId)).flatMap { result =>
      if (result.appMaster.isSuccess) {
        Future.successful(result.appMaster.get)
      } else {
        Future.failed(result.appMaster.failed.get)
      }
    }
    appmaster.flatMap(askActor[T](_, msg))
  }

  def askWorker[T](master: ActorRef, workerId: WorkerId, msg: Any)(implicit ex: ExecutionContext)
    : Future[T] = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val worker = askActor[ResolveWorkerIdResult](master, ResolveWorkerId(workerId))
      .flatMap { result =>
        if (result.worker.isSuccess) {
          Future.successful(result.worker.get)
        } else {
          Future.failed(result.worker.failed.get)
        }
      }
    worker.flatMap(askActor[T](_, msg))
  }

  def askActor[T](actor: ActorRef, msg: Any)(implicit ex: ExecutionContext): Future[T] = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    (actor ? msg).asInstanceOf[Future[T]]
  }

  def askActor[T](actor: ActorRef, msg: Any, timeout: Timeout)(implicit ex: ExecutionContext): T = {
    askActor(actor, msg, timeout, ActorRef.noSender)
  }

  def askActor[T](actor: ActorRef, msg: Any, timeout: Timeout, sender: ActorRef)
    (implicit ex: ExecutionContext): T = {
    Await.result(actor.ask(msg)(timeout, sender).asInstanceOf[Future[T]], Duration.Inf)
  }
}
