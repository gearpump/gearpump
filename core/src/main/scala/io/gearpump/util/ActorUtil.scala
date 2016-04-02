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

package io.gearpump.util

import akka.actor.Actor.Receive
import akka.actor._
import akka.pattern.ask
import io.gearpump.WorkerId
import io.gearpump.cluster.AppMasterToMaster.GetAllWorkers
import io.gearpump.cluster.ClientToMaster.{ResolveAppId, ResolveWorkerId}
import io.gearpump.cluster.MasterToAppMaster.WorkerList
import io.gearpump.cluster.MasterToClient.{ResolveAppIdResult, ResolveWorkerIdResult}
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, StartExecutorSystems}
import io.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import io.gearpump.transport.HostPort
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

object ActorUtil {
   private val LOG: Logger = LogUtil.getLogger(getClass)

  def getSystemAddress(system : ActorSystem) : Address = {
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  }

  def getFullPath(system : ActorSystem, path : ActorPath): String = {
    path.toStringWithAddress(getSystemAddress(system))
  }

  def getHostname(actor : ActorRef): String = {
    val path = actor.path
    path.address.host.getOrElse("localhost")
  }

  def defaultMsgHandler(actor : ActorRef) : Receive = {
    case msg : Any =>
    LOG.error(s"Cannot find a matching message, ${msg.getClass.toString}, forwarded from $actor")
  }

  def printActorSystemTree(system : ActorSystem) : Unit = {
    val extendedSystem = system.asInstanceOf[ExtendedActorSystem]
    val clazz = system.getClass
    val m = clazz.getDeclaredMethod("printTree")
    m.setAccessible(true)
    LOG.info(m.invoke(system).asInstanceOf[String])
  }

  // Check whether a actor is child actor by simply examining name
  //TODO: fix this, we should also check the path to root besides name
  def isChildActorPath(parent : ActorRef, child : ActorRef) : Boolean = {
    if (null != child) {
      parent.path.name == child.path.parent.name
    } else {
      false
    }
  }

  def actorNameForExecutor(appId : Int, executorId : Int) = "app" + appId + "-executor" + executorId

  /**
   * TODO:
   * Currently we explicitly require the master contacts to be started with this path pattern
   * 'akka.tcp://$MASTER@${master.host}:${master.port}/user/$MASTER'
   *
   */
  def getMasterActorPath(master: HostPort): ActorPath = {
    import Constants.MASTER
    ActorPath.fromString(s"akka.tcp://$MASTER@${master.host}:${master.port}/user/$MASTER")
  }

  def launchExecutorOnEachWorker(master: ActorRef, executorJvmConfig: ExecutorSystemJvmConfig,
    sender: ActorRef)(implicit executor : scala.concurrent.ExecutionContext) = {
    implicit val timeout = Constants.FUTURE_TIMEOUT

    (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]].map { list =>
      val resources = list.workers.map {
        workerId => ResourceRequest(Resource(1), workerId, relaxation = Relaxation.SPECIFICWORKER)
      }.toArray

      master.tell(StartExecutorSystems(resources, executorJvmConfig), sender)
    }
  }


  def askAppMaster[T](master: ActorRef, appId: Int, msg: Any)(implicit ex: ExecutionContext): Future[T] = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val appmaster =  askActor[ResolveAppIdResult](master, ResolveAppId(appId)).flatMap { result =>
      if (result.appMaster.isSuccess) {
        Future.successful(result.appMaster.get)
      } else {
        Future.failed(result.appMaster.failed.get)
      }
    }
    appmaster.flatMap(askActor[T](_, msg))
  }

  def askWorker[T](master: ActorRef, workerId: WorkerId, msg: Any)(implicit ex: ExecutionContext): Future[T] = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val worker =  askActor[ResolveWorkerIdResult](master, ResolveWorkerId(workerId)).flatMap { result =>
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
}
