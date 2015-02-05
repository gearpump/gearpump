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
package org.apache.gearpump.distributedshell

import akka.actor.{Deploy, Props}
import akka.remote.RemoteScope
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{StartExecutorSystemTimeout, ExecutorSystemStarted}
import org.apache.gearpump.cluster.{ApplicationMaster, ExecutorContext, AppMasterContext, Application}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.MsgToExecutor
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.{ResponseFromExecutor, RegisterExecutor}
import org.apache.gearpump.experiments.cluster.util.ResponseBuilder
import org.apache.gearpump.util.{Constants, ActorUtil, Util}
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.Future
import akka.pattern.{ask, pipe}

class DistShellAppMaster(appContext : AppMasterContext, app : Application) extends ApplicationMaster {
  import appContext._
  import context.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  protected var currentExecutorId = 0

  override def preStart(): Unit = {
    LOG.info(s"Distributed Shell AppMaster started")
    val executorJvmConfig = Util.getExecutorJvmConfig(app.clusterConfig, context.system, appContext)
    ActorUtil.launchExecutorOnEachWorker(masterProxy, executorJvmConfig, self)
  }

  override def receive: Receive = {
    case ExecutorSystemStarted(executorSystem) =>
      import executorSystem.{address, worker, resource => executorResource}
      val executorContext = ExecutorContext(currentExecutorId, worker.workerId, appId, self, executorResource)
      //start executor
      val executor = context.actorOf(
        Props(classOf[ShellExecutor], executorContext, app.userConfig)
          .withDeploy(Deploy(scope = RemoteScope(address))), currentExecutorId.toString)
      executorSystem.bindLifeCycleWith(executor)
      currentExecutorId += 1
    case StartExecutorSystemTimeout =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      LOG.info(s"executor $executorId has been launched")
      //watch for executor termination
      context.watch(executor)
    case msg: MsgToExecutor =>
      Future.fold(context.children.map(_ ? msg.msg))(new ResponseBuilder) { (builder, response) =>
        builder.aggregate(response.asInstanceOf[ResponseFromExecutor])
      }.map(_.toString()) pipeTo sender
  }
}