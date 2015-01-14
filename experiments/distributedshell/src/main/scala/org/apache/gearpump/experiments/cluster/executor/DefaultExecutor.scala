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
package org.apache.gearpump.experiments.cluster.executor

import akka.actor.{Actor, Terminated, Props}
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.{MsgToTask, LaunchTask}
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.{ResponsesFromTasks, RegisterExecutor}
import org.apache.gearpump.util.{ReferenceEqual, LogUtil, Constants}
import org.slf4j.Logger

import akka.pattern.{ask, pipe}
import scala.concurrent._

case class TaskDescription(taskClass: String, parallelism : Int) extends ReferenceEqual
case class TaskLaunchData(taskClass: String, config: UserConfig)

class DefaultExecutor(executorContext: ExecutorContext, userConf : UserConfig) extends Actor {
  import context.dispatcher
  import executorContext._

  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId,
    app = appId)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")

  appMaster ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)

  override def receive: Receive = appMasterMsgHandler orElse terminationWatch

  def appMasterMsgHandler : Receive = {
    case LaunchTask(taskContext, taskClass, conf) => {
      val taskDispatcher = context.system.settings.config.getString(Constants.GEARPUMP_TASK_DISPATCHER)
      context.actorOf(Props(taskClass, taskContext, userConf.withConfig(conf)).withDispatcher(taskDispatcher))
    }
    case MsgToTask(msg) =>
      Future.fold(context.children.map(_ ? msg))(ResponsesFromTasks(executorId, List.empty[Any])) { (list, response) =>
        ResponsesFromTasks(executorId, response :: list.msgs)
      } pipeTo sender
  }

  def terminationWatch : Receive = {
    case Terminated(actor) => {
      if (actor.compareTo(appMaster) == 0) {
        LOG.info(s"AppMaster ${appMaster.path.toString} is terminated, shutting down current executor $appId, $executorId")
        context.stop(self)
      }
    }
  }
}
