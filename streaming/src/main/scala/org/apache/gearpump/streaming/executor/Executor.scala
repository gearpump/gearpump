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

package org.apache.gearpump.streaming.executor

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig}
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.executor.Executor.{RestartExecutor, TaskLocationReady}
import org.apache.gearpump.streaming.task.TaskActor.RestartTask
import org.apache.gearpump.streaming.task.{TaskId, TaskLocations, TaskWrapper}
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.transport.{Express, HostPort}
import org.apache.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.language.postfixOps


class Executor(executorContext: ExecutorContext, userConf : UserConfig)  extends Actor {

  import context.dispatcher
  import executorContext._

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId,
    app = appId)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")

  appMaster ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)

  val express = Express(context.system)

  def receive : Receive = appMasterMsgHandler orElse terminationWatch

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
        LOG.info("We got MessageLossException from task, replaying application...")
        appMaster ! ReplayFromTimestampWindowTrailingEdge
        Restart
      case _: RestartException => Restart
    }

  def appMasterMsgHandler : Receive = {
    case LaunchTask(taskId, taskContext, taskClass, taskActorClass, taskConfig) => {
      LOG.info(s"Launching Task $taskId for app: ${appId}, $taskClass")

      val taskConf = userConf.withConfig(taskConfig)

      val task = new TaskWrapper(taskClass, taskContext, taskConf)
      val taskDispatcher = context.system.settings.config.getString(Constants.GEARPUMP_TASK_DISPATCHER)
      val taskActor = context.actorOf(Props(taskActorClass, taskContext, userConf, task).withDispatcher(taskDispatcher), ActorPathUtil.taskActorName(taskId))
    }
    case TaskLocations(locations) =>
      val result = locations.flatMap { kv =>
        val (host, taskIdList) = kv
        taskIdList.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.startClients(locations.keySet).map { _ =>
        express.remoteAddressMap.send(result)
        express.remoteAddressMap.future().map(result => context.children.foreach(_ ! TaskLocationReady))
      }
    case RestartExecutor =>
      LOG.info(s"Executor received restart tasks")
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      context.children.foreach(_ ! RestartTask)
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

object Executor {
  case object RestartExecutor

  case object TaskLocationReady
}