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
import org.apache.gearpump.streaming.executor.Executor.{RestartTasks, TaskLocationReady}
import org.apache.gearpump.streaming.task.TaskActor.RestartTask
import org.apache.gearpump.streaming.task.{TaskActor, TaskUtil, Subscriber, TaskContextData, TaskId, TaskLocations, TaskWrapper}
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.transport.{Express, HostPort}
import org.apache.gearpump.util.{ActorUtil, Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.language.postfixOps


class Executor(executorContext: ExecutorContext, userConf : UserConfig)  extends Actor {

  import context.dispatcher
  import executorContext._

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId,
    app = appId)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")
  LOG.info(s"Executor actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  appMaster ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)

  private var tasks = Map.empty[TaskId, ActorRef]

  val express = Express(context.system)

  def receive : Receive = appMasterMsgHandler orElse terminationWatch

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
        LOG.info("We got MessageLossException from task, replaying application...")
        LOG.info(s"sending to appMaster ${appMaster.path.toString}")

        appMaster ! ReplayFromTimestampWindowTrailingEdge(appId)
        Restart
      case _: RestartException => Restart
    }

  def appMasterMsgHandler : Receive = {
    case LaunchTasks(taskIds, processorDescription, subscribers: List[Subscriber]) => {
      LOG.info(s"Launching Task $taskIds for app: ${appId}")

      val taskConf = userConf.withConfig(processorDescription.taskConf)

      val taskContext = TaskContextData(executorId, appId, "appName", appMaster, processorDescription.parallelism, subscribers)
      val taskClass = TaskUtil.loadClass(processorDescription.taskClass)
      val taskActorClass = classOf[TaskActor]

      taskIds.foreach { taskId =>
        val task = new TaskWrapper(taskId, taskClass, taskContext, taskConf)
        val taskActor = context.actorOf(Props(taskActorClass, taskId, taskContext, userConf, task).
          withDispatcher(Constants.GEARPUMP_TASK_DISPATCHER), ActorPathUtil.taskActorName(taskId))
        tasks += taskId -> taskActor
      }
    }

    case ChangeTasks(taskIds, life, subscribers) =>
      //TODO: Chang existing tasks.
      taskIds.foreach { taskId =>
        val taskActor = tasks.get(taskId)
        taskActor.foreach(_ forward ChangeTask(life, subscribers))
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
    case RestartTasks =>
      //TODO: Support versioned DAG. Stop then start, instead of using exception.
      //In this case, we allow user to change the construction parameters.
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
  case object RestartTasks

  case object TaskLocationReady
}