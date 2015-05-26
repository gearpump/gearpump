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
import org.apache.gearpump.cluster.MasterToAppMaster.{MessageLoss, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig}
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.executor.Executor.{TaskStopped, TaskArgumentStore, TaskArgument, RestartTasks, TaskLocationReady}
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

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")
  LOG.info(s"Executor actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  appMaster ! RegisterExecutor(self, executorId, resource, worker)
  context.watch(appMaster)

  private var tasks = Map.empty[TaskId, ActorRef]

  val express = Express(context.system)

  def receive : Receive = appMasterMsgHandler

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
        LOG.info("We got MessageLossException from task, replaying application...")
        LOG.info(s"sending to appMaster ${appMaster.path.toString}")

        appMaster ! MessageLoss(executorId)
        Restart
      case _: RestartException => Restart
    }

  private def launchTask(taskId: TaskId, argument: TaskArgument): ActorRef = {
    launchTasks(List(taskId), argument).values.head
  }

  private def launchTasks(taskIds: List[TaskId], argument: TaskArgument): Map[TaskId, ActorRef] = {
    import argument.{processorDescription, subscribers}

    val taskConf = userConf.withConfig(processorDescription.taskConf)

    val taskContext = TaskContextData(executorId,
      appId, "appName", appMaster,
      processorDescription.parallelism,
      processorDescription.life, subscribers)

    val taskClass = TaskUtil.loadClass(processorDescription.taskClass)
    val taskActorClass = classOf[TaskActor]

    var tasks = Map.empty[TaskId, ActorRef]
    taskIds.foreach { taskId =>
      val task = new TaskWrapper(taskId, taskClass, taskContext, taskConf)
      val taskActor = context.actorOf(Props(taskActorClass, taskId, taskContext, userConf, task).
        withDispatcher(Constants.GEARPUMP_TASK_DISPATCHER), ActorPathUtil.taskActorName(taskId))
      context.watch(taskActor)
      tasks += taskId -> taskActor
    }
    tasks
  }

  private val taskArgumentStore = new TaskArgumentStore()

  def appMasterMsgHandler : Receive = terminationWatch orElse {
    case LaunchTasks(taskIds, dagVersion, processorDescription, subscribers: List[Subscriber]) => {
      LOG.info(s"Launching Task $taskIds for app: ${appId}")
      val taskArgument = TaskArgument(dagVersion, processorDescription, subscribers)
      taskIds.foreach(taskArgumentStore.add(_, taskArgument))
      tasks ++= launchTasks(taskIds, taskArgument)
    }

    case ChangeTasks(taskIds, dagVersion, life, subscribers) =>
      LOG.info(s"Change Tasks $taskIds for app: ${appId}, verion: $life, $dagVersion, $subscribers")
      taskIds.foreach { taskId =>
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val processorDescription = taskArgument.processorDescription.copy(life = life)
          taskArgumentStore.add(taskId, TaskArgument(dagVersion, processorDescription, subscribers))
        }

        val taskActor = tasks.get(taskId)
        taskActor.foreach(_ forward ChangeTask(dagVersion, life, subscribers))
      }

    case TaskLocations(locations) =>
      LOG.info(s"TaskLocations Ready...")
      val result = locations.flatMap { kv =>
        val (host, taskIdList) = kv
        taskIdList.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.startClients(locations.keySet).map { _ =>
        express.remoteAddressMap.send(result)
        express.remoteAddressMap.future().map(result => context.children.foreach(_ ! TaskLocationReady))
      }
    case RestartTasks(dagVersion) =>
      LOG.info(s"Executor received restart tasks")
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      context.children.foreach(context.stop(_))
      context.become(restartingTask(dagVersion, remain = tasks.keys.size, restarted = Map.empty[TaskId, ActorRef]))
  }

  def restartingTask(dagVersion: Int, remain: Int, restarted: Map[TaskId, ActorRef]): Receive = terminationWatch orElse {
    case TaskStopped(actor) =>
      for ((taskId, _) <- (tasks.find(_._2 == actor))) {
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val task = launchTask(taskId, taskArgument)
          val newRestarted = restarted + (taskId -> task)
          val newRemain = remain - 1
          if (newRemain == 0) {
            tasks ++= newRestarted
            context.become(appMasterMsgHandler)
          } else {
            context.become(restartingTask(dagVersion, newRemain, newRestarted))
          }
        }
      }
  }

  val terminationWatch: Receive = {
    case Terminated(actor) => {
      if (actor.compareTo(appMaster) == 0) {

        LOG.info(s"AppMaster ${appMaster.path.toString} is terminated, shutting down current executor $appId, $executorId")
        context.stop(self)
      } else {
        self ! TaskStopped(actor)
      }
    }
  }
}

object Executor {
  case class RestartTasks(dagVersion: Int)

  case object TaskLocationReady

  case class TaskArgument(dagVersion: Int, processorDescription: ProcessorDescription, subscribers: List[Subscriber])

  class TaskArgumentStore {

    private var store = Map.empty[TaskId, List[TaskArgument]]

    def add(taskId: TaskId, task: TaskArgument): Unit = {
      val list = store.getOrElse(taskId, List.empty[TaskArgument])
      store += taskId -> (task :: list)
    }

    def get(dagVersion: Int, taskId: TaskId): Option[TaskArgument] = {
      store.get(taskId).flatMap { list =>
        list.find { arg =>
          arg.dagVersion <= dagVersion
        }
      }
    }

    /**
     * when the new DAG is successfully deployed, then we should remove old TaskArgument.
     */
    def removeObsoleteVersion: Unit = {
      store = store.map{ kv =>
        val (k, list) = kv
        (k, list.take(1))
      }
    }
  }

  case class TaskStopped(task: ActorRef)
}