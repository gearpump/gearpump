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

import akka.actor.{Props, ActorRef, ActorRefFactory}
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig}
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.executor.TaskLauncher.TaskArgument
import org.apache.gearpump.streaming.task.{Subscriber, TaskWrapper, TaskActor, TaskUtil, TaskContextData, TaskId}
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.util.Constants
import akka.actor.Actor

trait ITaskLauncher {
  def launch(taskIds: List[TaskId], argument: TaskArgument, context: ActorRefFactory): Map[TaskId, ActorRef]
}

class TaskLauncher(
    appId: Int,
    executorId: Int,
    appMaster: ActorRef,
    userConf: UserConfig,
    taskActorClass: Class[_ <: Actor])
  extends ITaskLauncher{
  def launch(taskIds: List[TaskId], argument: TaskArgument, context: ActorRefFactory): Map[TaskId, ActorRef] = {
    import argument.{appName, processorDescription, subscribers}

    val taskConf = userConf.withConfig(processorDescription.taskConf)

    val taskContext = TaskContextData(executorId,
      appId, appName, appMaster,
      processorDescription.parallelism,
      processorDescription.life, subscribers)

    val taskClass = TaskUtil.loadClass(processorDescription.taskClass)

    var tasks = Map.empty[TaskId, ActorRef]
    taskIds.foreach { taskId =>
      val task = new TaskWrapper(taskId, taskClass, taskContext, taskConf)
      val taskActor = context.actorOf(Props(taskActorClass, taskId, taskContext, userConf, task).
        withDispatcher(Constants.GEARPUMP_TASK_DISPATCHER), ActorPathUtil.taskActorName(taskId))
      tasks += taskId -> taskActor
    }
    tasks
  }
}

object TaskLauncher {

  case class TaskArgument(dagVersion: Int, processorDescription: ProcessorDescription, subscribers: List[Subscriber], appName: String)

  def apply(executorContext: ExecutorContext, userConf: UserConfig): TaskLauncher = {
    import executorContext.{appId, appMaster, executorId}
    new TaskLauncher(appId, executorId, appMaster, userConf, classOf[TaskActor])
  }
}