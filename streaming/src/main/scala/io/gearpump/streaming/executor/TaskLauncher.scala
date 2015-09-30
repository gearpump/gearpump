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


package io.gearpump.streaming.executor

import akka.actor.{Props, ActorRef, ActorRefFactory, Actor}
import io.gearpump.streaming.ProcessorDescription
import io.gearpump.streaming.task._
import io.gearpump.streaming.util.ActorPathUtil
import io.gearpump.cluster.{ExecutorContext, UserConfig}
import io.gearpump.serializer.SerializerPool
import TaskLauncher.TaskArgument
import io.gearpump.util.Constants

trait ITaskLauncher {
  def launch(taskIds: List[TaskId], argument: TaskArgument, context: ActorRefFactory, serializer: SerializerPool): Map[TaskId, ActorRef]
}

class TaskLauncher(
    appId: Int,
    appName: String,
    executorId: Int,
    appMaster: ActorRef,
    userConf: UserConfig,
    taskActorClass: Class[_ <: Actor])
  extends ITaskLauncher{

  override def launch(taskIds: List[TaskId], argument: TaskArgument, context: ActorRefFactory, serializer: SerializerPool): Map[TaskId, ActorRef] = {
    import argument.{processorDescription, subscribers}

    val taskConf = userConf.withConfig(processorDescription.taskConf)

    val taskContext = TaskContextData(executorId,
      appId, appName, appMaster,
      processorDescription.parallelism,
      processorDescription.life, subscribers)

    val taskClass = TaskUtil.loadClass(processorDescription.taskClass)

    var tasks = Map.empty[TaskId, ActorRef]
    taskIds.foreach { taskId =>
      val task = new TaskWrapper(taskId, taskClass, taskContext, taskConf)
      val taskActor = context.actorOf(Props(taskActorClass, taskId, taskContext, userConf, task, serializer).
        withDispatcher(Constants.GEARPUMP_TASK_DISPATCHER), ActorPathUtil.taskActorName(taskId))
      tasks += taskId -> taskActor
    }
    tasks
  }
}

object TaskLauncher {

  case class TaskArgument(dagVersion: Int, processorDescription: ProcessorDescription, subscribers: List[Subscriber])

  def apply(executorContext: ExecutorContext, userConf: UserConfig): TaskLauncher = {
    import executorContext.{appId, appName, appMaster, executorId}
    new TaskLauncher(appId, appName, executorId, appMaster, userConf, classOf[TaskActor])
  }
}