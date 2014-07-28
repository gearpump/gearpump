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

import akka.actor.Actor
import org.apache.gearpump.task.TaskInit
import org.apache.gears.cluster.AppMasterToExecutor._
import org.apache.gears.cluster.ExecutorToAppMaster._
import org.apache.gears.cluster.ExecutorToWorker._
import org.slf4j.{Logger, LoggerFactory}

class Executor(config : Configs)  extends Actor {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[Executor])

  val appMaster = config.appMaster
  val executorId = config.executorId
  val slots = config.slots
  val appId = config.appId

  override def preStart : Unit = {
    context.parent ! RegisterExecutor(appMaster, appId, executorId, slots)
  }

  def receive : Receive = {
    case LaunchTask(taskId, conf, taskDescription, outputs) => {
      LOG.info(s"Launching Task $taskId for app: $appId, $taskDescription, $outputs")
      val task = context.actorOf(taskDescription.task, "stage_" + taskId.stageId + "_task_" + taskId.index)
      sender ! TaskLaunched(taskId, task)

      task ! TaskInit(taskId, appMaster, outputs, conf, taskDescription.partitioner)
    }
  }
}