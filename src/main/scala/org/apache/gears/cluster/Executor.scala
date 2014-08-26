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

package org.apache.gears.cluster

import akka.actor.{Actor, Props, Terminated}
import org.apache.gears.cluster.AppMasterToExecutor._
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
    context.watch(appMaster)
  }

  def receive : Receive = appMasterMsgHandler orElse terminationWatch

  def appMasterMsgHandler : Receive = {
    case LaunchTask(taskId, config, taskClass) => {
      LOG.info(s"Launching Task $taskId for app: $appId, $taskClass")
      val taskDispatcher = context.system.settings.config.getString("gearpump.task-dispatcher")
      val task = context.actorOf(Props(taskClass, config.withTaskId(taskId)).withDispatcher(taskDispatcher), "group_" + taskId.groupId + "_task_" + taskId.index)
    }
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