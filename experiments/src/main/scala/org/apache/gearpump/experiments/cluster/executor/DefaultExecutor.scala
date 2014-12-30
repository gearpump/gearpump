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
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.{MsgToTask, LaunchTask}
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.util.{ReferenceEqual, LogUtil, Constants, Configs}
import org.slf4j.Logger

case class TaskDescription(taskClass: String, parallelism : Int) extends ReferenceEqual
case class TaskLaunchData(taskClass: String, config: Configs)

class DefaultExecutor(config : Configs) extends Actor {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  protected val executorId = config.executorId
  protected val appMaster = config.appMaster
  protected val resource = config.resource
  protected val appId = config.appId
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId,
    app = config.appId)

  protected val workerId = config.workerId
  context.parent ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)

  override def receive: Receive = appMasterMsgHandler orElse terminationWatch

  def appMasterMsgHandler : Receive = {
    case LaunchTask(config, taskClass) => {
      val taskDispatcher = context.system.settings.config.getString(Constants.GEARPUMP_TASK_DISPATCHER)
      context.actorOf(Props(taskClass, config).withDispatcher(taskDispatcher))
    }
    case MsgToTask(msg) =>
      context.children.map(_ ! msg)
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
