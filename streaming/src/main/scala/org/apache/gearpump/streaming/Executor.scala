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

package org.apache.gearpump.streaming

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ConfigsHelper._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.task.{TaskId, TaskLocations}
import org.apache.gearpump.transport.{HostPort, Express}
import org.apache.gearpump.util.{LogUtil, Constants, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import org.apache.gearpump.cluster.scheduler.Resource

case object TaskLocationReady

class Executor(executorId : Int,appMaster : ActorRef, resource : Resource, appId : Int,
               workerId: Int, var startClock : TimeStamp)  extends Actor {
  def this(config : Configs) = {
    this(config.executorId, config.appMaster, config.resource,
      config.appId, config.workerId, config.startTime)
  }

  import context.dispatcher

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId,
    app = appId)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")

  context.parent ! RegisterExecutor(self, executorId, resource, workerId)
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
    case LaunchTask(taskId, config, taskClass) => {
      LOG.info(s"Launching Task $taskId for app: $appId, $taskClass")
      val taskDispatcher = context.system.settings.config.getString(Constants.GEARPUMP_TASK_DISPATCHER)
      val task = context.actorOf(Props(taskClass, config.withTaskId(taskId)).withDispatcher(taskDispatcher), "group_" + taskId.groupId + "_task_" + taskId.index)
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
    case r @ RestartTasks(clock) =>
      LOG.info(s"Executor received restart tasks at time: $clock")
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      this.startClock = clock
      context.children.foreach(_ ! r)
    case GetStartClock =>
      LOG.info(s"Executor received GetStartClock, return: $startClock")
      sender ! StartClock(startClock)
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