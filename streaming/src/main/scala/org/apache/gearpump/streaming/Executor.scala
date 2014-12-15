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
import org.apache.gearpump.util.{Constants, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

case object TaskLocationReady

class Executor(config : Configs)  extends Actor {
  import context.dispatcher
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Executor])

  val appMaster = config.appMaster
  val executorId = config.executorId
  val resource = config.resource
  val appId = config.appId
  val workerId = config.workerId
  var startClock : TimeStamp = config.startTime

  context.parent ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)

  val express = Express(context.system)

  def receive : Receive = appMasterMsgHandler orElse terminationWatch

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
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
        val (host, set) = kv
        set.map(taskId => (TaskId.toLong(taskId), host))
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