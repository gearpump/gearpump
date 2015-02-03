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

package org.apache.gearpump.streaming.appmaster

import akka.actor._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterDataDetailRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster._
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.storage.InMemoryAppStoreOnMaster
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util._
import org.slf4j.Logger

class AppMaster(appContext : AppMasterContext, app : Application)  extends ApplicationMaster {
  import app.userConfig
  import appContext.{appId, masterProxy, username}

  private var currentExecutorId = 0
  implicit val actorSystem = context.system

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  LOG.info(s"AppMaster[$appId] is launched by $username $app xxxxxxxxxxxxxxxxx")

  private val (clockService, taskManager, executorManager) = {
    val dag = DAG(userConfig.getValue[Graph[TaskDescription, Partitioner]](AppDescription.DAG).get)
    val clockService : ActorRef = {
      val store = new InMemoryAppStoreOnMaster(appId, masterProxy)
      context.actorOf(Props(new ClockService(dag, store)))
    }
    val executorManager = context.actorOf(ExecutorManager.props(userConfig, appContext))
    val taskManager = context.actorOf(Props(new TaskManager(appContext, dag, clockService, executorManager, self)))
    (clockService, taskManager, executorManager)
  }

  override def receive : Receive =
    clockServiceHandler orElse
      executorMessageHandler orElse
      taskMessageHandler orElse
      recover orElse
      appMasterInfoService orElse
      ActorUtil.defaultMsgHandler(self)

  def clockServiceHandler: Receive = {
    case clock: UpdateClock =>
      clockService forward clock
    case GetLatestMinClock =>
      clockService forward GetLatestMinClock
  }

  def executorMessageHandler: Receive = {
    case register: RegisterExecutor =>
      executorManager forward register
  }

  def taskMessageHandler: Receive = {
    case register: RegisterTask =>
      taskManager forward register
  }

  def appMasterInfoService: Receive = {
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      LOG.info(s"***AppMaster got AppMasterDataDetailRequest for $appId ***")
      sender ! AppMasterDataDetail(appId, app)
  }

  def recover: Receive = {
    case AllocateResourceTimeOut =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
    case ReplayFromTimestampWindowTrailingEdge =>
      taskManager forward ReplayFromTimestampWindowTrailingEdge
  }
}

object AppMaster {
  case object AllocateResourceTimeOut
}