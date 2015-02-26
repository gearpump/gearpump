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
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterDataDetailRequest, AppMasterMetricsRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster._
import org.apache.gearpump.metrics.Metrics.MetricType
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager.GetExecutorPathList
import org.apache.gearpump.streaming.storage.InMemoryAppStoreOnMaster
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.Future

class AppMaster(appContext : AppMasterContext, app : Application)  extends ApplicationMaster {
  import app.userConfig
  import appContext.{appId, masterProxy, username}

  implicit val actorSystem = context.system

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  LOG.info(s"AppMaster[$appId] is launched by $username $app xxxxxxxxxxxxxxxxx")
  private val address = ActorUtil.getFullPath(context.system, self.path)
  private var subscribers = false

  private val (taskManager, executorManager, clockService) = {
    val dag = DAG(userConfig.getValue[Graph[TaskDescription, Partitioner]](AppDescription.DAG).get)

    val executorManager = context.actorOf(ExecutorManager.props(userConfig, appContext))

    val store = new InMemoryAppStoreOnMaster(appId, appContext.masterProxy)
    val clockService = context.actorOf(Props(new ClockService(dag, store)))

    val taskScheduler: TaskScheduler = new TaskSchedulerImpl(appId, context.system.settings.config)
    val taskManager = context.actorOf(Props(new TaskManager(appContext.appId, dag,
      taskScheduler, executorManager, clockService, self, app.name)))
    (taskManager, executorManager, clockService)
  }

  override def receive : Receive =
    taskMessageHandler orElse
      executorMessageHandler orElse
      recover orElse
      appMasterInfoService orElse
      ActorUtil.defaultMsgHandler(self)

  def taskMessageHandler: Receive = {
    case clock: UpdateClock =>
      taskManager forward clock
    case GetLatestMinClock =>
      taskManager forward GetLatestMinClock
    case register: RegisterTask =>
      taskManager forward register
    case ReplayFromTimestampWindowTrailingEdge =>
      taskManager forward ReplayFromTimestampWindowTrailingEdge
    case metrics: MetricType =>
      if(subscribers) {
        LOG.info(s"***AppMaster publishing metrics***")
        actorSystem.eventStream.publish(metrics)
      }
  }

  def executorMessageHandler: Receive = {
    case register: RegisterExecutor =>
      executorManager forward register
  }

  implicit val timeOut = Constants.FUTURE_TIMEOUT

  def appMasterInfoService: Receive = {
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      LOG.info(s"***AppMaster got AppMasterDataDetailRequest for $appId ***")


      val executorsFuture = getExecutorList
      val clockFuture = getMinClock

      val appMasterDataDetail = for {executors <- executorsFuture
        clock <- clockFuture } yield AppMasterDataDetail(appId, app.name, app, address, clock, executors)
      val client = sender()

      appMasterDataDetail.map{appData =>
        client ! appData
      }
    case appMasterMetricsRequest: AppMasterMetricsRequest =>
      val client = sender()
      subscribers = true
      actorSystem.eventStream.subscribe(client, classOf[MetricType])
  }

  def recover: Receive = {
    case AllocateResourceTimeOut =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
  }

  import akka.pattern.ask
  implicit val dispatcher = context.dispatcher
  private def getMinClock: Future[TimeStamp] = {
    (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map(_.clock)
  }

  private def getExecutorList: Future[List[String]] = {
    (executorManager ? GetExecutorPathList).asInstanceOf[Future[List[ActorPath]]].map(list => list.map(_.toString))
  }
}

object AppMaster {
  case object AllocateResourceTimeOut
}