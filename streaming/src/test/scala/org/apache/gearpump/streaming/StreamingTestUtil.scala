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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.dispatch.Dispatcher
import akka.testkit.{TestProbe, TestActorRef}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.AppMasterToMaster.{RegisterAppMaster, SaveAppData}
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.cluster.master.AppMasterRuntimeInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppMasterContext, UserConfig}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.StartClock
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.Express
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

object StreamingTestUtil {
  private var executorId = 0
  val testUserName = "testuser"

  def startAppMaster(miniCluster: MiniCluster, appId: Int): TestActorRef[AppMaster] = {

    implicit val actorSystem = miniCluster.system
    val masterConf = AppMasterContext(appId, testUserName, Resource(1),  None,miniCluster.mockMaster,AppMasterRuntimeInfo(appId, appName = appId.toString))

    val props = Props(new AppMaster(masterConf, AppDescription("test", UserConfig.empty, Graph.empty)))
    val appMaster = miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
    val registerAppMaster = RegisterAppMaster(appMaster, masterConf.registerData)
    miniCluster.mockMaster.tell(registerAppMaster, appMaster)

    appMaster
  }
}