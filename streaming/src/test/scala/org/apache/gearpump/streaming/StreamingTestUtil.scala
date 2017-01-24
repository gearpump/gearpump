/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming

import akka.actor._
import akka.testkit.TestActorRef
import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.appmaster.ApplicationRuntimeInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppDescription, AppMasterContext, MiniCluster, UserConfig}
import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.util.Graph

object StreamingTestUtil {
  private var executorId = 0
  val testUserName = "testuser"

  def startAppMaster(miniCluster: MiniCluster, appId: Int): TestActorRef[AppMaster] = {

    implicit val actorSystem = miniCluster.system
    val masterConf = AppMasterContext(appId, testUserName, Resource(1), null,
      None, miniCluster.mockMaster)

    val app = StreamApplication("test", Graph.empty, UserConfig.empty)
    val appDescription = AppDescription(app.name, app.appMaster.getName, app.userConfig)
    val props = Props(new AppMaster(masterConf, appDescription))
    val appMaster = miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
    val registerAppMaster = RegisterAppMaster(appId, ActorRef.noSender, null)
    miniCluster.mockMaster.tell(registerAppMaster, appMaster)

    appMaster
  }
}