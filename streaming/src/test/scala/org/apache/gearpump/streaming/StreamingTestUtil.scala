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

import akka.actor.Props
import akka.testkit.TestActorRef
import org.apache.gearpump.cluster.AppMasterInfo
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.{Configs, Graph}

object StreamingTestUtil {
  private var executorId = 0

  def startAppMaster(miniCluster: MiniCluster, appId: Int,
                     app: AppDescription = AppDescription("test", Configs.empty, Graph.empty)): TestActorRef[AppMaster] = {
    val config = Configs.empty.withAppDescription(app).withExecutorId(executorId).withAppId(appId).
      withMasterProxy(miniCluster.mockMaster).withResource(Resource.empty)
      .withAppMasterRegisterData(AppMasterInfo(miniCluster.worker))
    val props = Props(classOf[AppMaster], config)
    executorId += 1
    miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
  }
}
