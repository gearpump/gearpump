/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming

import akka.actor.{ActorRef, Props}
import akka.testkit.TestActorRef
import io.gearpump.cluster.{AppDescription, AppMasterContext, MiniCluster, UserConfig}
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.appmaster.AppMaster
import io.gearpump.util.Graph

object StreamingTestUtil {
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
