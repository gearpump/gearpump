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

package io.gearpump.cluster.master

import akka.actor._
import akka.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{MasterHarness, TestUtil}
import io.gearpump.cluster.AppMasterToMaster.RequestResource
import io.gearpump.cluster.AppMasterToWorker.{LaunchExecutor, ShutdownExecutor}
import io.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import io.gearpump.cluster.MasterToClient.SubmitApplicationResult
import io.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.util.ActorSystemBooter._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scala.util.Success

class AppMasterLauncherSpec extends FlatSpec with Matchers
  with BeforeAndAfterEach with MasterHarness {

  override def config: Config = TestUtil.DEFAULT_CONFIG

  val appId = 1
  val executorId = 2
  var master: TestProbe = null
  var client: TestProbe = null
  var worker: TestProbe = null
  var watcher: TestProbe = null
  var appMasterLauncher: ActorRef = null

  override def beforeEach(): Unit = {
    startActorSystem()
    master = createMockMaster()
    client = TestProbe()(getActorSystem)
    worker = TestProbe()(getActorSystem)
    watcher = TestProbe()(getActorSystem)
    appMasterLauncher = getActorSystem.actorOf(AppMasterLauncher.props(appId, executorId,
      TestUtil.dummyApp, None, "username", master.ref, Some(client.ref)))
    watcher watch appMasterLauncher
    master.expectMsg(RequestResource(appId, ResourceRequest(Resource(1), WorkerId.unspecified)))
    val resource = ResourceAllocated(
      Array(ResourceAllocation(Resource(1), worker.ref, WorkerId(0, 0L))))
    master.reply(resource)
    worker.expectMsgType[LaunchExecutor]
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  "AppMasterLauncher" should "launch appmaster correctly" in {
    worker.reply(RegisterActorSystem("systempath"))
    worker.expectMsgType[ActorSystemRegistered]

    worker.expectMsgType[CreateActor]
    worker.reply(ActorCreated(master.ref, "appmaster"))

    client.expectMsg(SubmitApplicationResult(Success(appId)))
    watcher.expectTerminated(appMasterLauncher)
  }

  "AppMasterLauncher" should "reallocate resource if executor launch rejected" in {
    worker.reply(ExecutorLaunchRejected(""))
    master.expectMsg(RequestResource(appId, ResourceRequest(Resource(1), WorkerId.unspecified)))

    val resource = ResourceAllocated(
      Array(ResourceAllocation(Resource(1), worker.ref, WorkerId(0, 0L))))
    master.reply(resource)
    worker.expectMsgType[LaunchExecutor]

    worker.reply(RegisterActorSystem("systempath"))
    worker.expectMsgType[ActorSystemRegistered]

    worker.expectMsgType[CreateActor]
    worker.reply(CreateActorFailed("", new Exception))
    worker.expectMsgType[ShutdownExecutor]
    assert(client.receiveN(1).head.asInstanceOf[SubmitApplicationResult].appId.isFailure)
    watcher.expectTerminated(appMasterLauncher)
  }
}
