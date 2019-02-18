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

package io.gearpump.cluster.appmaster

import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{MasterHarness, TestUtil, _}
import io.gearpump.cluster.AppMasterToMaster.{AppDataSaved, RegisterAppMaster, _}
import io.gearpump.cluster.ClientToMaster.{ResolveAppId, ShutdownApplication, SubmitApplication}
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterRegistered, AppMastersData, AppMastersDataRequest, _}
import io.gearpump.cluster.MasterToClient.{ResolveAppIdResult, ShutdownApplicationResult, SubmitApplicationResult}
import io.gearpump.cluster.master.{AppManager, AppMasterLauncherFactory}
import io.gearpump.cluster.master.AppManager._
import io.gearpump.cluster.master.InMemoryKVService.{GetKV, GetKVSuccess, PutKV, PutKVSuccess}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.util.LogUtil
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scala.util.Success

class AppManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  var kvService: TestProbe = null
  var haService: TestProbe = null
  var appLauncher: TestProbe = null
  var appManager: ActorRef = null
  private val LOG = LogUtil.getLogger(getClass)

  override def config: Config = TestUtil.DEFAULT_CONFIG

  override def beforeEach(): Unit = {
    startActorSystem()
    kvService = TestProbe()(getActorSystem)
    appLauncher = TestProbe()(getActorSystem)

    appManager = getActorSystem.actorOf(Props(new AppManager(kvService.ref,
      new DummyAppMasterLauncherFactory(appLauncher))))
    kvService.expectMsgType[GetKV]
    kvService.reply(GetKVSuccess(MASTER_STATE, MasterState(0, Map.empty)))
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  "AppManager" should "handle AppMaster message correctly" in {
    val app = TestUtil.dummyApp
    val submit = SubmitApplication(app, None, "username")
    val client = TestProbe()(getActorSystem)

    client.send(appManager, submit)

    val appMaster = TestProbe()(getActorSystem)
    val appId = 1

    kvService.expectMsgType[PutKV]
    kvService.expectMsgType[PutKV]
    appLauncher.expectMsg(LauncherStarted(appId))
    val register = RegisterAppMaster(appId, appMaster.ref, WorkerInfo(WorkerId(1, 0), null))
    appMaster.send(appManager, register)
    appMaster.expectMsgType[AppMasterRegistered]

    val active = ApplicationStatusChanged(appId, ApplicationStatus.ACTIVE, 0, null)
    appMaster.send(appManager, active)
    appMaster.expectMsgType[AppMasterActivated]
  }

  "DataStoreService" should "support Put and Get" in {
    val appMaster = TestProbe()(getActorSystem)
    appMaster.send(appManager, SaveAppData(0, "key", 1))
    kvService.expectMsgType[PutKV]
    kvService.reply(PutKVSuccess)
    appMaster.expectMsg(AppDataSaved)

    appMaster.send(appManager, GetAppData(0, "key"))
    kvService.expectMsgType[GetKV]
    kvService.reply(GetKVSuccess("key", 1))
    appMaster.expectMsg(GetAppDataResult("key", 1))
  }

  "AppManager" should "support application submission and shutdown" in {
    testClientSubmission(withRecover = false)
  }

  "AppManager" should "support application submission and recover if appmaster dies" in {
    LOG.info("=================testing recover==============")
    testClientSubmission(withRecover = true)
  }

  "AppManager" should "handle client message correctly" in {
    val mockClient = TestProbe()(getActorSystem)
    mockClient.send(appManager, ShutdownApplication(1))
    assert(mockClient.receiveN(1).head.asInstanceOf[ShutdownApplicationResult].appId.isFailure)

    mockClient.send(appManager, ResolveAppId(1))
    assert(mockClient.receiveN(1).head.asInstanceOf[ResolveAppIdResult].appMaster.isFailure)

    mockClient.send(appManager, AppMasterDataRequest(1))
    mockClient.expectMsg(AppMasterData(ApplicationStatus.NONEXIST))
  }

  "AppManager" should "reject the application submission if the app name already existed" in {
    val app = TestUtil.dummyApp
    val submit = SubmitApplication(app, None, "username")
    val client = TestProbe()(getActorSystem)
    val appMaster = TestProbe()(getActorSystem)
    val worker = TestProbe()(getActorSystem)
    val appId = 1

    client.send(appManager, submit)

    kvService.expectMsgType[PutKV]
    kvService.expectMsgType[PutKV]
    appLauncher.expectMsg(LauncherStarted(appId))
    val register = RegisterAppMaster(appId, appMaster.ref, WorkerInfo(WorkerId(1, 0), worker.ref))
    appMaster.send(appManager, register)
    appMaster.expectMsgType[AppMasterRegistered]

    client.send(appManager, submit)
    assert(client.receiveN(1).head.asInstanceOf[SubmitApplicationResult].appId.isFailure)
  }

  def testClientSubmission(withRecover: Boolean): Unit = {
    val app = TestUtil.dummyApp
    val submit = SubmitApplication(app, None, "username")
    val client = TestProbe()(getActorSystem)
    val appMaster = TestProbe()(getActorSystem)
    val worker = TestProbe()(getActorSystem)
    val appId = 1

    client.send(appManager, submit)

    kvService.expectMsgType[PutKV]
    kvService.expectMsgType[PutKV]
    appLauncher.expectMsg(LauncherStarted(appId))
    val register = RegisterAppMaster(appId, appMaster.ref, WorkerInfo(WorkerId(1, 0), worker.ref))
    appMaster.send(appManager, register)
    kvService.expectMsgType[PutKV]
    appMaster.expectMsgType[AppMasterRegistered]

    client.send(appManager, ResolveAppId(appId))
    client.expectMsg(ResolveAppIdResult(Success(appMaster.ref)))

    client.send(appManager, AppMastersDataRequest)
    client.expectMsgType[AppMastersData]

    client.send(appManager, AppMasterDataRequest(appId, false))
    client.expectMsgType[AppMasterData]

    if (!withRecover) {
      client.send(appManager, ShutdownApplication(appId))
      client.expectMsg(ShutdownApplicationResult(Success(appId)))
    } else {
      // Do recovery
      getActorSystem.stop(appMaster.ref)
      kvService.expectMsgType[GetKV]
      val appState = ApplicationMetaData(appId, 1, app, None, "username")
      kvService.reply(GetKVSuccess(APP_METADATA, appState))
      appLauncher.expectMsg(LauncherStarted(appId))
    }
  }
}

class DummyAppMasterLauncherFactory(test: TestProbe) extends AppMasterLauncherFactory {
  override def props(appId: Int, executorId: Int, app: AppDescription, jar: Option[AppJar],
      username: String, master: ActorRef, client: Option[ActorRef]): Props = {
    Props(new DummyAppMasterLauncher(test, appId))
  }
}

class DummyAppMasterLauncher(test: TestProbe, appId: Int) extends Actor {
  test.ref ! LauncherStarted(appId)
  
  override def receive: Receive = {
    case any: Any => test.ref forward any
  }
}

case class LauncherStarted(appId: Int)
