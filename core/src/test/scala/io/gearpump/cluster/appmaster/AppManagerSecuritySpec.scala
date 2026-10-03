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

import org.apache.pekko.actor.{ActorRef, Props, Status}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{AppDescription, ApplicationStatus, MasterHarness, TestUtil, UserConfig}
import io.gearpump.cluster.AppMasterToMaster._
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.master.AppManager
import io.gearpump.cluster.master.AppManager._
import io.gearpump.cluster.master.InMemoryKVService._
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.security.{ApplicationControl, ControlCapability, ControlRequest, GetApplicationControl, KvReply}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class AppManagerSecuritySpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach
    with MasterHarness {
  override def config: Config = TestUtil.DEFAULT_CONFIG
  private var kv: TestProbe = _
  private var client: TestProbe = _
  private var launches: TestProbe = _
  private var manager: ActorRef = _
  private var first: String = _
  private var second: String = _
  private def admin(message: Any) = ControlCapability.wrap(config, message)
  private def response(message: Any) = KvReply(
    ControlCapability.token(config, ControlCapability.AdminKey), message)
  override def beforeEach(): Unit = {
    startActorSystem()
    kv = TestProbe()(getActorSystem)
    client = TestProbe()(getActorSystem)
    launches = TestProbe()(getActorSystem)
    first = ControlCapability.random()
    second = ControlCapability.random()
    manager = getActorSystem.actorOf(Props(new AppManager(kv.ref,
      new DummyAppMasterLauncherFactory(launches))))
    assert(kv.expectMsgType[ControlRequest].message == GetKV("master_group", MASTER_STATE))
    val registry = Map(1 -> ApplicationRuntimeInfo(1, "one", appMaster = client.ref,
      config = ControlCapability.runtimeConfig(config, 1, first), status = ApplicationStatus.PENDING),
      2 -> ApplicationRuntimeInfo(2, "two", appMaster = client.ref,
        config = ControlCapability.runtimeConfig(config, 2, second), status = ApplicationStatus.PENDING))
    kv.reply(response(GetKVSuccess(MASTER_STATE, MasterState(2, registry))))
  }
  override def afterEach(): Unit = shutdownActorSystem()
  it should "deny raw lifecycle, submission and state messages before touching storage" in {
    val app = AppDescription("bad", TestUtil.dummyApp.appMaster, UserConfig.empty)
    Seq(SubmitApplication(app, None, "admin"), RestartApplication(1), ShutdownApplication(1),
      RegisterAppMaster(1, client.ref, WorkerInfo(WorkerId(0, 0), client.ref)),
      ApplicationStatusChanged(1, ApplicationStatus.SUCCEEDED, 0),
      SaveAppData(1, "key", "bad"), GetAppData(1, "key")).foreach { message =>
      client.send(manager, message)
      assert(client.expectMsgType[Status.Failure].cause.isInstanceOf[SecurityException])
    }
    kv.expectNoMessage(100.millis)
    launches.expectNoMessage(100.millis)
  }
  it should "deny cross-application scope and forged credentials" in {
    Seq(ShutdownApplication(2), RegisterAppMaster(2, client.ref,
      WorkerInfo(WorkerId(0, 0), client.ref)),
      ApplicationStatusChanged(2, ApplicationStatus.SUCCEEDED, 0),
      SaveAppData(2, "key", "bad"), GetAppData(2, "key")).foreach { message =>
      client.send(manager, ControlRequest(first, Some(1), message))
      client.expectMsgType[Status.Failure]
    }
    client.send(manager, ControlRequest(ControlCapability.random(), Some(1), GetAppData(1, "key")))
    client.expectMsgType[Status.Failure]
    client.send(manager, ControlRequest(first, None, GetApplicationControl(2)))
    client.expectMsgType[Status.Failure]
    kv.expectNoMessage(100.millis)
  }
  it should "derive application data namespaces and protect recovery keys" in {
    client.send(manager, ControlRequest(first, Some(1), SaveAppData(1, "key", "value")))
    val put = kv.expectMsgType[ControlRequest].message.asInstanceOf[PutKV]
    assert(put == PutKV("app-data:1", "key", "value"))
    kv.reply(response(PutKVSuccess))
    client.expectMsg(AppDataSaved)
    client.send(manager, ControlRequest(first, Some(1), GetAppData(1, "key")))
    assert(kv.expectMsgType[ControlRequest].message == GetKV("app-data:1", "key"))
    kv.reply(response(GetKVSuccess("key", "value")))
    client.expectMsg(GetAppDataResult("key", "value"))
    Seq(SaveAppData(1, APP_METADATA, "bad"), GetAppData(1, MASTER_STATE)).foreach { message =>
      client.send(manager, ControlRequest(first, Some(1), message))
      client.expectMsgType[Status.Failure]
    }
    kv.expectNoMessage(100.millis)
  }
  it should "return application authority only to administrators" in {
    client.send(manager, admin(GetApplicationControl(1)))
    assert(client.expectMsgType[ApplicationControl].capability == first)
    client.send(manager, ControlRequest(first, Some(1), GetApplicationControl(1)))
    client.expectMsgType[Status.Failure]
  }
  it should "rotate capability on recovery and reject the previous runtime epoch" in {
    val app = TestUtil.dummyApp.copy(clusterConfig = ControlCapability.runtimeConfig(config, 1, first))
    client.send(manager, admin(RecoverApplication(ApplicationMetaData(1, 0, app, None, "spoofed"))))
    val metadata = kv.expectMsgType[ControlRequest].message.asInstanceOf[PutKV]
      .value.asInstanceOf[ApplicationMetaData]
    val fresh = ControlCapability.token(metadata.appDesc.clusterConfig, ControlCapability.AppKey)
    assert(fresh != first && ControlCapability.valid(fresh))
    kv.expectMsgType[ControlRequest]
    launches.expectMsg(LauncherStarted(1))
    client.send(manager, ControlRequest(first, Some(1), GetAppData(1, "key")))
    client.expectMsgType[Status.Failure]
    client.send(manager, ControlRequest(fresh, Some(1), GetAppData(1, "key")))
    assert(kv.expectMsgType[ControlRequest].message == GetKV("app-data:1", "key"))
    kv.reply(response(GetKVSuccess("key", "good")))
    client.expectMsg(GetAppDataResult("key", "good"))
  }
  it should "strip submitted administrator authority and bind the configured audit identity" in {
    client.send(manager, admin(SubmitApplication(TestUtil.dummyApp, None, "spoofed-admin")))
    val metadata = kv.expectMsgType[ControlRequest].message.asInstanceOf[PutKV]
      .value.asInstanceOf[ApplicationMetaData]
    assert(metadata.username == config.getString("gearpump.security.control-user"))
    assert(metadata.appDesc.clusterConfig.getString(ControlCapability.AdminKey).isEmpty)
    assert(ControlCapability.valid(metadata.appDesc.clusterConfig.getString(ControlCapability.AppKey)))
    assert(metadata.appDesc.clusterConfig.getInt(ControlCapability.AppId) == 3)
  }
}
