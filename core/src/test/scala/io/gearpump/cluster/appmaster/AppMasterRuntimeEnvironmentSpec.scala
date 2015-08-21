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

package io.gearpump.cluster.appmaster

import akka.actor._
import akka.testkit.TestProbe
import io.gearpump.TestProbeUtil._
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster._
import io.gearpump.cluster.appmaster.AppMasterRuntimeEnvironment._
import io.gearpump.cluster.appmaster.AppMasterRuntimeEnvironmentSpec.TestAppMasterEnv
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler.StartExecutorSystems
import io.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus.{MasterConnected, MasterStopped}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AppMasterRuntimeEnvironmentSpec extends FlatSpec with Matchers with BeforeAndAfterAll  {
  implicit var system: ActorSystem = null

  override def beforeAll() = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll() = {
    system.shutdown()
  }

  "MasterWithExecutorSystemProvider" should "forward request StartExecutorSystem to ExecutorSystemProvider" in {

    val client = TestProbe()
    val master = TestProbe()
    val provider = TestProbe()
    val providerProps: Props = provider
    val masterEnhanced = system.actorOf(Props(new MasterWithExecutorSystemProvider(master.ref, providerProps)))

    val start = StartExecutorSystems(null, null)
    client.send(masterEnhanced, start)
    provider.expectMsg(start)

    val anyOtherMessage = "any other message"
    client.send(masterEnhanced, anyOtherMessage)
    master.expectMsg(anyOtherMessage)
    system.stop(masterEnhanced)
  }

  "LazyStartAppMaster" should "forward command to appmaster when app master started" in {

    val appMaster = TestProbe()
    val appMasterProps: Props = appMaster
    val lazyAppMaster = system.actorOf(Props(new LazyStartAppMaster(appId = 0, appMasterProps)))
    val msg = "Some"
    lazyAppMaster ! msg
    lazyAppMaster ! StartAppMaster
    appMaster.expectMsg(msg)

    system.stop(appMaster.ref)
    val client = TestProbe()
    client.watch(lazyAppMaster)
    client.expectTerminated(lazyAppMaster)
  }

  "AppMasterRuntimeEnvironment" should "start appMaster when master is connected" in {
    val TestAppMasterEnv(master, appMaster, masterConnectionKeeper, runtimeEnv) = setupAppMasterRuntimeEnv

    masterConnectionKeeper.send(runtimeEnv, MasterConnected)
    appMaster.expectMsg(StartAppMaster)
  }

  "AppMasterRuntimeEnvironment" should "shutdown itself when master is stopped" in {

    val TestAppMasterEnv(master, appMaster, masterConnectionKeeper, runtimeEnv) = setupAppMasterRuntimeEnv

    masterConnectionKeeper.send(runtimeEnv, MasterStopped)
    val client = TestProbe()
    client.watch(runtimeEnv)
    client.expectTerminated(runtimeEnv)
  }

  "AppMasterRuntimeEnvironment" should "shutdown itself when appMaster is stopped" in {

    val TestAppMasterEnv(master, appMaster, masterConnectionKeeper, runtimeEnv) = setupAppMasterRuntimeEnv

    val client = TestProbe()
    client.watch(runtimeEnv)
    system.stop(appMaster.ref)
    client.expectTerminated(runtimeEnv)
  }

  private def setupAppMasterRuntimeEnv: TestAppMasterEnv = {
    val appContext = AppMasterContext(0, null, null, null,  null, null, null)
    val app = AppDescription("app", "AppMasterClass", null, null)
    val master = TestProbe()
    val masterFactory = (_: AppId, _: MasterActorRef) => toProps(master)
    val appMaster = TestProbe()
    val appMasterFactory = (_: AppMasterContext, _: AppDescription)=> toProps(appMaster)
    val masterConnectionKeeper = TestProbe()
    val masterConnectionKeeperFactory =
      (_: MasterActorRef, _: RegisterAppMaster, _: ListenerActorRef) =>
        toProps(masterConnectionKeeper)

    val runtimeEnv = system.actorOf(
      Props(new AppMasterRuntimeEnvironment(
        appContext, app, List(master.ref.path), masterFactory,
        appMasterFactory, masterConnectionKeeperFactory)))

    TestAppMasterEnv(master, appMaster, masterConnectionKeeper, runtimeEnv)
  }
}

object AppMasterRuntimeEnvironmentSpec {

  case class TestAppMasterEnv(master: TestProbe, appMaster: TestProbe, connectionkeeper: TestProbe, appMasterRuntimeEnv: ActorRef)
}