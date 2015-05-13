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
package gearpump.cluster.main

import java.util.concurrent.TimeUnit

import akka.testkit.TestProbe
import gearpump.cluster.ClientToMaster.ShutdownApplication
import gearpump.cluster.MasterToAppMaster._
import gearpump.cluster.MasterToClient.{ReplayApplicationResult, ShutdownApplicationResult}
import gearpump.cluster.MasterToWorker.WorkerRegistered
import gearpump.cluster.WorkerToMaster.RegisterNewWorker
import gearpump.cluster.master.{MasterProxy, AppMasterRuntimeInfo}
import gearpump.cluster.{MasterHarness, TestUtil}
import gearpump.transport.HostPort
import gearpump.util.{LogUtil, Constants, Util}
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.util.{Success, Try}
import gearpump.util.Constants._

class MainSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  private val LOG = LogUtil.getLogger(getClass)

  override def config = TestUtil.DEFAULT_CONFIG

  override def beforeEach() = {
    startActorSystem()
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "Worker" should "register worker address to master when started." in {

    val masterReceiver = createMockMaster()

    val tempTestConf = convertTestConf(getHost, getPort)


    val worker = Util.startProcess(Array(s"-D$GEARPUMP_CUSTOM_CONFIG_FILE=${tempTestConf.toString}") ++ getMasterListOption(),
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Worker),
      Array.empty)


    try {
      masterReceiver.expectMsg(PROCESS_BOOT_TIME, RegisterNewWorker)

      tempTestConf.delete()
    } finally {
      worker.destroy()
    }
  }

  "Master" should "accept worker RegisterNewWorker when started" in {
    val worker = TestProbe()(getActorSystem)

    val port = Util.findFreePort.get

    val masterConfig =  Array(s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=127.0.0.1:$port",
      s"-D${Constants.GEARPUMP_HOSTNAME}=127.0.0.1")

    val masterProcess = Util.startProcess(masterConfig,
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Master),
      Array("-ip", "127.0.0.1", "-port", port.toString))

    //wait for master process to be started

    try {

      val masterProxy = getActorSystem.actorOf(MasterProxy.props(List(HostPort("127.0.0.1", port))), "mainSpec")

      worker.send(masterProxy, RegisterNewWorker)
      worker.expectMsgType[WorkerRegistered](PROCESS_BOOT_TIME)
    } finally {
      masterProcess.destroy()
    }
  }

  "Info" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val info = Util.startProcess(getMasterListOption(),
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Info),
      Array.empty)

    try {

      masterReceiver.expectMsg(PROCESS_BOOT_TIME, AppMastersDataRequest)
      masterReceiver.reply(AppMastersData(List(AppMasterData(AppMasterActive, 0, "appName"))))

    } finally {
      info.destroy()
    }
  }

  "Kill" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val kill = Util.startProcess(getMasterListOption(),
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Kill),
      Array("-appid", "0"))


    try {

      masterReceiver.expectMsg(PROCESS_BOOT_TIME, ShutdownApplication(0))
      masterReceiver.reply(ShutdownApplicationResult(Success(0)))

    } finally {
      kill.destroy()
    }
  }

  "Replay" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val replay = Util.startProcess(getMasterListOption(),
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Replay),
      Array("-appid", "0"))

    try {

      masterReceiver.expectMsgType[ReplayFromTimestampWindowTrailingEdge](PROCESS_BOOT_TIME)
      masterReceiver.reply(ReplayApplicationResult(Success(0)))
    } finally {
      replay.destroy()
    }
  }

  "Local" should "be started without exception" in {
    val port = Util.findFreePort.get
    val options = Array(s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=$getHost:$port",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$getHost")

    val local = Util.startProcess(options,
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Local),
      Array.empty)

    def retry(times: Int)(fn: => Boolean): Boolean = {

      LOG.info(s"Local Test: Checking whether local port is available, remain times $times ..")

      val result = fn
      if (result || times <= 0) {
        result
      } else  {
        Thread.sleep(1000)
        retry(times - 1)(fn)
      }
    }

    try {
      assert(retry(10)(isPortUsed("127.0.0.1", port)), "local is not started successfully, as port is not used " + port)
    } finally {
      local.destroy()
    }
  }

  "Gear" should "support app|info|kill|shell|replay" in {

    val commands = Array("app", "info", "kill", "shell", "replay")

    assert(Try(Gear.main(Array.empty)).isSuccess, "print help, no throw")

    for (command <- commands) {
      //Temporarily disable this test
      //assert(Try(Gear.main(Array(command))).isSuccess, "print help, no throw, command: " + command)
      assert(Try(Gear.main(Array("-noexist"))).isFailure, "pass unknown option, throw, command: " + command)
    }

    assert(Try(Gear.main(Array("unknownCommand"))).isFailure, "unknown command, throw ")

    val tryThis = Try(Gear.main(Array("unknownCommand", "-noexist")))
    assert(tryThis.isFailure, "unknown command, throw")
  }

  "Shell" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val shell = Util.startProcess(getMasterListOption(),
      getContextClassPath,
      getMainClassName(gearpump.cluster.main.Shell),
      Array("-master", s"$getHost:$getPort"))

    try {

      val scalaHome = Option(System.getenv("SCALA_HOME")).map { _ =>
        // Only test this when SCALA_HOME env is set
        masterReceiver.expectMsg(Duration(15, TimeUnit.SECONDS), AppMastersDataRequest)
      }
    } finally {
      shell.destroy()
    }
  }
}

