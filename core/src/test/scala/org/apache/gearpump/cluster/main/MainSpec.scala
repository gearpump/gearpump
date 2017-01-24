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

package org.apache.gearpump.cluster.main

import java.util.Properties

import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.ClientToMaster.{ResolveAppId, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge, _}
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ResolveAppIdResult, ShutdownApplicationResult}
import org.apache.gearpump.cluster.MasterToWorker.WorkerRegistered
import org.apache.gearpump.cluster.WorkerToMaster.RegisterNewWorker
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.{ApplicationStatus, MasterHarness, TestUtil}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Constants, LogUtil, Util}
import org.scalatest._

import scala.concurrent.Future
import scala.util.{Success, Try}

class MainSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  private val LOG = LogUtil.getLogger(getClass)

  override def config: Config = TestUtil.DEFAULT_CONFIG

  override def beforeEach(): Unit = {
    startActorSystem()
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  "Worker" should "register worker address to master when started." in {

    val masterReceiver = createMockMaster()

    val tempTestConf = convertTestConf(getHost, getPort)

    val options = Array(
      s"-D$GEARPUMP_CUSTOM_CONFIG_FILE=${tempTestConf.toString}",
      s"-D${PREFER_IPV4}=true"
    ) ++ getMasterListOption()

    val worker = Util.startProcess(options,
      getContextClassPath,
      getMainClassName(Worker),
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

    val host = "127.0.0.1"
    val port = Util.findFreePort().get

    val properties = new Properties()
    properties.put(s"${GEARPUMP_CLUSTER_MASTERS}.0", s"$host:$port")
    properties.put(s"${GEARPUMP_HOSTNAME}", s"$host")
    val masterConfig = ConfigFactory.parseProperties(properties)
      .withFallback(TestUtil.MASTER_CONFIG)
    Future {
      Master.main(masterConfig, Array("-ip", "127.0.0.1", "-port", port.toString))
    }

    val masterProxy = getActorSystem.actorOf(
      MasterProxy.props(List(HostPort("127.0.0.1", port))), "mainSpec")

    worker.send(masterProxy, RegisterNewWorker)
    worker.expectMsgType[WorkerRegistered](PROCESS_BOOT_TIME)
  }

  "Info" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    Future {
      org.apache.gearpump.cluster.main.Info.main(masterConfig, Array.empty)
    }

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, AppMastersDataRequest)
    masterReceiver.reply(AppMastersData(List(AppMasterData(
      ApplicationStatus.ACTIVE, 0, "appName"))))
  }

  "Kill" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    Future {
      Kill.main(masterConfig, Array("-appid", "0"))
    }

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, ShutdownApplication(0))
    masterReceiver.reply(ShutdownApplicationResult(Success(0)))
  }

  "Replay" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    Future {
      Replay.main(masterConfig, Array("-appid", "0"))
    }

    masterReceiver.expectMsgType[ResolveAppId](PROCESS_BOOT_TIME)
    masterReceiver.reply(ResolveAppIdResult(Success(masterReceiver.ref)))
    masterReceiver.expectMsgType[ReplayFromTimestampWindowTrailingEdge](PROCESS_BOOT_TIME)
    masterReceiver.reply(ReplayApplicationResult(Success(0)))
  }

  "Local" should "be started without exception" in {
    val port = Util.findFreePort().get
    val options = Array(s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=$getHost:$port",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$getHost",
      s"-D${PREFER_IPV4}=true")

    val local = Util.startProcess(options,
      getContextClassPath,
      getMainClassName(Local),
      Array.empty)

    def retry(times: Int)(fn: => Boolean): Boolean = {

      LOG.info(s"Local Test: Checking whether local port is available, remain times $times ..")

      val result = fn
      if (result || times <= 0) {
        result
      } else {
        Thread.sleep(1000)
        retry(times - 1)(fn)
      }
    }

    try {
      assert(retry(10)(isPortUsed("127.0.0.1", port)),
        "local is not started successfully, as port is not used " + port)
    } finally {
      local.destroy()
    }
  }

  "Gear" should "support app|info|kill|shell|replay" in {

    val commands = Array("app", "info", "kill", "shell", "replay")

    assert(Try(Gear.main(Array.empty)).isSuccess, "print help, no throw")

    for (command <- commands) {
      assert(Try(Gear.main(Array("-noexist"))).isFailure,
        "pass unknown option, throw, command: " + command)
    }

    assert(Try(Gear.main(Array("unknownCommand"))).isFailure, "unknown command, throw ")

    val tryThis = Try(Gear.main(Array("unknownCommand", "-noexist")))
    assert(tryThis.isFailure, "unknown command, throw")
  }
}

