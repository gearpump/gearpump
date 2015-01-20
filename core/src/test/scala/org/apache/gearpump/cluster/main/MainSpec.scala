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

import java.util.concurrent.TimeUnit

import akka.actor.Props
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData, AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ShutdownApplicationResult}
import org.apache.gearpump.cluster.MasterToWorker.WorkerRegistered
import org.apache.gearpump.cluster.WorkerToMaster.RegisterNewWorker
import org.apache.gearpump.cluster.master.{MasterProxy, AppMasterRuntimeInfo}
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Util
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.util.{Success, Try}
import org.apache.gearpump.util.Constants._

class MainSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  override def config = TestUtil.MASTER_CONFIG

  override def beforeEach() = {
    startActorSystem()
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "Worker" should "register worker address to master when started." in {

    val masterReceiver = createMockMaster()

    val tempTestConf = convertTestConf(getHost, getPort)

    val worker = Util.startProcess(Array(s"-D$GEARPUMP_CUSTOM_CONFIG_FILE=${tempTestConf.toString}"),
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Worker),
      Array.empty[String])

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, RegisterNewWorker)

    tempTestConf.delete()
    worker.destroy()
  }

  "Master" should "accept worker RegisterNewWorker when started" in {
    val worker = TestProbe()(getActorSystem)

    val port = Util.findFreePort.get
    val tempTestConf = convertTestConf("127.0.0.1", port)

    val masterProcess = Util.startProcess(Array(s"-D$GEARPUMP_CUSTOM_CONFIG_FILE=${tempTestConf.toString}"),
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Master),
      Array("-ip", "127.0.0.1", "-port", port.toString))

    //wait for master process to be started

    val masterProxy = getActorSystem.actorOf(Props(classOf[MasterProxy], List(HostPort("127.0.0.1", port))), "proxy")

    worker.send(masterProxy, RegisterNewWorker)
    worker.expectMsgType[WorkerRegistered](PROCESS_BOOT_TIME)

    tempTestConf.delete()
    masterProcess.destroy()
  }

  "Info" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val info = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Info),
      Array("-master", s"$getHost:$getPort"))

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, AppMastersDataRequest)
    masterReceiver.reply(AppMastersData(List(AppMasterData(0, null))))

    info.destroy()
  }

  "Kill" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val kill = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Kill),
      Array("-master", s"$getHost:$getPort", "-appid", "0"))

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, ShutdownApplication(0))
    masterReceiver.reply(ShutdownApplicationResult(Success(0)))

    kill.destroy()
  }

  "Replay" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val replay = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Replay),
      Array("-master", s"$getHost:$getPort", "-appid", "0"))

    masterReceiver.expectMsgType[ReplayFromTimestampWindowTrailingEdge](PROCESS_BOOT_TIME)
    masterReceiver.reply(ReplayApplicationResult(Success(0)))

    replay.destroy()
  }

  "Local" should "be started without exception" in {

    val port = Util.findFreePort.get

    val local = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Local),
      Array("-ip", "127.0.0.1", "-port", port.toString))

    def retry(seconds: Int)(fn: => Boolean): Boolean = {
      val result = fn
      if (result) {
        result
      } else {
        Thread.sleep(1000)
        retry(seconds - 1)(fn)
      }
    }

    assert(retry(10)(isPortUsed("127.0.0.1", port)), "local is not started successfully, as port is not used " + port)
    local.destroy()
  }

  "Gear" should "support app|info|kill|shell|replay" in {

    val commands = Array("app", "info", "kill", "shell", "replay")

    assert(Try(Gear.main(Array.empty)).isSuccess, "print help, no throw")

    for (command <- commands) {
      assert(Try(Gear.main(Array(command))).isSuccess, "print help, no throw, command: " + command)
      assert(Try(Gear.main(Array("-noexist"))).isFailure, "pass unknown option, throw, command: " + command)
    }

    assert(Try(Gear.main(Array("unknownCommand"))).isFailure, "unknown command, throw ")
    assert(Try(Gear.main(Array("unknownCommand", "-noexist"))).isFailure, "unknown command, throw")
  }

  "Shell" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val shell = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Shell),
      Array("-master", s"$getHost:$getPort"))


    val scalaHome = Option(System.getenv("SCALA_HOME")).map { _ =>
      // Only test this when SCALA_HOME env is set
      masterReceiver.expectMsg(Duration(15, TimeUnit.SECONDS), AppMastersDataRequest)
    }

    shell.destroy()
  }
}

