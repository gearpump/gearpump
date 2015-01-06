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

import java.io.File
import java.net.{InetSocketAddress, ServerSocket, URLClassLoader}
import java.util.concurrent.TimeUnit

import akka.actor.{Address, Actor, ActorSystem, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData, AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ShutdownApplicationResult}
import org.apache.gearpump.cluster.MasterToWorker.WorkerRegistered
import org.apache.gearpump.cluster.WorkerToMaster.RegisterNewWorker
import org.apache.gearpump.cluster.{AppMasterInfo, MasterProxy, TestUtil}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, Util}
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

class MainSpec extends FlatSpec with Matchers with BeforeAndAfterEach {
  var system : ActorSystem = null
  var systemAddress : Address = null
  //actor system host and port
  var host : String = null
  var port : Int = 0

  override def beforeEach() = {
    val systemConfig = TestUtil.DEFAULT_CONFIG
    //We name it as master node must be started by system MASTER
    system = ActorSystem(MASTER, systemConfig)
    systemAddress = ActorUtil.getSystemAddress(system)

    host = systemAddress.host.get
    port = systemAddress.port.get
  }

  override def afterEach() = {
    system.shutdown()
  }

  val PROCESS_BOOT_TIME = Duration(10, TimeUnit.SECONDS)

  "Worker" should "register worker address to master when started." in {

    val masterReceiver = createMockMaster()

    val tempTestConf = convertTestConf(systemAddress.host.get, systemAddress.port.get)

    val worker = Util.startProcess(Array(s"-Dconfig.file=${tempTestConf.toString}"),
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Worker),
      Array.empty[String])

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, RegisterNewWorker)

    tempTestConf.delete()
    worker.destroy()
  }

  "Master" should "accept worker RegisterNewWorker when started" in {
    val worker = TestProbe()(system)

    val port = Util.findFreePort.get
    val tempTestConf = convertTestConf("127.0.0.1", port)

    val masterProcess = Util.startProcess(Array(s"-Dconfig.file=${tempTestConf.toString}"),
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Master),
      Array("-ip", "127.0.0.1", "-port", port.toString))

    //wait for master process to be started

    val masterProxy = system.actorOf(Props(classOf[MasterProxy], List(HostPort("127.0.0.1", port))), "proxy")

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
      Array("-master", s"$host:$port"))

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, AppMastersDataRequest)
    masterReceiver.reply(AppMastersData(List(AppMasterData(0, AppMasterInfo(null)))))

    info.destroy()
  }

    "Kill" should "be started without exception" in {

      val masterReceiver = createMockMaster()

      val kill = Util.startProcess(Array.empty[String],
        getContextClassPath,
        getMainClassName(org.apache.gearpump.cluster.main.Kill),
        Array("-master", s"$host:$port", "-appid", "0"))

      masterReceiver.expectMsg(PROCESS_BOOT_TIME, ShutdownApplication(0))
      masterReceiver.reply(ShutdownApplicationResult(Success(0)))

      kill.destroy()
    }

  "Replay" should "be started without exception" in {

    val masterReceiver = createMockMaster()

    val replay = Util.startProcess(Array.empty[String],
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Replay),
      Array("-master", s"$host:$port", "-appid", "0"))

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

    def retry(seconds: Int)(fn: => Boolean) : Boolean = {
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

    val commands = Array("app", "info",  "kill", "shell", "replay")

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
      Array("-master", s"$host:$port"))


    val scalaHome = Option(System.getenv("SCALA_HOME")).map {_ =>
      // Only test this when SCALA_HOME env is set
      masterReceiver.expectMsg(Duration(15, TimeUnit.SECONDS), AppMastersDataRequest)
    }

    shell.destroy()
  }

  private def createMockMaster() : TestProbe = {
    val masterReceiver = TestProbe()(system)
    val master = system.actorOf(Props(classOf[MainSpec.MockMaster], masterReceiver), MASTER)
    masterReceiver
  }

  private def convertTestConf(host : String, port : Int) : File = {
    val test = ConfigFactory.parseResourcesAnySyntax("test.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val newConf = test.withValue("gearpump.cluster.masters",
      ConfigValueFactory.fromAnyRef(Array(s"$host:$port").toList.asJava))

    val confFile = File.createTempFile("main", ".conf")
    val serialized = newConf.root().render()
    FileUtils.write(confFile, serialized)
    confFile
  }

  def isPortUsed(host : String, port : Int) : Boolean = {
    val takePort = Try {
      val socket = new ServerSocket()
      socket.setReuseAddress(true)
      socket.bind(new InetSocketAddress(host, port))
      socket.close
    }
    takePort.isFailure
  }

  def getContextClassPath : Array[String] = {
    val buffer = new StringBuffer();
    val contextLoader = Thread.currentThread().getContextClassLoader()

    val urlLoader = if (!contextLoader.isInstanceOf[URLClassLoader]) {
      contextLoader.getParent.asInstanceOf[URLClassLoader]
    } else {
      contextLoader.asInstanceOf[URLClassLoader]
    }

    val urls = urlLoader.getURLs()
    val classPath = urls.map { url =>
      new File(url.getPath()).toString
    }
    classPath
  }

  /**
   * Remove trailing $
   */
  private def getMainClassName(mainObj : Any) : String = {
    mainObj.getClass.getName.dropRight(1)
  }
}

object MainSpec {
  class MockMaster(receiver: TestProbe) extends Actor {
    def receive: Receive = {
      case msg => receiver.ref forward msg
    }
  }
}