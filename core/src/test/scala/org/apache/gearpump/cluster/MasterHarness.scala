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

package org.apache.gearpump.cluster

import java.io.File
import java.net.{InetSocketAddress, ServerSocket, URLClassLoader}
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Address, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.MasterHarness.MockMaster
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{LogUtil, ActorUtil, Util}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.sys.process.Process
import scala.util.Try

trait MasterHarness {
  private val LOG = LogUtil.getLogger(getClass)

  private var system: ActorSystem = null
  private var systemAddress: Address = null
  private var host: String = null
  private var port: Int = 0
  val PROCESS_BOOT_TIME = Duration(10, TimeUnit.SECONDS)

  def getActorSystem: ActorSystem = system
  def getHost: String = host
  def getPort: Int = port

  def config : Config

  def startActorSystem(): Unit = {
    val systemConfig = config
    system = ActorSystem(MASTER, systemConfig)
    systemAddress = ActorUtil.getSystemAddress(system)
    host = systemAddress.host.get
    port = systemAddress.port.get
    LOG.info(s"Actor system is started, $host, $port")
  }

  def shutdownActorSystem():Unit = {
    system.shutdown()
    LOG.info(s"Actor system is stopped, $host, $port")
  }

  def startMasterProcess(host: String, port: Int): Process ={
    val tempTestConf = convertTestConf(host, port)
    Util.startProcess(Array(s"-D$GEARPUMP_CUSTOM_CONFIG_FILE=${tempTestConf.toString}"),
      getContextClassPath,
      getMainClassName(org.apache.gearpump.cluster.main.Master),
      Array("-ip", host, "-port", port.toString))
  }

  def convertTestConf(host : String, port : Int) : File = {
    val test = ConfigFactory.parseResourcesAnySyntax("test.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val newConf = test.withValue("gearpump.cluster.masters",
      ConfigValueFactory.fromAnyRef(Array(s"$host:$port").toList.asJava))

    val confFile = File.createTempFile("main", ".conf")
    val serialized = newConf.root().render()
    FileUtils.write(confFile, serialized)
    confFile
  }

  def createMockMaster() : TestProbe = {
    val masterReceiver = TestProbe()(system)
    val master = system.actorOf(Props(classOf[MockMaster], masterReceiver), MASTER)
    masterReceiver
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
  def getMainClassName(mainObj : Any) : String = {
    mainObj.getClass.getName.dropRight(1)
  }
}

object MasterHarness {
  class MockMaster(receiver: TestProbe) extends Actor {
    def receive: Receive = {
      case msg => {
        receiver.ref forward msg
      }
    }
  }
}
