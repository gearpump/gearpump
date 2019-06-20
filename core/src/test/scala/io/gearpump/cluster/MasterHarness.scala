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

package io.gearpump.cluster

import akka.actor.{Actor, ActorSystem, Address, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigValueFactory}
import io.gearpump.cluster.MasterHarness._
import io.gearpump.cluster.client.ClientContext
import io.gearpump.util.{ActorUtil, FileUtils, LogUtil}
import io.gearpump.util.Constants._
import java.io.File
import java.net.{InetSocketAddress, Socket, SocketTimeoutException, UnknownHostException, URLClassLoader}
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

trait MasterHarness {

  implicit val pool = MasterHarness.cachedPool

  private var system: ActorSystem = null
  private var systemAddress: Address = null
  private var host: String = null
  private var port: Int = 0
  private val masterProperties = new Properties()
  val PROCESS_BOOT_TIME = Duration(25, TimeUnit.SECONDS)

  def getActorSystem: ActorSystem = system
  def getHost: String = host
  def getPort: Int = port

  protected def config: Config

  def startActorSystem(): Unit = {
    val systemConfig = config
    system = ActorSystem(MASTER, systemConfig)
    systemAddress = ActorUtil.getSystemAddress(system)
    host = systemAddress.host.get
    port = systemAddress.port.get

    masterProperties.put(s"${GEARPUMP_CLUSTER_MASTERS}.0", s"$getHost:$getPort")
    masterProperties.put(s"${GEARPUMP_HOSTNAME}", s"$getHost")

    LOG.info(s"Actor system is started, $host, $port")
    // Make sure there will be no EmbeddedCluster created, otherwise mock master won't work
    ClientContext.setRemote()
  }

  def shutdownActorSystem(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    LOG.info(s"Actor system is stopped, $host, $port")
  }

  def convertTestConf(host: String, port: Int): File = {
    val test = ConfigFactory.parseResourcesAnySyntax("test.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val newConf = test.withValue(GEARPUMP_CLUSTER_MASTERS,
      ConfigValueFactory.fromAnyRef(Array(s"$host:$port").toList.asJava))

    val confFile = File.createTempFile("main", ".conf")
    val serialized = newConf.root().render()
    FileUtils.write(confFile, serialized)
    confFile
  }

  def createMockMaster(): TestProbe = {
    val masterReceiver = TestProbe()(system)
    system.actorOf(Props(classOf[MockMaster], masterReceiver), MASTER)
    masterReceiver
  }

  def isPortUsed(host: String, port: Int): Boolean = {

    var isPortUsed = true
    val socket = new Socket()
    try {
      socket.setReuseAddress(true)
      socket.connect(new InetSocketAddress(host, port), 1000)
      socket.isConnected
    } catch {
      case _: SocketTimeoutException =>
        isPortUsed = false
      case _: UnknownHostException =>
        isPortUsed = false
      case _: Throwable =>
        // For other case, we think the port has been occupied.
        isPortUsed = true
    } finally {
      socket.close()
    }
    isPortUsed
  }

  def getContextClassPath: Array[String] = {
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
  def getMainClassName(mainObj: Any): String = {
    mainObj.getClass.getName.dropRight(1)
  }

  def getMasterListOption(): Array[String] = {
    masterProperties.asScala.toList.map { kv =>
      s"-D${kv._1}=${kv._2}"
    }.toArray
  }

  def masterConfig: Config = {
    ConfigFactory.parseProperties(masterProperties).withFallback(system.settings.config)
  }
}

object MasterHarness {
  private val LOG = LogUtil.getLogger(getClass)

  val cachedPool = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  class MockMaster(receiver: TestProbe) extends Actor {
    def receive: Receive = {
      case msg => {
        receiver.ref forward msg
      }
    }
  }
}
