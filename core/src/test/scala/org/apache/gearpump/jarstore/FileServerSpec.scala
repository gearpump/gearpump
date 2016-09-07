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

package org.apache.gearpump.jarstore

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigValueFactory, ConfigValue}
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.google.common.io.Files
import org.apache.gearpump.jarstore.local.LocalJarStore
import org.apache.gearpump.util.{FileUtils, LogUtil}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.apache.gearpump.jarstore.FileServer._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FileServerSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val timeout = akka.util.Timeout(25, TimeUnit.SECONDS)
  val host = "localhost"
  private val LOG = LogUtil.getLogger(getClass)

  var system: ActorSystem = null

  override def afterAll {
    if (null != system) {
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }

  override def beforeAll {
    system = ActorSystem("FileServerSpec", TestUtil.DEFAULT_CONFIG)
  }

  private def save(client: Client, data: Array[Byte]): FilePath = {
    val file = File.createTempFile("fileserverspec", "test")
    FileUtils.writeByteArrayToFile(file, data)
    val future = client.upload(file)
    import scala.concurrent.duration._
    val path = Await.result(future, 30.seconds)
    file.delete()
    path
  }

  private def get(client: Client, remote: FilePath): Array[Byte] = {
    val file = File.createTempFile("fileserverspec", "test")
    val future = client.download(remote, file)
    import scala.concurrent.duration._
    val data = Await.result(future, 10.seconds)

    val bytes = FileUtils.readFileToByteArray(file)
    file.delete()
    bytes
  }

  "The file server" should {
    "serve the data previously stored" in {

      val rootDir = Files.createTempDir()
      val localJarStore: JarStore = new LocalJarStore
      val conf = TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
        ConfigValueFactory.fromAnyRef(rootDir.getAbsolutePath))
      localJarStore.init(conf)

      val server = new FileServer(system, host, 0, localJarStore)
      val port = Await.result(server.start, Duration(25, TimeUnit.SECONDS))

      LOG.info("start test web server on port " + port)

      val sizes = List(1, 100, 1000000, 50000000)
      val client = new Client(system, host, port.port)

      sizes.foreach { size =>
        val bytes = randomBytes(size)
        val url = s"http://$host:${port.port}/$size"
        val remote = save(client, bytes)
        val fetchedBytes = get(client, remote)
        assert(fetchedBytes sameElements bytes, s"fetch data is coruppted, $url, $rootDir")
      }
      server.stop
      rootDir.delete()
    }
  }

  "The file server" should {
    "handle missed file" in {

      val rootDir = Files.createTempDir()
      val localJarStore: JarStore = new LocalJarStore
      val conf = TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
        ConfigValueFactory.fromAnyRef(rootDir.getAbsolutePath))
      localJarStore.init(conf)

      val server = new FileServer(system, host, 0, localJarStore)
      val port = Await.result(server.start, Duration(25, TimeUnit.SECONDS))

      val client = new Client(system, host, port.port)
      val fetchedBytes = get(client, FilePath("noexist"))
      assert(fetchedBytes.length == 0)
      rootDir.delete()
    }
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new java.util.Random().nextBytes(bytes)
    bytes
  }
}