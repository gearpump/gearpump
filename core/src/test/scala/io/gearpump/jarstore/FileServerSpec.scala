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

package io.gearpump.jarstore

import org.apache.pekko.actor.ActorSystem
import com.google.common.io.Files
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.jarstore.FileServer._
import io.gearpump.jarstore.local.LocalJarStore
import io.gearpump.util.{FileUtils, LogUtil}
import java.io.File
import java.util.concurrent.TimeUnit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FileServerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val timeout: org.apache.pekko.util.Timeout =
    org.apache.pekko.util.Timeout(25, TimeUnit.SECONDS)
  val host = "localhost"
  private val LOG = LogUtil.getLogger(getClass)

  var system: ActorSystem = _

  override def afterAll(): Unit = {
    if (null != system) {
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }

  override def beforeAll(): Unit = {
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
    Await.result(future, 10.seconds)

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
        val url = s"https://$host:${port.port}/$size"
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
      intercept[IllegalArgumentException] { get(client, FilePath("noexist")) }
      server.stop
      rootDir.delete()
    }
  }

  "The artifact client" should {
    "reject altered bytes without replacing the destination and remove partial downloads" in {
      val rootDir = java.nio.file.Files.createTempDirectory("artifact-tamper-")
      val store = new LocalJarStore
      store.init(TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
        ConfigValueFactory.fromAnyRef(rootDir.toString)))
      val server = new FileServer(system, host, 0, store)
      val port = Await.result(server.start, Duration(25, TimeUnit.SECONDS))
      val client = new Client(system, host, port.port)
      val remote = save(client, Array[Byte](1, 2, 3))
      java.nio.file.Files.write(rootDir.resolve(remote.path), Array[Byte](3, 2, 1))
      val destinationDir = java.nio.file.Files.createTempDirectory("artifact-destination-")
      val destination = destinationDir.resolve("application.jar")
      java.nio.file.Files.write(destination, Array[Byte](9))
      intercept[java.io.IOException] {
        Await.result(client.download(remote, destination.toFile),
          Duration(10, TimeUnit.SECONDS))
      }
      assert(java.nio.file.Files.readAllBytes(destination).sameElements(Array[Byte](9)))
      val files = java.nio.file.Files.list(destinationDir)
      try assert(files.count() == 1) finally files.close()
      Await.result(client.delete(remote), Duration(10, TimeUnit.SECONDS))
      assert(!java.nio.file.Files.exists(rootDir.resolve(remote.path)))
      server.stop
    }
    "reject a TLS client without a certificate before accepting HTTP traffic" in {
      val root = java.nio.file.Files.createTempDirectory("artifact-mtls-")
      val store = new LocalJarStore
      store.init(TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
        ConfigValueFactory.fromAnyRef(root.toString)))
      val server = new FileServer(system, host, 0, store)
      val port = Await.result(server.start, Duration(25, TimeUnit.SECONDS))
      val trust = java.security.KeyStore.getInstance("PKCS12")
      val in = getClass.getResourceAsStream("/security-test-only.p12")
      try trust.load(in, "gearpump-test-only".toCharArray) finally in.close()
      val factory = javax.net.ssl.TrustManagerFactory.getInstance(
        javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm)
      factory.init(trust)
      val anonymous = javax.net.ssl.SSLContext.getInstance("TLS")
      anonymous.init(Array.empty[javax.net.ssl.KeyManager], factory.getTrustManagers,
        new java.security.SecureRandom())
      val response = org.apache.pekko.http.scaladsl.Http(system).singleRequest(
        org.apache.pekko.http.scaladsl.model.HttpRequest(
          uri = s"https://localhost:${port.port}/download?file=test.jar"),
        connectionContext = org.apache.pekko.http.scaladsl.ConnectionContext.httpsClient(anonymous))
      val error = intercept[Exception] { Await.result(response, Duration(10, TimeUnit.SECONDS)) }
      val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toList
      assert(causes.exists(_.isInstanceOf[javax.net.ssl.SSLException]))
      assert(store.listFiles().isEmpty)
      server.stop
    }
    "refuse cleartext endpoint discovery" in {
      intercept[IllegalArgumentException] { new Client(system, "http://localhost:1/") }
    }
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new java.util.Random().nextBytes(bytes)
    bytes
  }
}
