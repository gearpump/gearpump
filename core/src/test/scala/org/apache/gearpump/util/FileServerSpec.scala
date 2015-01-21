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

package org.apache.gearpump.util

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import com.google.common.io.Files
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.util.FileServer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Success

class FileServerSpec  extends WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val timeout = akka.util.Timeout(10, TimeUnit.SECONDS)
  val host = "localhost"

  var system: ActorSystem = null

  override def afterAll {
    if (null != system) system.shutdown()
  }

  override def beforeAll {
    system = ActorSystem("FileServerSpec", TestUtil.DEFAULT_CONFIG)
  }

  "The file server" should {
    "server the data previously stored" in {

      val rootDir = Files.createTempDir()

      val server = system.actorOf(Props(classOf[FileServer], rootDir, host, 0))
      val port = Await.result((server ? GetPort).asInstanceOf[Future[Port]], Duration(15, TimeUnit.SECONDS))

      val sizes = List(1, 100, 1000000, 50000000)

      val client = FileServer.newClient
      sizes.foreach { size =>
        val bytes = randomBytes(size)
        val url = s"http://$host:${port.port}/$size"
        val saveReturnCode = client.save(url, bytes)
        assert(saveReturnCode == Success(200), s"save data fails, $url, $rootDir")

        val fetchedBytes = client.get(url)
        assert(fetchedBytes.isSuccess, s"fetch data fails, $url, $rootDir")
        assert(fetchedBytes.get sameElements bytes, s"fetch data is coruppted, $url, $rootDir")
      }
      rootDir.delete()

      system.stop(server)
    }
  }

  "The file server" should {
    "handle missed file" in {

      val rootDir = Files.createTempDir()

      val server = system.actorOf(Props(classOf[FileServer], rootDir, host, 0))
      val port = Await.result((server ? GetPort).asInstanceOf[Future[Port]], Duration(15, TimeUnit.SECONDS))

      val url = s"http://$host:${port.port}/notexist"

      val client = FileServer.newClient
      val fetchedBytes = client.get(url)
      assert(fetchedBytes.isFailure, s"fetch data fails, $url, $rootDir")
      rootDir.delete()
    }
  }

  private def randomBytes(size : Int) : Array[Byte] = {
    val bytes = new Array[Byte](size)
    (new java.util.Random()).nextBytes(bytes)
    bytes
  }
}