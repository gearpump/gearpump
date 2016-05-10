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

package org.apache.gearpump.experiments.yarn.appmaster

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.experiments.yarn.appmaster.UIServiceSpec.{Info, MockUI}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class UIServiceSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var system: ActorSystem = null

  override def beforeAll(): Unit = {
    system = ActorSystem(getClass.getSimpleName, TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "start UI server correctly" in {
    val probe = TestProbe()
    val masters = List(
      HostPort("127.0.0.1", 3000),
      HostPort("127.0.0.1", 3001),
      HostPort("127.0.0.1", 3002)
    )
    val host = "local"
    val port = 8091

    val ui = system.actorOf(Props(new MockUI(masters, host, port, probe.ref)))

    probe.expectMsgPF() {
      case info: Info => {
        assert(info.masterHost == "127.0.0.1")
        assert(info.masterPort == 3000)
        val conf = ConfigFactory.parseFile(new java.io.File(info.configFile))
        assert(conf.getString(Constants.GEARPUMP_SERVICE_HOST) == host)
        assert(conf.getString(Constants.GEARPUMP_SERVICE_HTTP) == "8091")
        assert(conf.getString(Constants.NETTY_TCP_HOSTNAME) == host)
      }
    }

    system.stop(ui)
  }
}

object UIServiceSpec {

  case class Info(supervisor: String, masterHost: String, masterPort: Int, configFile: String)

  class MockUI(masters: List[HostPort], host: String, port: Int, probe: ActorRef)
    extends UIService(masters, host, port) {

    override def launch(
        supervisor: String, masterHost: String, masterPort: Int, configFile: String): Unit = {
      probe ! Info(supervisor, masterHost, masterPort, configFile)
    }
  }
}
