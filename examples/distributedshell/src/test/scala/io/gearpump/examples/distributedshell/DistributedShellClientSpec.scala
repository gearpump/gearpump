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
package io.gearpump.examples.distributedshell

import akka.testkit.TestProbe
import io.gearpump.cluster.ClientToMaster.ResolveAppId
import io.gearpump.cluster.MasterToClient.ResolveAppIdResult
import io.gearpump.cluster.{TestUtil, MasterHarness}
import DistShellAppMaster.ShellCommand
import io.gearpump.util.{LogUtil, Constants, Util}
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import scala.concurrent.Future

import scala.util.{Try, Success}

class DistributedShellClientSpec extends PropSpec with Matchers with BeforeAndAfter with MasterHarness {

  private val LOG = LogUtil.getLogger(getClass)

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("DistributedShellClient should succeed to submit application with required arguments") {
    val command = "ls /"
    val requiredArgs = Array("-appid", "0", "-command", command)
    val masterReceiver = createMockMaster()

    assert(Try(DistributedShellClient.main(Array.empty[String])).isFailure, "missing required arguments, print usage")


    Future {DistributedShellClient.main(masterConfig, requiredArgs)}

    masterReceiver.expectMsg(PROCESS_BOOT_TIME, ResolveAppId(0))
    val mockAppMaster = TestProbe()(getActorSystem)
    masterReceiver.reply(ResolveAppIdResult(Success(mockAppMaster.ref)))
    LOG.info(s"Reply back ResolveAppIdResult, current actorRef: ${mockAppMaster.ref.path.toString}")
    mockAppMaster.expectMsg(PROCESS_BOOT_TIME, ShellCommand(command))
    mockAppMaster.reply("result")
  }
}
