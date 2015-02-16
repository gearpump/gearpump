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
package org.apache.gearpump.examples.distributedshell

import akka.testkit.TestProbe
import org.apache.gearpump.cluster.ClientToMaster.ResolveAppId
import org.apache.gearpump.cluster.MasterToClient.ResolveAppIdResult
import org.apache.gearpump.cluster.{TestUtil, MasterHarness}
import org.apache.gearpump.examples.distributedshell.DistShellAppMaster.ShellCommand
import org.apache.gearpump.util.Util
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import scala.util.{Try, Success}

class DistributedShellClientSpec extends PropSpec with Matchers with BeforeAndAfter with MasterHarness {

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("DistributedShellClient should succeed to submit application with required arguments") {
    val command = "ls"
    val arguments = "/"
    val requiredArgs = Array("-master", s"$getHost:$getPort", "-appid", "0", "-command", command, "-args", arguments)
    val masterReceiver = createMockMaster()

    assert(Try(DistributedShellClient.main(Array.empty[String])).isFailure, "missing required arguments, print usage")

    val process = Util.startProcess(Array.empty[String], getContextClassPath,
        getMainClassName(DistributedShellClient), requiredArgs)
    masterReceiver.expectMsg(PROCESS_BOOT_TIME, ResolveAppId(0))
    val mockAppMaster = TestProbe()(getActorSystem)
    masterReceiver.reply(ResolveAppIdResult(Success(mockAppMaster.ref)))
    mockAppMaster.expectMsg(PROCESS_BOOT_TIME, ShellCommand(command, arguments))
    mockAppMaster.reply("result")
    process.destroy()
  }
}
