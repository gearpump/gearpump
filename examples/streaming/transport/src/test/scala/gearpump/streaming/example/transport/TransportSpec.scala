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
package gearpump.streaming.example.transport

import gearpump.streaming.examples.transport.Transport
import gearpump.cluster.ClientToMaster.SubmitApplication
import gearpump.cluster.MasterToClient.SubmitApplicationResult
import gearpump.cluster.{TestUtil, MasterHarness}
import gearpump.util.Util
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.util.Success

class TransportSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {
  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("Transport should succeed to submit application with required arguments"){
    val requiredArgs = Array.empty[String]
    val optionalArgs = Array(
      "-source", "1",
      "-inspector", "1",
      "-vehicle", "100",
      "-citysize", "10",
      "-threshold", "60")

    val args = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs, optionalArgs.take(0)),
        (requiredArgs, optionalArgs.take(2)),
        (requiredArgs, optionalArgs.take(4)),
        (requiredArgs, optionalArgs.take(6)),
        (requiredArgs, optionalArgs.take(8)),
        (requiredArgs, optionalArgs)
      )
    }
    val masterReceiver = createMockMaster()
    forAll(args) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs

      val process = Util.startProcess(getMasterListOption(), getContextClassPath,
        getMainClassName(Transport), args)

      try {

        masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
        masterReceiver.reply(SubmitApplicationResult(Success(0)))
      } finally {
        process.destroy()
      }
    }
  }

}
