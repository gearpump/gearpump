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

package gearpump.streaming.examples.fsio

import gearpump.cluster.ClientToMaster.{ShutdownApplication, SubmitApplication}
import gearpump.cluster.MasterToClient.{ShutdownApplicationResult, SubmitApplicationResult}
import gearpump.cluster.{MasterHarness, TestUtil}
import gearpump.util.{Constants, Util}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import scala.util.{Success, Try}

class SequenceFileIOSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {
  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("SequenceFileIO should succeed to submit application with required arguments") {
    val requiredArgs = Array(
      "-input", "/tmp/input",
      "-output", "/tmp/output"
    )
    val optionalArgs = Array(
      "-source", "1",
      "-sink", "1"
    )
    val validArgs = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs, optionalArgs.take(0)),
        (requiredArgs, optionalArgs.take(2)),
        (requiredArgs, optionalArgs)
      )
    }
    val masterReceiver = createMockMaster()
    forAll(validArgs) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs
      val process = Util.startProcess(getMasterListOption(), getContextClassPath,
        getMainClassName(SequenceFileIO), args)

      try {

        masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
        masterReceiver.reply(SubmitApplicationResult(Success(0)))
      } finally {
        process.destroy()
      }
    }

    val invalidArgs = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs.take(0), optionalArgs),
        (requiredArgs.take(2), optionalArgs)
      )
    }
    forAll(invalidArgs) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = optionalArgs
      assert(Try(SequenceFileIO.main(args)).isFailure, "missing required arguments, print usage")
    }
  }

}
