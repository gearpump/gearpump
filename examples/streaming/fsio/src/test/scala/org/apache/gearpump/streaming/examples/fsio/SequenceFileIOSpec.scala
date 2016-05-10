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

package org.apache.gearpump.streaming.examples.fsio

import scala.concurrent.Future
import scala.util.{Success, Try}

import com.typesafe.config.Config
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}

import org.apache.gearpump.cluster.ClientToMaster.SubmitApplication
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}

class SequenceFileIOSpec
  extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterAll with MasterHarness {

  override def beforeAll {
    startActorSystem()
  }

  override def afterAll {
    shutdownActorSystem()
  }

  override def config: Config = TestUtil.DEFAULT_CONFIG

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
        (requiredArgs, optionalArgs)
      )
    }
    val masterReceiver = createMockMaster()
    forAll(validArgs) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs

      Future {
        SequenceFileIO.main(masterConfig, args)
      }
      masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
      masterReceiver.reply(SubmitApplicationResult(Success(0)))
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
