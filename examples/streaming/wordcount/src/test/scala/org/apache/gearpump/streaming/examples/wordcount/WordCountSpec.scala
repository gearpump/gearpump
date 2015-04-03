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

package org.apache.gearpump.streaming.examples.wordcount

import org.apache.gearpump.cluster.ClientToMaster.{ShutdownApplication, SubmitApplication}
import org.apache.gearpump.cluster.MasterToClient.{ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}
import org.apache.gearpump.util.Util
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import scala.util.{Success, Try}

class WordCountSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("WordCount should succeed to submit application with required arguments") {
    val requiredArgs = Array.empty[String]
    val optionalArgs = Array(
      "-split", "1",
      "-sum", "1")

    val args = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs, optionalArgs.take(0)),
        (requiredArgs, optionalArgs.take(2)),
        (requiredArgs, optionalArgs.take(4)),
        (requiredArgs, optionalArgs.take(6)),
        (requiredArgs, optionalArgs)
      )
    }
    val masterReceiver = createMockMaster()
    forAll(args) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs

      val process = Util.startProcess(getMasterListOption(), getContextClassPath,
        getMainClassName(WordCount), args)
      masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
      masterReceiver.reply(SubmitApplicationResult(Success(0)))

      process.destroy()
    }
  }


}
