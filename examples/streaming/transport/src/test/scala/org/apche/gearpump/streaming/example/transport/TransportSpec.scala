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
package org.apche.gearpump.streaming.examples.transport

import org.apache.gearpump.cluster.ClientToMaster.SubmitApplication
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.{TestUtil, MasterHarness}
import org.apache.gearpump.streaming.examples.transport.Transport
import org.apache.gearpump.util.Util
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.util.Success

class TransportSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterAll with MasterHarness {
  override def beforeAll {
    startActorSystem()
  }

  override def afterAll {
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
