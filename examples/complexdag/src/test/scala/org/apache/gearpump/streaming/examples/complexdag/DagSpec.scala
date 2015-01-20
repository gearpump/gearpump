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

package org.apache.gearpump.streaming.examples.complexdag

import org.apache.gearpump.cluster.ClientToMaster.{ShutdownApplication, SubmitApplication}
import org.apache.gearpump.cluster.{TestUtil, MasterHarness}
import org.apache.gearpump.cluster.MasterToClient.{ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.util.Util
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.util.{Try, Success}

class DagSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config = TestUtil.DEFAULT_CONFIG

  property("Dag should succeed to submit application with required arguments") {
    val requiredArgs = Array("-master", s"$getHost:$getPort")
    val runseconds = Array("-runseconds", "0")

    val masterReceiver = createMockMaster()
    val args = requiredArgs ++ runseconds

    val process = Util.startProcess(Array.empty[String], getContextClassPath,
      getMainClassName(Dag), args)
    masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
    masterReceiver.reply(SubmitApplicationResult(Success(0)))
    masterReceiver.expectMsgType[ShutdownApplication](PROCESS_BOOT_TIME)
    masterReceiver.reply(ShutdownApplicationResult(Success(0)))

    process.destroy()

    val missingargs = runseconds
    assert(Try(Dag.main(missingargs)).isFailure, "missing required arguments, print usage")
  }


}
