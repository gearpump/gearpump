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

package org.apache.gearpump.streaming.examples.state

import scala.concurrent.Future
import scala.util.Success

import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import org.apache.gearpump.cluster.ClientToMaster.SubmitApplication
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}
import org.apache.gearpump.streaming.examples.state.MessageCountApp._

class MessageCountAppSpec
  extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  protected override def config = TestUtil.DEFAULT_CONFIG

  property("MessageCount should succeed to submit application with required arguments") {
    val requiredArgs = Array(
      s"-$SOURCE_TOPIC", "source",
      s"-$SINK_TOPIC", "sink",
      s"-$ZOOKEEPER_CONNECT", "localhost:2181",
      s"-$BROKER_LIST", "localhost:9092",
      s"-$DEFAULT_FS", "hdfs://localhost:9000"
    )
    val optionalArgs = Array(
      s"-$SOURCE_TASK", "2",
      s"-$COUNT_TASK", "2",
      s"-$SINK_TASK", "2"
    )

    val args = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs, optionalArgs.take(0)),
        (requiredArgs, optionalArgs.take(2)),
        (requiredArgs, optionalArgs.take(4)),
        (requiredArgs, optionalArgs)
      )
    }

    val masterReceiver = createMockMaster()
    forAll(args) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs
      Future {
        MessageCountApp.main(masterConfig, args)
      }
      masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
      masterReceiver.reply(SubmitApplicationResult(Success(0)))
    }
  }
}
