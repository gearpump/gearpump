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

package org.apache.gearpump.streaming.state.user.example.processor

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.system.impl.PersistentStateConfig
import org.apache.gearpump.streaming.task.{CheckpointTime, StartTime}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class CountProcessorSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("CountProcessor should update state") {

    val taskContext = MockUtil.mockTaskContext

    implicit val system = ActorSystem("test")

    val longGen = Gen.chooseNum[Long](1, 1000)
    forAll(longGen, longGen) {
      (data: Long, num: Long) =>
        val stateConfig = new PersistentStateConfig(ConfigFactory.parseString(
          s"""state {
            checkpoint {
              interval = $num # milliseconds
              store.factory = org.apache.gearpump.streaming.state.system.impl.InMemoryCheckpointStoreFactory
            }
          }""".stripMargin
        ))



        val conf = UserConfig.empty
          .withValue(PersistentStateConfig.NAME, stateConfig)
        val count = new CountProcessor(taskContext, conf)
        count.onStart(StartTime(0L))
        for (i <- 1L to num) {
          when(taskContext.upstreamMinClock).thenReturn(0L)
          count.onNext(Message("" + data, 0L))
        }

        count.state.get shouldBe Some(num)

        when(taskContext.upstreamMinClock).thenReturn(num)

        val appMaster = TestProbe()(system)
        when(taskContext.appMaster).thenReturn(appMaster.ref)
        count.onNext(Message("" + data, num))
        appMaster.expectMsg(CheckpointTime(taskContext.taskId, num))

        count.state.get shouldBe Some(num + 1)

    }
    system.shutdown()
  }
}
