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

package org.apache.gearpump.streaming.state.example.processor

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.impl.DefaultCheckpointManager
import org.apache.gearpump.streaming.task.StartTime
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration._

class WindowCountProcessorSpec extends PropSpec with PropertyChecks with Matchers {
  property("WindowCountProcessor should update state clock") {

    val taskContext = MockUtil.mockTaskContext

    implicit val system = ActorSystem("test")

    val longGen = Gen.chooseNum[Long](1, 1000)
    forAll(longGen, longGen, longGen) {
      (upstreamMinClock: Long, windowSize: Long, num: Long) =>

        when(taskContext.upstreamMinClock).thenReturn(0L, upstreamMinClock)

        val checkpointInterval = windowSize
        val conf = UserConfig.empty
          .withValue[Duration](WindowCountProcessor.WINDOW_SIZE, Duration(windowSize, MILLISECONDS))
          // in a window application, checkpoint interval is set to window size such that
          .withLong(DefaultCheckpointManager.CHECKPOINT_INTERVAL, checkpointInterval)

        val windowCount = new WindowCountProcessor(taskContext, conf)
        windowCount.onStart(StartTime(0L))
        windowCount.onNext(Message(num + "", num))
        windowCount.onNext(Message(num + "", num + 1))
        if (upstreamMinClock < checkpointInterval) {
          windowCount.stateClock shouldBe Some(0L)
        } else {
          windowCount.stateClock shouldBe Some(checkpointInterval)
        }

        system.shutdown()
    }
  }
}
