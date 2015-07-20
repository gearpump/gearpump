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

package org.apache.gearpump.streaming.examples.state.processor

import akka.actor.ActorSystem
import com.twitter.algebird.AveragedValue
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.system.impl.{InMemoryCheckpointStoreFactory, PersistentStateConfig, WindowConfig}
import org.apache.gearpump.streaming.task.StartTime
import org.apache.gearpump.streaming.transaction.api.CheckpointStoreFactory
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}


class WindowAverageProcessorSpec extends PropSpec with PropertyChecks with Matchers {
  property("WindowAverageProcessor should update state") {

    val taskContext = MockUtil.mockTaskContext

    implicit val system = ActorSystem("test")

    val longGen = Gen.chooseNum[Long](1, 1000)
    forAll(longGen, longGen) {
      (data: Long, num: Long) =>
        val windowSize = num
        val windowStep = num
        val checkpointStoreFactory = new InMemoryCheckpointStoreFactory
        val conf = UserConfig.empty
            .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, num)
            .withValue[CheckpointStoreFactory](PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, checkpointStoreFactory)
            .withValue(WindowConfig.NAME, WindowConfig(windowSize, windowStep))

        val windowAverage = new WindowAverageProcessor(taskContext, conf)

        windowAverage.onStart(StartTime(0L))
        for (i <- 1L to num) {
          when(taskContext.upstreamMinClock).thenReturn(0L)
          windowAverage.onNext(Message("" + data, 0L))
        }

        windowAverage.state.get shouldBe Some(AveragedValue(num, data))

        when(taskContext.upstreamMinClock).thenReturn(num)
        windowAverage.onNext(Message("" + data, num))

        windowAverage.state.get shouldBe Some(AveragedValue(1L, data))
        system.shutdown()
    }
  }
}
