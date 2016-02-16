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

package io.gearpump.streaming.examples.state.processor

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.state.api.PersistentTask
import io.gearpump.streaming.state.impl.PersistentStateConfig
import io.gearpump.streaming.task.ReportCheckpointClock
import io.gearpump.streaming.transaction.api.CheckpointStoreFactory
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.state.impl.InMemoryCheckpointStoreFactory
import io.gearpump.streaming.task.StartTime
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration._

class CountProcessorSpec extends PropSpec with PropertyChecks with Matchers {

  property("CountProcessor should update state") {

    val taskContext = MockUtil.mockTaskContext

    implicit val system = ActorSystem("test")

    val longGen = Gen.chooseNum[Long](1, 1000)
    forAll(longGen) {
      (num: Long) =>

        val conf = UserConfig.empty
          .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
          .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, num)
          .withValue[CheckpointStoreFactory](PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, new InMemoryCheckpointStoreFactory)

        val count = new CountProcessor(taskContext, conf)

        val appMaster = TestProbe()(system)
        when(taskContext.appMaster).thenReturn(appMaster.ref)

        count.onStart(StartTime(0L))
        appMaster.expectMsg(ReportCheckpointClock(taskContext.taskId, 0L))


        for (i <- 0L to num) {
          count.onNext(Message("", i))
          count.state.get shouldBe Some(i + 1)
        }
        // next checkpoint time is at num
        // not yet
        when(taskContext.upstreamMinClock).thenReturn(0L)
        count.onNext(PersistentTask.CHECKPOINT)
        appMaster.expectNoMsg(10 milliseconds)

        // time to checkpoint
        when(taskContext.upstreamMinClock).thenReturn(num)
        count.onNext(PersistentTask.CHECKPOINT)
        // only the state before checkpoint time is checkpointed
        appMaster.expectMsg(ReportCheckpointClock(taskContext.taskId, num))
    }

    system.shutdown()
    system.awaitTermination()
  }
}
