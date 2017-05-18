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

package org.apache.gearpump.streaming.examples.state.processor

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.impl.{InMemoryCheckpointStoreFactory, PersistentStateConfig}
import org.apache.gearpump.streaming.task.UpdateCheckpointClock
import org.apache.gearpump.streaming.transaction.api.CheckpointStoreFactory

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
          .withValue[CheckpointStoreFactory](PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY,
            new InMemoryCheckpointStoreFactory)

        val count = new CountProcessor(taskContext, conf)

        val appMaster = TestProbe()(system)
        when(taskContext.appMaster).thenReturn(appMaster.ref)

        count.onStart(Instant.EPOCH)
        appMaster.expectMsg(UpdateCheckpointClock(taskContext.taskId, 0L))

        for (i <- 0L to num) {
          count.onNext(Message("", i))
          count.getState.get shouldBe Some(i + 1)
        }

        // Time to checkpoint
        count.onWatermarkProgress(Instant.ofEpochMilli(num))
        // Only the state before checkpoint time is checkpointed
        appMaster.expectMsg(UpdateCheckpointClock(taskContext.taskId, num))
    }

    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
