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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.api.CheckpointStore
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class CheckpointManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  val checkpointIntervalGen = Gen.chooseNum[Long](100L, 10000L)
  property("CheckpointManager should recover from CheckpointStore") {
    forAll(timestampGen, checkpointIntervalGen) {
      (timestamp: TimeStamp, checkpointInterval: Long) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        checkpointManager.recover(timestamp)

        verify(checkpointStore).recover(timestamp)
        checkpointManager.getCheckpointTime - timestamp should be <= checkpointInterval
    }
  }

  property("CheckpointManager should write checkpoint to CheckpointStore") {
    val checkpointGen = Gen.alphaStr.map(_.getBytes("UTF-8"))
    forAll(timestampGen, checkpointIntervalGen, checkpointGen) {
      (timestamp: TimeStamp, checkpointInterval: Long, checkpoint: Array[Byte]) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        checkpointManager.checkpoint(timestamp, checkpoint)

        verify(checkpointStore).persist(timestamp, checkpoint)
    }
  }

   property("CheckpointManager should close CheckpointStore") {
    forAll(checkpointIntervalGen) {
      (checkpointInterval: Long) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        checkpointManager.close()
        verify(checkpointStore).close()
    }
  }

  property("CheckpointManager should update checkpoint time according to max message timestamp") {
    val timestampListGen = Gen.containerOf[Array, TimeStamp](timestampGen) suchThat (_.nonEmpty)
    forAll(timestampListGen, checkpointIntervalGen) {
      (timestamps: Array[TimeStamp], checkpointInterval: Long) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        timestamps.foreach(checkpointManager.update)
        val maxTimestamp = timestamps.max
        checkpointManager.getMaxMessageTime shouldBe maxTimestamp

        val checkpointTime = checkpointManager.getCheckpointTime
        checkpointManager.checkpoint(checkpointTime, Array.empty[Byte])
        verify(checkpointStore).persist(MockitoMatchers.eq(checkpointTime),
          MockitoMatchers.anyObject[Array[Byte]]())
        val newCheckpointTime = checkpointManager.updateCheckpointTime()
        maxTimestamp should (be < newCheckpointTime and be >= (newCheckpointTime - checkpointInterval))
    }
  }

}
