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

import com.twitter.bijection.Injection
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.state.api.CheckpointStore
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}

class DefaultCheckpointManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](1, 1000)

  property("DefaultCheckpointManager should checkpoint when timestamp exceeds nextCheckpointTime") {
    forAll(longGen, longGen) { (interval: Long, timestamp: TimeStamp) =>
      val checkpointStore = mock[CheckpointStore]
      val checkpointManager = new DefaultCheckpointManager(checkpointStore, interval)

      checkpointManager.getCheckpointTime shouldBe None
      checkpointManager.syncTime(0L)
      checkpointManager.syncTime(timestamp)
      checkpointManager.shouldCheckpoint shouldBe timestamp >= interval
    }
  }

  property("DefaultCheckpointManager checkpoint should write time -> checkpoint to store") {
    forAll(longGen, longGen) { (timestamp: TimeStamp, data: Long) =>
      val checkpointStore = mock[CheckpointStore]
      val checkpointManager = new DefaultCheckpointManager(checkpointStore)
      val bytes = Injection[Long, Array[Byte]](data)

      checkpointManager.checkpoint(timestamp, bytes)
      verify(checkpointStore).write(timestamp, bytes)
    }
  }

  property("DefaultCheckpointManager recover should read checkpoint at a time from store") {
    forAll(longGen, longGen) { (timestamp: TimeStamp, data: Long) =>
      val checkpointStore = mock[CheckpointStore]
      val checkpointManager = new DefaultCheckpointManager(checkpointStore)
      val bytes = Injection[Long, Array[Byte]](data)

      when(checkpointStore.read(timestamp)).thenReturn(Some(bytes))

      checkpointManager.recover(timestamp) shouldBe Some(bytes)
      verify(checkpointStore).read(timestamp)
    }
  }

  property("DefaultCheckpointManager should close store") {
    val checkpointStore = mock[CheckpointStore]
    val checkpointManager = new DefaultCheckpointManager(checkpointStore)

    checkpointManager.close()
    verify(checkpointStore).close()
  }

  property("DefaultCheckpointManager should update nextCheckpointTime on each checkpoint") {
    forAll(longGen, longGen, longGen) { (interval: Long, timestamp: TimeStamp, data: Long) =>
      val checkpointStore = mock[CheckpointStore]
      val checkpointManager = new DefaultCheckpointManager(checkpointStore, interval)
      val bytes = Injection[Long, Array[Byte]](data)

      doNothing().when(checkpointStore).write(timestamp, bytes)

      checkpointManager.getCheckpointTime shouldBe None
      checkpointManager.syncTime(0L)
      checkpointManager.syncTime(timestamp)
      checkpointManager.getCheckpointTime shouldBe Some(interval)
      checkpointManager.checkpoint(timestamp, bytes)
      checkpointManager.getCheckpointTime shouldBe Some(interval * 2)

      checkpointManager.setCheckpointInterval(interval * 2)
      checkpointManager.getCheckpointTime shouldBe Some(interval * 2)
      checkpointManager.checkpoint(timestamp, bytes)
      checkpointManager.getCheckpointTime shouldBe Some(interval * 4)
    }
  }
}
