/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.state.impl

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.transaction.api.CheckpointStore
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

class CheckpointManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  val checkpointIntervalGen = Gen.chooseNum[Long](100L, 10000L)
  property("CheckpointManager should recover from CheckpointStore") {
    forAll(timestampGen, checkpointIntervalGen) {
      (timestamp: MilliSeconds, checkpointInterval: Long) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        checkpointManager.recover(timestamp)

        verify(checkpointStore).recover(timestamp)
    }
  }

  property("CheckpointManager should write checkpoint to CheckpointStore") {
    val checkpointGen = Gen.alphaStr.map(_.getBytes("UTF-8"))
    forAll(timestampGen, checkpointIntervalGen, checkpointGen) {
      (timestamp: MilliSeconds, checkpointInterval: Long, checkpoint: Array[Byte]) =>
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
    forAll(timestampGen, checkpointIntervalGen) {
      (timestamp: MilliSeconds, checkpointInterval: Long) =>
        val checkpointStore = mock[CheckpointStore]
        val checkpointManager =
          new CheckpointManager(checkpointInterval, checkpointStore)
        checkpointManager.update(timestamp)
        checkpointManager.getMaxMessageTime shouldBe timestamp

        val checkpointTime = checkpointManager.getCheckpointTime.get
        timestamp should (be < checkpointTime and be >= (checkpointTime - checkpointInterval))

        checkpointManager.checkpoint(checkpointTime, Array.empty[Byte])
        verify(checkpointStore).persist(MockitoMatchers.eq(checkpointTime),
          MockitoMatchers.anyObject[Array[Byte]]())
        checkpointManager.getCheckpointTime shouldBe empty
    }
  }
}
