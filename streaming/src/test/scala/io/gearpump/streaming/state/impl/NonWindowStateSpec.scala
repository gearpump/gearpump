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
import io.gearpump.streaming.state.api.{Monoid, Serializer}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks
import scala.util.Success

class NonWindowStateSpec extends AnyPropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](100L, System.currentTimeMillis())

  property("NonWindowState should recover checkpointed state at given timestamp") {
    forAll(longGen) {
      (timestamp: MilliSeconds) =>
        val monoid = mock[Monoid[AnyRef]]
        val serializer = mock[Serializer[AnyRef]]
        val bytes = Array.empty[Byte]
        val checkpoint = mock[AnyRef]
        val zero = mock[AnyRef]
        when(monoid.zero).thenReturn(zero, zero)
        when(monoid.plus(zero, zero)).thenReturn(zero)
        when(monoid.plus(checkpoint, zero)).thenReturn(checkpoint)

        val state = new NonWindowState[AnyRef](monoid, serializer)
        state.left shouldBe zero
        state.right shouldBe zero
        state.get shouldBe Some(zero)

        when(serializer.deserialize(bytes)).thenReturn(Success(checkpoint))
        state.recover(timestamp, bytes)

        state.left shouldBe checkpoint
        state.right shouldBe zero
        state.get shouldBe Some(checkpoint)
    }
  }

  property("NonWindowState checkpoints state") {
    forAll(longGen) {
      (checkpointTime: MilliSeconds) =>
        val monoid = mock[Monoid[AnyRef]]
        val serializer = mock[Serializer[AnyRef]]

        val left = mock[AnyRef]
        val right = mock[AnyRef]
        val zero = mock[AnyRef]
        val plus = mock[AnyRef]

        when(monoid.zero).thenReturn(zero, zero)
        when(monoid.plus(zero, zero)).thenReturn(zero)

        val state = new NonWindowState[AnyRef](monoid, serializer)
        state.left shouldBe zero
        state.right shouldBe zero
        state.get shouldBe Some(zero)

        state.left = left
        state.right = right

        when(monoid.zero).thenReturn(zero)
        when(monoid.plus(left, right)).thenReturn(plus)
        when(monoid.plus(plus, zero)).thenReturn(plus)
        state.checkpoint()

        verify(serializer).serialize(left)
        state.left shouldBe plus
        state.right shouldBe zero
        state.get shouldBe Some(plus)
    }
  }

  property("NonWindowState updates state") {
    forAll(longGen) {
      (checkpointTime: MilliSeconds) =>
        val monoid = mock[Monoid[AnyRef]]
        val serializer = mock[Serializer[AnyRef]]

        val left = mock[AnyRef]
        val right = mock[AnyRef]
        val zero = mock[AnyRef]
        val plus = mock[AnyRef]

        when(monoid.zero).thenReturn(zero, zero)
        when(monoid.plus(zero, zero)).thenReturn(zero)

        val state = new NonWindowState[AnyRef](monoid, serializer)
        state.left shouldBe zero
        state.right shouldBe zero
        state.get shouldBe Some(zero)

        when(monoid.plus(zero, left)).thenReturn(left)
        when(monoid.plus(left, zero)).thenReturn(left)
        state.setNextCheckpointTime(checkpointTime)
        state.update(checkpointTime - 1, left)
        state.left shouldBe left
        state.right shouldBe zero
        state.get shouldBe Some(left)

        when(monoid.plus(zero, right)).thenReturn(right)
        when(monoid.plus(left, right)).thenReturn(plus)
        state.setNextCheckpointTime(checkpointTime)
        state.update(checkpointTime + 1, right)
        state.left shouldBe left
        state.right shouldBe right
        state.get shouldBe Some(plus)
    }
  }
}
