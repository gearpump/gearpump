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
package io.gearpump.streaming.dsl.task

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue, TriggeredOutputs}
import java.time.Instant
import org.mockito.Mockito.{verify, when}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

class TransformTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("MergeTask should trigger on watermark") {
    val longGen = Gen.chooseNum[Long](1L, 1000L)
    val watermarkGen = longGen.map(Instant.ofEpochMilli)

    forAll(watermarkGen) { (watermark: Instant) =>
      val windowRunner = mock[StreamingOperator[Any, Any]]
      val context = MockUtil.mockTaskContext
      val config = UserConfig.empty
      val task = new TransformTask[Any, Any](windowRunner, context, config)
      val time = watermark.minusMillis(1L)
      val value: Any = time
      val message = Message(value, time)

      task.onNext(message)
      verify(windowRunner).foreach(TimestampedValue(value, time))

      when(windowRunner.trigger(watermark)).thenReturn(
        TriggeredOutputs(Some(TimestampedValue(value, time)), watermark))
      task.onWatermarkProgress(watermark)
      verify(context).output(message)
      verify(context).updateWatermark(watermark)
    }
  }

}
