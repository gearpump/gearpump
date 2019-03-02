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

package io.gearpump.streaming.dsl.window.impl

import io.gearpump.Message
import io.gearpump.streaming.dsl.api.functions.ReduceFunction
import io.gearpump.streaming.dsl.plan.functions.FoldRunner
import io.gearpump.streaming.dsl.window.api.SessionWindows
import io.gearpump.streaming.source.Watermark
import java.time.{Duration, Instant}
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

class DefaultWindowRunnerSpec extends PropSpec with PropertyChecks
  with Matchers with MockitoSugar {

  property("DefaultWindowRunner should handle SessionWindow") {

    val data = List(
      Message(("foo", 1L), Instant.ofEpochMilli(1L)),
      Message(("foo", 1L), Instant.ofEpochMilli(15L)),
      Message(("foo", 1L), Instant.ofEpochMilli(25L)),
      Message(("foo", 1L), Instant.ofEpochMilli(26L))
    )

    type KV = (String, Long)
    val reduce = ReduceFunction[KV]((kv1, kv2) => (kv1._1, kv1._2 + kv2._2))
    val windows = SessionWindows.apply(Duration.ofMillis(4L))
    val windowRunner = new WindowOperator[KV, Option[KV]](windows,
      new FoldRunner[KV, Option[KV]](reduce, "reduce"))

    data.foreach(m => windowRunner.foreach(TimestampedValue(m.value.asInstanceOf[KV], m.timestamp)))
    windowRunner.trigger(Watermark.MAX).outputs.toList shouldBe
      List(
        TimestampedValue(Some(("foo", 1)), Instant.ofEpochMilli(4)),
        TimestampedValue(Some(("foo", 1)), Instant.ofEpochMilli(18)),
        TimestampedValue(Some(("foo", 2)), Instant.ofEpochMilli(29))
      )
  }

}
