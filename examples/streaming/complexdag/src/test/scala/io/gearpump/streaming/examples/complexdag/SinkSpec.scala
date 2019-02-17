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
package io.gearpump.streaming.examples.complexdag

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class SinkSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {

  val context = MockUtil.mockTaskContext

  val sink = new Sink(context, UserConfig.empty)

  property("Sink should send a Vector[String](classOf[Sink].getCanonicalName, " +
    "classOf[Sink].getCanonicalName") {
    val list = Vector(classOf[Sink].getCanonicalName)
    val expected = Vector(classOf[Sink].getCanonicalName, classOf[Sink].getCanonicalName)
    sink.onNext(Message(list))

    (0 until sink.list.size).map(i => {
      assert(sink.list(i).equals(expected(i)))
    })
  }
}
