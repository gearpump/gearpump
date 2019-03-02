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

import akka.actor.ActorSystem
import io.gearpump.Message
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.MockUtil._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

class SourceSpec extends WordSpec with Matchers {

  "Source" should {
    "Source should send a msg of Vector[String](classOf[Source].getCanonicalName)" in {
      ActorSystem("Source", TestUtil.DEFAULT_CONFIG)

      ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)

      val context = MockUtil.mockTaskContext

      val source = new Source(context, UserConfig.empty)
      source.onNext(Message("start"))

      verify(context).output(argMatch[Message](
        Vector(classOf[Source].getCanonicalName) == _.value))
    }
  }
}
