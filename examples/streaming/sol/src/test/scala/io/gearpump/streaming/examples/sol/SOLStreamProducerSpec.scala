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
package io.gearpump.streaming.examples.sol

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import java.time.Instant
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

class SOLStreamProducerSpec extends WordSpec with Matchers {

  "SOLStreamProducer" should {
    "producer message continuously" in {

      val conf = UserConfig.empty.withInt(SOLStreamProducer.BYTES_PER_MESSAGE, 100)
      val context = MockUtil.mockTaskContext

      val producer = new SOLStreamProducer(context, conf)
      producer.onStart(Instant.EPOCH)
      producer.onNext(Message("msg"))
      verify(context).output(any[Message])
    }
  }
}
