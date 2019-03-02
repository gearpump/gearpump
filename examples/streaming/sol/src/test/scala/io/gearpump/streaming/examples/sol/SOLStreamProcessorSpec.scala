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
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class SOLStreamProcessorSpec extends FlatSpec with Matchers {

  it should "pass the message downstream" in {
    val context = MockUtil.mockTaskContext

    val sol = new SOLStreamProcessor(context, UserConfig.empty)
    sol.onStart(Instant.EPOCH)
    val msg = Message("msg")
    sol.onNext(msg)
    verify(context, times(1)).output(msg)
    sol.onStop()
  }
}
