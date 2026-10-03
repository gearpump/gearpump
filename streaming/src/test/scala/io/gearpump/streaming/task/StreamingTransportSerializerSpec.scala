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

package io.gearpump.streaming.task

import io.gearpump.transport.netty.FrameDataInput
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StreamingTransportSerializerSpec extends AnyFlatSpec with Matchers {
  it should "reject negative and enclosing-frame-inconsistent lengths before allocation" in {
    Seq(-1, Int.MaxValue, 8).foreach { length =>
      val bytes = new ByteArrayOutputStream()
      val output = new DataOutputStream(bytes)
      output.writeLong(0L)
      output.writeInt(length)
      val input = new FrameDataInput(new DataInputStream(
        new ByteArrayInputStream(bytes.toByteArray)), bytes.size())
      intercept[IOException] { new SerializedMessageSerializer().read(input) }
    }
  }

  it should "round trip a valid serialized message in its bounded frame" in {
    val serializer = new StreamingTransportSerializer
    val message = SerializedMessage(123L, Array[Byte](1, 2, 3))
    val bytes = new ByteArrayOutputStream()
    serializer.serialize(new DataOutputStream(bytes), message)
    val result = serializer.deserialize(new DataInputStream(
      new ByteArrayInputStream(bytes.toByteArray)), bytes.size()).asInstanceOf[SerializedMessage]
    assert(result.timeStamp == message.timeStamp)
    assert(result.bytes.sameElements(message.bytes))
  }
}
