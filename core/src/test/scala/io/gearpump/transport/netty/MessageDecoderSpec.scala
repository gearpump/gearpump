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

package io.gearpump.transport.netty

import io.gearpump.transport.MockTransportSerializer
import java.io.EOFException
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, TooLongFrameException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MessageDecoderSpec extends AnyFlatSpec with Matchers {
  private class Decoder extends MessageDecoder(new MockTransportSerializer) {
    def read(buffer: ChannelBuffer): java.util.List[TaskMessage] = decode(null, null, buffer)
  }

  private def header(length: Int): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    buffer.writeInt(1)
    buffer.writeLong(2L)
    buffer.writeLong(3L)
    buffer.writeInt(length)
    buffer
  }

  it should "reject oversized lengths before buffering their payload" in {
    intercept[TooLongFrameException] { new Decoder().read(header(Int.MaxValue)) }
    intercept[CorruptedFrameException] { new Decoder().read(header(-1)) }
  }

  it should "preserve partial frames and decode bounded complete frames" in {
    val buffer = header(4)
    val decoder = new Decoder
    assert(decoder.read(buffer) == null)
    assert(buffer.readerIndex() == 0)
    buffer.writeInt(42)
    assert(decoder.read(buffer).size() == 1)
    assert(buffer.readableBytes() == 0)
  }

  it should "prevent nested reads from consuming another frame" in {
    val buffer = ChannelBuffers.dynamicBuffer()
    buffer.writeLong(10L)
    buffer.writeInt(99)
    val input = new FrameDataInput(new WrappedChannelBuffer(buffer.readSlice(8)), 8)
    assert(input.readLong() == 10L)
    intercept[EOFException] { input.readInt() }
    assert(buffer.readInt() == 99)
  }
}
