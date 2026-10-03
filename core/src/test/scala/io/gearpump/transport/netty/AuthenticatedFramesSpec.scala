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

import io.gearpump.security.ControlCapability
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.CorruptedFrameException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuthenticatedFramesSpec extends AnyFlatSpec with Matchers {
  private val capability = ControlCapability.random()
  private class Encoder(key: String = capability) extends AuthenticatedFrames.Encoder(key) {
    def frame(payload: ChannelBuffer): ChannelBuffer = encode(null, null, payload).asInstanceOf[ChannelBuffer]
  }
  private class Decoder(key: String = capability) extends AuthenticatedFrames.Decoder(key) {
    def read(payload: ChannelBuffer): ChannelBuffer = decode(null, null, payload).asInstanceOf[ChannelBuffer]
  }
  private def payload = ChannelBuffers.wrappedBuffer(Array[Byte](1, 2, 3, 4))
  it should "authenticate full metadata/payload, preserve fragments and reject replay" in {
    val encoder = new Encoder
    val decoder = new Decoder
    val frame = encoder.frame(payload)
    val fragment = ChannelBuffers.dynamicBuffer()
    fragment.writeBytes(frame, 0, 18)
    decoder.read(fragment) shouldBe null
    fragment.readerIndex() shouldBe 0
    fragment.writeBytes(frame, 18, frame.readableBytes() - 18)
    decoder.read(fragment) shouldBe payload
    intercept[CorruptedFrameException] { decoder.read(frame.copy()) }
    decoder.read(encoder.frame(payload)) shouldBe payload
  }
  it should "reject another application's key and tampered metadata or payload" in {
    val frame = new Encoder().frame(payload)
    intercept[CorruptedFrameException] { new Decoder(ControlCapability.random()).read(frame.copy()) }
    Seq(0, 4, 12, 16, frame.readableBytes() - 1).foreach { index =>
      val forged = frame.copy()
      forged.setByte(index, forged.getByte(index) ^ 1)
      intercept[CorruptedFrameException] { new Decoder().read(forged) }
    }
  }
  it should "reject cleartext, negative and oversized declarations before buffering" in {
    Seq(-1, Int.MaxValue, AuthenticatedFrames.MaxBatchLength + 1, 0).foreach { size =>
      val header = ChannelBuffers.buffer(16)
      header.writeInt(0x47505431)
      header.writeLong(0)
      header.writeInt(size)
      intercept[CorruptedFrameException] { new Decoder().read(header) }
    }
    intercept[CorruptedFrameException] { new Decoder().read(ChannelBuffers.wrappedBuffer(new Array[Byte](24))) }
    intercept[IllegalArgumentException] { new Encoder("") }
  }
}
