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

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

/** App authority is verified before any transport or application deserializer runs.
 * TLS supplies peer authentication, confidentiality and connection replay protection.
 */
object AuthenticatedFrames {
  val MaxBatchLength = 4 * 1024 * 1024
  private val Magic = 0x47505431 // GPT1, deliberately incompatible with cleartext frames.
  private val Header = 16
  private val Tag = 32
  private def key(capability: String): Array[Byte] = {
    require(capability != null && capability.matches("[A-Za-z0-9_-]{43}"),
      "AppMaster-issued 256-bit task transport capability required")
    val decoded = Base64.getUrlDecoder.decode(capability)
    require(decoded.length == 32, "Invalid task transport capability")
    decoded
  }
  private def tag(key: Array[Byte], bytes: Array[Byte]): Array[Byte] = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key, "HmacSHA256"))
    mac.update("gearpump-task-frame-v1".getBytes(UTF_8))
    mac.doFinal(bytes)
  }

  class Encoder(capability: String) extends OneToOneEncoder {
    private val secret = key(capability)
    private var sequence = 0L
    override protected def encode(ctx: ChannelHandlerContext, channel: Channel,
        message: Any): AnyRef = {
      val batch = message.asInstanceOf[ChannelBuffer]
      val length = batch.readableBytes()
      require(length > 0 && length <= MaxBatchLength && sequence >= 0,
        "Task batch exceeds authenticated transport bounds")
      val frame = ChannelBuffers.buffer(Header + length + Tag)
      frame.writeInt(Magic)
      frame.writeLong(sequence)
      frame.writeInt(length)
      frame.writeBytes(batch, batch.readerIndex(), length)
      val signed = new Array[Byte](Header + length)
      frame.getBytes(0, signed)
      frame.writeBytes(tag(secret, signed))
      sequence += 1
      frame
    }
  }

  class Decoder(capability: String) extends FrameDecoder {
    private val secret = key(capability)
    private var sequence = 0L
    override protected def decode(ctx: ChannelHandlerContext, channel: Channel,
        buffer: ChannelBuffer): AnyRef = {
      if (buffer.readableBytes() < Header) return null
      val start = buffer.readerIndex()
      val magic = buffer.getInt(start)
      val actualSequence = buffer.getLong(start + 4)
      val length = buffer.getInt(start + 12)
      if (magic != Magic || sequence < 0 || actualSequence != sequence ||
          length <= 0 || length > MaxBatchLength) {
        throw new CorruptedFrameException("Invalid authenticated task frame")
      }
      if (buffer.readableBytes() < Header + length + Tag) return null
      val signed = new Array[Byte](Header + length)
      val actualTag = new Array[Byte](Tag)
      buffer.getBytes(start, signed)
      buffer.getBytes(start + Header + length, actualTag)
      if (!MessageDigest.isEqual(tag(secret, signed), actualTag)) {
        throw new CorruptedFrameException("Task application authentication failed")
      }
      buffer.skipBytes(Header)
      val batch = buffer.readBytes(length)
      buffer.skipBytes(Tag)
      sequence += 1
      batch
    }
  }
}
