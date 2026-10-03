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

package io.gearpump.transport.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

import java.util.ArrayList;
import java.util.List;

public class MessageDecoder extends FrameDecoder {
  public static final int MAX_FRAME_LENGTH = 1024 * 1024;
  private final ITransportMessageSerializer serializer;

  public MessageDecoder(ITransportMessageSerializer serializer) {
    this.serializer = serializer;
  }

  /*
   * Each TaskMessage is encoded as:
   *  sessionId ... int(4)
   *  source task ... long(8)
   *  target task ... long(8)
   *  len ... int(4)
   *  payload ... byte[]     *
   */
  protected List<TaskMessage> decode(ChannelHandlerContext ctx, Channel channel,
      ChannelBuffer buf) throws Exception {
    final int SESSION_LENGTH = 4; //int
    final int SOURCE_TASK_LENGTH = 8; //long
    final int TARGET_TASK_LENGTH = 8; //long
    final int MESSAGE_LENGTH = 4; //int
    final int HEADER_LENGTH = SESSION_LENGTH + SOURCE_TASK_LENGTH + TARGET_TASK_LENGTH + MESSAGE_LENGTH;

    // Make sure that we have received at least a short message
    long available = buf.readableBytes();
    if (available < HEADER_LENGTH) {
      //need more data
      return null;
    }

    List<TaskMessage> taskMessageList = new ArrayList<TaskMessage>();

    // Use while loop, try to decode as more messages as possible in single call
    while (available >= HEADER_LENGTH) {

      // Mark the current buffer position before reading task/len field
      // because the whole frame might not be in the buffer yet.
      // We will reset the buffer position to the marked position if
      // there's not enough bytes in the buffer.
      buf.markReaderIndex();

      int sessionId = buf.readInt();
      long targetTask = buf.readLong();
      long sourceTask = buf.readLong();
      // Read the length field.
      int length = buf.readInt();

      available -= HEADER_LENGTH;

      if (length < 0) {
        throw new CorruptedFrameException("Negative frame length");
      }
      if (length > MAX_FRAME_LENGTH) {
        throw new TooLongFrameException("Frame exceeds maximum length");
      }
      if (length == 0) {
        taskMessageList.add(new TaskMessage(sessionId, targetTask, sourceTask, null));
        break;
      }

      // Make sure if there's enough bytes in the buffer.
      if (available < length) {
        // The whole bytes were not received yet - return null.
        buf.resetReaderIndex();
        break;
      }
      available -= length;

      // There's enough bytes in the buffer. Read it.
      // A serializer must not read into the next frame in the cumulative buffer.
      FrameDataInput input = new FrameDataInput(
          new WrappedChannelBuffer(buf.readSlice(length)), length);
      Object message = serializer.deserialize(input, length);
      if (input.remaining() != 0) {
        throw new CorruptedFrameException("Serializer did not consume the complete frame");
      }

      // Successfully decoded a frame.
      // Return a TaskMessage object
      taskMessageList.add(new TaskMessage(sessionId, targetTask, sourceTask, message));
    }

    return taskMessageList.size() == 0 ? null : taskMessageList;
  }
}