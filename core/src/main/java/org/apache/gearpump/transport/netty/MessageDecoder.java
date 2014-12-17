/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.transport.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.util.ArrayList;
import java.util.List;

public class MessageDecoder extends FrameDecoder {
  /*
   * Each TaskMessage is encoded as:
   *  task (>=0) ... short(8)
   *  len ... int(4)
   *  payload ... byte[]     *
   */
  protected List<TaskMessage> decode(ChannelHandlerContext ctx, Channel channel,
                                     ChannelBuffer buf) {

    // Make sure that we have received at least a short message
    long available = buf.readableBytes();
    if (available < 8) {
      //need more data
      return null;
    }

    List<TaskMessage> taskMessageList = new ArrayList<>();

    // Use while loop, try to decode as more messages as possible in single call
    while (available >= 8) {

      // Mark the current buffer position before reading task/len field
      // because the whole frame might not be in the buffer yet.
      // We will reset the buffer position to the marked position if
      // there's not enough bytes in the buffer.
      buf.markReaderIndex();

      available -= 8;

      long targetTask = buf.readLong();

      if(available < 8) {
        buf.resetReaderIndex();
        break;
      }
      available -= 8;
      long sourceTask = buf.readLong();

      // Make sure that we have received at least an integer (length)
      if (available < 4) {
        // need more data
        buf.resetReaderIndex();
        break;
      }

      // Read the length field.
      int length = buf.readInt();

      available -= 4;

      if (length <= 0) {
        taskMessageList.add(new TaskMessage(targetTask, sourceTask, null));
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
      ChannelBuffer payload = buf.readBytes(length);

      // Successfully decoded a frame.
      // Return a TaskMessage object
      taskMessageList.add(new TaskMessage(targetTask, sourceTask, payload.array()));
    }

    return taskMessageList.size() == 0 ? null : taskMessageList;
  }
}