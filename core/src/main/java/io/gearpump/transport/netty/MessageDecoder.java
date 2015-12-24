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

package io.gearpump.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.ArrayList;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
  private ITransportMessageSerializer serializer;
  private WrappedByteBuf dataInput = new WrappedByteBuf();

  public MessageDecoder(ITransportMessageSerializer serializer){
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

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
    this.dataInput.setByteBuf(byteBuf);

    final int SESION_LENGTH = 4; //int
    final int SOURCE_TASK_LENGTH = 8; //long
    final int TARGET_TASK_LENGTH = 8; //long
    final int MESSAGE_LENGTH = 4; //int
    final int HEADER_LENGTH = SESION_LENGTH + SOURCE_TASK_LENGTH + TARGET_TASK_LENGTH + MESSAGE_LENGTH;

    // Make sure that we have received at least a short message
    long available = byteBuf.readableBytes();
    if (available < HEADER_LENGTH) {
      //need more data
      return;
    }

    List<TaskMessage> taskMessageList = new ArrayList<TaskMessage>();

    // Use while loop, try to decode as more messages as possible in single call
    while (available >= HEADER_LENGTH) {

      // Mark the current buffer position before reading task/len field
      // because the whole frame might not be in the buffer yet.
      // We will reset the buffer position to the marked position if
      // there's not enough bytes in the buffer.
      byteBuf.markReaderIndex();

      int sessionId = byteBuf.readInt();
      long targetTask = byteBuf.readLong();
      long sourceTask = byteBuf.readLong();
      // Read the length field.
      int length = byteBuf.readInt();

      available -= HEADER_LENGTH;

      if (length <= 0) {
        taskMessageList.add(new TaskMessage(sessionId, targetTask, sourceTask, null));
        break;
      }

      // Make sure if there's enough bytes in the buffer.
      if (available < length) {
        // The whole bytes were not received yet - return null.
        byteBuf.resetReaderIndex();
        break;
      }
      available -= length;

      // There's enough bytes in the buffer. Read it.
      Object message = serializer.deserialize(dataInput, length);

      // Successfully decoded a frame.
      // Return a TaskMessage object
      taskMessageList.add(new TaskMessage(sessionId, targetTask, sourceTask, message));
    }

    if(taskMessageList.size() > 0) {
      list.add(taskMessageList);
    }
  }
}