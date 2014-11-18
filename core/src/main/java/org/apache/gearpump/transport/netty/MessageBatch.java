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

import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MessageBatch {

  private static final Logger log = LoggerFactory.getLogger(MessageBatch.class);

  private int buffer_size;
  private List<TaskMessage> messages;
  private int encoded_length;

  MessageBatch(int buffer_size) {
    this.buffer_size = buffer_size;
    messages = new ArrayList<>();
    encoded_length = 0;
  }

  void add(TaskMessage taskMessage) {
    if (taskMessage == null) {
      throw new RuntimeException("null object forbidden in a message batch");
    }

    messages.add(taskMessage);
    encoded_length += msgEncodeLength(taskMessage);
  }


  TaskMessage get(int index) {
    return messages.get(index);
  }

  /**
   * try to add a TaskMessage to a batch
   *
   * @param taskMsg - {@link org.apache.gearpump.transport.netty.TaskMessage}
   * @return false if the msg could not be added due to buffer size limit; true otherwise
   */
  boolean tryAdd(TaskMessage taskMsg) {
    if ((encoded_length + msgEncodeLength(taskMsg)) <= buffer_size) {
      add(taskMsg);
      return true;
    }
    return false;
  }

  private int msgEncodeLength(TaskMessage taskMsg) {
    int size = 0;
    if (taskMsg != null) {
      size = 12; //LONG + INT
      if (taskMsg.message() != null) {
        size += taskMsg.message().length;
      }
    }
    return size;
  }

  /**
   *
   * @return true, if allowed buffer is Full
   */
  boolean isFull() {
    return encoded_length >= buffer_size;
  }

  /**
   *
   * @return true, if no messages in this batch
   */
  boolean isEmpty() {
    return messages.isEmpty();
  }

  /**
   *
   * @return number of messages available in this batch
   */
  int size() {
    return messages.size();
  }

  /**
   * create a buffer containing the encoding of this batch
   */
  ChannelBuffer buffer() throws IOException {
    ChannelBufferOutputStream bout =
        new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encoded_length));

    try {
      for (TaskMessage msg : messages) {
        writeTaskMessage(bout, msg);
      }
      return bout.buffer();
    } catch (IOException e) {
      log.error("Error while writing Tasks to Channel Buffer - {}", e.getMessage());
    } finally {
      Closeables.close(bout, false);
    }
    return null;
  }

  /**
   * write a TaskMessage into a stream
   * <p/>
   * Each TaskMessage is encoded as:
   * task ... long(8)
   * len ... int(4)
   * payload ... byte[]     *
   */
  private void writeTaskMessage(ChannelBufferOutputStream bout,
                                TaskMessage message) throws IOException {
    int payload_len = 0;
    if (message.message() != null) {
      payload_len = message.message().length;
    }
    long task_id = message.task();

    bout.writeLong(task_id);
    bout.writeInt(payload_len);
    if (payload_len > 0) {
      bout.write(message.message());
    }
  }
}