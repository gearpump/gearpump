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

import java.nio.ByteBuffer;

public class TaskMessage {
  private long _targetTask;
  private long _sourceTask;
  private byte[] _message;

  public TaskMessage(long targetTask, long sourceTask,  byte[] message) {
    _targetTask = targetTask;
    _sourceTask = sourceTask;
    _message = message;
  }

  public long targetTask() {
    return _targetTask;
  }

  public long sourceTask(){
    return _sourceTask;
  }

  public byte[] message() {
    return _message;
  }

  public ByteBuffer serialize() {
    ByteBuffer bb = ByteBuffer.allocate(_message.length + 8);
    bb.putLong(_targetTask);
    bb.putLong(_sourceTask);
    bb.put(_message);
    return bb;
  }

  public void deserialize(ByteBuffer packet) {
    if (packet == null) return;
    _targetTask = packet.getLong();
    _sourceTask = packet.getLong();
    _message = new byte[packet.limit() - 8];
    packet.get(_message);
  }

}
