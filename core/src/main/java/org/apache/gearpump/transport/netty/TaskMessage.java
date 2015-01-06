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

import java.util.Arrays;

public class TaskMessage {

  // When network partition happen, there may be several task instances of
  // same taskId co-existing for a short period of time. When they send messages
  // to same target task, it may cause confusion.
  // With sessionId, we can know which messages are from an old session, and which
  // are from new session. Messages of old sesson will be dropped.

  private int _sessionId;
  private long _targetTask;
  private long _sourceTask;
  private byte[] _message;

  public TaskMessage(int sessionId, long targetTask, long sourceTask,  byte[] message) {
    _sessionId = sessionId;
    _targetTask = targetTask;
    _sourceTask = sourceTask;
    _message = message;
  }

  public int sessionId() {return _sessionId; }

  public long targetTask() {
    return _targetTask;
  }

  public long sourceTask(){
    return _sourceTask;
  }

  public byte[] message() {
    return _message;
  }

  @Override
  public boolean equals(Object obj)  {
    TaskMessage other = (TaskMessage)obj;
    return this.sessionId() == other.sessionId()
        && this._sourceTask == other.sourceTask()
        && this.targetTask() == other.targetTask()
        && Arrays.equals(this.message(), other.message());
  }
}
