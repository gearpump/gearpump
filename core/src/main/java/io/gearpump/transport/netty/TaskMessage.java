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

public class TaskMessage {

  // When network partition happen, there may be several task instances of
  // same taskId co-existing for a short period of time. When they send messages
  // to same target task, it may cause confusion.
  // With sessionId, we can know which messages are from an old session, and which
  // are from new session. Messages of old sesson will be dropped.

  private int _sessionId;
  private long _targetTask;
  private long _sourceTask;
  private Object _message;

  public TaskMessage(int sessionId, long targetTask, long sourceTask, Object message) {
    _sessionId = sessionId;
    _targetTask = targetTask;
    _sourceTask = sourceTask;
    _message = message;
  }

  public int sessionId() {
    return _sessionId;
  }

  public long targetTask() {
    return _targetTask;
  }

  public long sourceTask() {
    return _sourceTask;
  }

  public Object message() {
    return _message;
  }
}
