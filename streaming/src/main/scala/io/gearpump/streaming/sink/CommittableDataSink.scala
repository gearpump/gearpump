/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.sink

import io.gearpump.Time.MilliSeconds

/**
 * A sink whose external commits are coordinated with Gearpump checkpoints.
 *
 * [[DataSinkTask]] persists the bytes returned by [[prepareCommit]] before invoking [[commit]],
 * and reports the checkpoint clock only after the external commit succeeds. Implementations must
 * make a commit idempotent for a given checkpoint timestamp because a successful external commit
 * can be replayed before its checkpoint clock becomes globally visible.
 */
trait CommittableDataSink extends DataSink {

  /** Sets the boundary used to separate the next committable from later records. */
  def setNextCheckpointTime(checkpointTime: MilliSeconds): Unit

  /** Restores a committable previously persisted for the recovered checkpoint. */
  def restoreCommit(checkpointTime: MilliSeconds, checkpoint: Array[Byte]): Unit

  /** Completes the writes before `checkpointTime` and serializes their committable state. */
  def prepareCommit(checkpointTime: MilliSeconds): Array[Byte]

  /** Publishes the prepared or restored committable to the external system. */
  def commit(checkpointTime: MilliSeconds): Unit
}
