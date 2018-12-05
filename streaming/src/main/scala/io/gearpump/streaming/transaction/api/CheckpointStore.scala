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

package io.gearpump.streaming.transaction.api

import io.gearpump.Time.MilliSeconds

/**
 * CheckpointStore persistently stores mapping of timestamp to checkpoint
 * it's possible that two checkpoints have the same timestamp
 * CheckpointStore needs to handle this either during write or read
 */
trait CheckpointStore {

  def persist(timeStamp: MilliSeconds, checkpoint: Array[Byte]): Unit

  def recover(timestamp: MilliSeconds): Option[Array[Byte]]

  def close(): Unit
}

/**
 * Creates CheckpointStore instance at runtime
 */
trait CheckpointStoreFactory extends java.io.Serializable {
  /**
   * @param name a unique name which maps to a unique path in checkpoint store
   * @return a CheckpointStore instance
   */
  def getCheckpointStore(name: String): CheckpointStore
}

