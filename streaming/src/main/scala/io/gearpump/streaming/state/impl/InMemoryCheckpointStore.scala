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

package io.gearpump.streaming.state.impl

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}

/**
 * an in memory store provided for test
 * should not be used in real cases
 */
class InMemoryCheckpointStore extends CheckpointStore {
  private var checkpoints = Map.empty[MilliSeconds, Array[Byte]]

  override def persist(timestamp: MilliSeconds, checkpoint: Array[Byte]): Unit = {
    checkpoints += timestamp -> checkpoint
  }

  override def recover(timestamp: MilliSeconds): Option[Array[Byte]] = {
    checkpoints.get(timestamp)
  }

  override def close(): Unit = {
    checkpoints = Map.empty[MilliSeconds, Array[Byte]]
  }
}

class InMemoryCheckpointStoreFactory extends CheckpointStoreFactory {
  override def getCheckpointStore(name: String): CheckpointStore = {
    new InMemoryCheckpointStore
  }
}
