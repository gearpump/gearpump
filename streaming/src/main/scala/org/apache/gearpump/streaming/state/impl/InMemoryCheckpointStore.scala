/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}

/**
 * an in memory store provided for test
 * should not be used in real cases
 */
class InMemoryCheckpointStore extends CheckpointStore {
  private var checkpoints = Map.empty[TimeStamp, Array[Byte]]

  override def persist(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    checkpoints += timestamp -> checkpoint
  }

  override def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    checkpoints.get(timestamp)
  }

  override def close(): Unit = {
    checkpoints = Map.empty[TimeStamp, Array[Byte]]
  }
}

class InMemoryCheckpointStoreFactory extends CheckpointStoreFactory {
  override def getCheckpointStore(name: String): CheckpointStore = {
    new InMemoryCheckpointStore
  }
}
