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

package org.apache.gearpump.streaming.transaction.storage.api

import org.apache.gearpump.streaming.transaction.checkpoint.api.{CheckpointSerDe, Checkpoint, CheckpointManager}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaSource

class StorageManager[K, V](id: String,
                           store: KeyValueStore[K, V],
                           checkpointManager: CheckpointManager[K, V],
                           checkpointSerDe: CheckpointSerDe[K, V])
  extends KeyValueStore[K, V] {

  var states: Map[K, V] = Map.empty[K, V]

  def checkpoint: Checkpoint[K, V] = {
    val kafkaSource: KafkaSource = KafkaSource(getStorageTopic, 0)
    val checkpoint: Checkpoint[K, V] = Checkpoint(states.toList)
    states = Map.empty[K, V]
    checkpointManager.writeCheckpoint(kafkaSource, checkpoint, checkpointSerDe)
    checkpoint
  }

  def restore: Checkpoint[K, V] = {
    val kafkaSource: KafkaSource = KafkaSource(getStorageTopic, 0)
    val checkpoint: Checkpoint[K, V] = checkpointManager.readCheckpoint(kafkaSource, checkpointSerDe)
    store.putAll(checkpoint.records)
    checkpoint
  }

  override def close(): Unit = {
    store.close()
  }

  override def flush(): Unit = {
    store.flush()
  }

  override def delete(key: K): Option[V] = {
    store.delete(key)
  }

  override def putAll(kvs: List[(K, V)]): Unit = {
    states ++= kvs
    store.putAll(kvs)
  }

  override def put(key: K, value: V): Option[V] = {
    states += key -> value
    store.put(key, value)
  }

  override def get(key: K): Option[V] = {
    store.get(key)
  }

  private def getStorageTopic: String = {
    s"storage_${id}"
  }
}
