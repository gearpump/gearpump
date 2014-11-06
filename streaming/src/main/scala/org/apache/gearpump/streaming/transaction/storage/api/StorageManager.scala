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

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.checkpoint.api.{CheckpointSerDe, Checkpoint, CheckpointManager, Source}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaUtil._
import org.slf4j.{Logger, LoggerFactory}

object StorageManager {
  class StoreCheckpointSerDe[K, V](keyValueSerDe: KeyValueSerDe[K, V])
    extends CheckpointSerDe[TimeStamp, (K, V)] {
    override def toKeyBytes(key: TimeStamp): Array[Byte] = {
      longToByteArray(key)
    }

    override def toValueBytes(value: (K, V)): Array[Byte] = {
      keyValueSerDe.toBytes(value)
    }

    override def fromKeyBytes(bytes: Array[Byte]): TimeStamp = {
      byteArrayToLong(bytes)
    }

    override def fromValueBytes(bytes: Array[Byte]): (K, V) = {
      keyValueSerDe.fromBytes(bytes)
    }
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[StorageManager[_, _]])
}

class StorageManager[K, V](id: String,
                           store: KeyValueStore[K, V],
                           keyValueSerDe: KeyValueSerDe[K, V],
                           checkpointManager: CheckpointManager[TimeStamp, (K, V)]
                           )
  extends KeyValueStore[K, V] {
  import org.apache.gearpump.streaming.transaction.storage.api.StorageManager._

  private var states: Map[K, V] = Map.empty[K, V]
  private val source : Source = new Source {
    def name: String = s"storage_$id"
    def partition: Int = 0
  }
  private val checkpointSerDe = new StoreCheckpointSerDe[K, V](keyValueSerDe)

  def start(): Unit = {
    checkpointManager.register(Array(source))
    checkpointManager.start()
  }

  def checkpoint(timestamp: TimeStamp): Checkpoint[TimeStamp, (K, V)] = {
    val checkpoint = Checkpoint(states.map(kv => (timestamp, kv)).toList)
    states = Map.empty[K, V]
    checkpointManager.writeCheckpoint(source, checkpoint, checkpointSerDe)
    checkpoint
  }

  def restore(timestamp: TimeStamp): Checkpoint[TimeStamp, (K, V)] = {
    val checkpoint = checkpointManager.readCheckpoint(source, checkpointSerDe)
    store.putAll(checkpoint.records.filter(_._1 == timestamp).map(_._2))
    checkpoint
  }

  override def close(): Unit = {
    checkpointManager.close()
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
}
