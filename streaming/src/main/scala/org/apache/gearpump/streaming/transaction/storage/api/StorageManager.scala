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
import org.apache.gearpump.util.LogUtil
import org.slf4j.{Logger, LoggerFactory}

import com.twitter.bijection._

import scala.util.{Failure, Success}

object StorageManager {

  class StoreCheckpointSerDe[K: Codec, V: Codec]
    extends CheckpointSerDe[TimeStamp, (K, V)] {

    implicit def kvInjection: Injection[(K, V), Array[Byte]] = {
      implicit val buf = Bufferable.viaInjection[(K, V),
        (Array[Byte], Array[Byte])]
      Bufferable.injectionOf[(K, V)]
    }

    override def toKeyBytes(key: TimeStamp): Array[Byte] = {
      Injection[Long, Array[Byte]](key)
    }

    override def toValueBytes(value: (K, V)): Array[Byte] = {
      Injection[(K, V), Array[Byte]](value)
    }

    override def fromKeyBytes(bytes: Array[Byte]): TimeStamp = {
      Injection.invert[Long, Array[Byte]](bytes) match {
        case Success(l) => l
        case Failure(e) => throw e
      }
    }

    override def fromValueBytes(bytes: Array[Byte]): (K, V) = {
      Injection.invert[(K, V), Array[Byte]](bytes) match {
        case Success(kv) => kv
        case Failure(e) => throw e
      }
    }
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

class StorageManager[K: Codec, V: Codec](id: String,
                           store: KeyValueStore[K, V],
                           checkpointManager: CheckpointManager[TimeStamp, (K, V)]
                           )
  extends KeyValueStore[K, V] {
  import org.apache.gearpump.streaming.transaction.storage.api.StorageManager._

  private var states: Map[K, V] = Map.empty[K, V]
  private val source : Source = new Source {
    val name: String = s"storage_$id"
    val partition: Int = 0
  }
  private val checkpointSerDe = new StoreCheckpointSerDe[K, V]()

  def start(): Unit = {
    checkpointManager.register(Array(source))
    checkpointManager.start()
  }

  def checkpoint(timestamp: TimeStamp): Checkpoint[TimeStamp, (K, V)] = {
    val checkpoint = Checkpoint(states.toList.map(kv => (timestamp, kv)))
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
