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

package org.apache.gearpump.streaming.transaction.kafka

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

class KafkaProducer[K, V](config: ProducerConfig,
                    batchSize: Int) {

  private var buffer = ArrayBuffer[KeyedMessage[K, V]]()
  private val producer = new Producer[K, V](config)

  def send(topic: String, key: K, msg: V): Unit = send(topic, key, null, msg)

  def send(topic: String, key: K, partKey: Any, msg: V): Unit = {
    buffer += new KeyedMessage[K, V](topic, key, partKey, msg)
    if (buffer.size >= batchSize) {
      flush()
    }
  }

  def flush(): Unit = {
    producer.send(buffer: _*)
    buffer.clear()
  }

  def close(): Unit = {
    flush()
    producer.close()
  }
}
