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

package org.apache.gearpump.streaming.kafka

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.gearpump.streaming.kafka.lib.{KafkaUtil, KafkaConfig}

import scala.collection.mutable.ArrayBuffer

class KafkaSink private[kafka](producer: Producer[Array[Byte], Array[Byte]], batchSize: Int) {

  private var buffer = ArrayBuffer[KeyedMessage[Array[Byte], Array[Byte]]]()

  def this(producerConfig: ProducerConfig, batchSize: Int) =
    this(new Producer[Array[Byte], Array[Byte]](producerConfig), batchSize)

  def this(config: KafkaConfig) = this(KafkaUtil.buildProducerConfig(config), config.getProducerEmitBatchSize)

  def write(topic: String, key: Array[Byte], msg: Array[Byte]): Unit = write(topic, key, key, msg)

  def write(topic: String, key: Array[Byte], partKey: Any, msg: Array[Byte]): Unit = {
    buffer += new KeyedMessage(topic, key, partKey, msg)
    if (buffer.size >= batchSize) {
      flush()
    }
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      producer.send(buffer: _*)
      buffer.clear()
    }
  }

  def close(): Unit = {
    flush()
    producer.close()
  }
}
