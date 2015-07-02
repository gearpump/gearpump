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

import org.apache.gearpump.streaming.kafka.lib.producer.KafkaProducerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaSink private[kafka](producer: KafkaProducer[Array[Byte], Array[Byte]]) {

  def this(config: KafkaProducerConfig) = this(config.buildProducer)

  def write(topic: String, key: Array[Byte], msg: Array[Byte]): Unit = {
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, key, msg))
  }

  def write(topic: String, partition: Int, key: Array[Byte], msg: Array[Byte]): Unit = {
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, key, msg))
  }

  def close(): Unit = {
    producer.close()
  }
}
