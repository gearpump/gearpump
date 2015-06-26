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

package org.apache.gearpump.streaming.kafka.sink

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.lib.{KafkaConfig, KafkaUtil}
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
 * Kafka sink connectors which write data to a kafka queue
 * @param getProducer is a function to construct a KafkaProducer
 * @param topic is the kafka topic to write to
 */
class KafkaSink(getProducer: () => KafkaProducer[Array[Byte], Array[Byte]], topic: String) extends DataSink {

  def this(config: KafkaConfig) = {
    this(() => new KafkaProducer[Array[Byte], Array[Byte]](KafkaUtil.buildProducerConfig(config),
    new ByteArraySerializer, new ByteArraySerializer), config.getProducerTopic)
  }

  private lazy val producer = getProducer()

  override def open(context: TaskContext): Unit = {}

  override def write(message: Message): Unit = {
    val (key, value) = message.msg.asInstanceOf[(Array[Byte], Array[Byte])]
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
    producer.send(record)
  }

  override def close(): Unit = {
    producer.close()
  }
}

