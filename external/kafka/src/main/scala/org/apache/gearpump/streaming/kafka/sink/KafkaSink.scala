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

import java.util.Properties

import org.apache.gearpump.streaming.kafka.lib.{KafkaConfig, KafkaUtil}
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

class KafkaSink private[kafka](producer: KafkaProducer[Array[Byte], Array[Byte]])
  extends DataSink[ProducerRecord[Array[Byte], Array[Byte]]] {

  def this(producerConfig: Properties) =
    this(new KafkaProducer[Array[Byte], Array[Byte]](
      producerConfig,new ByteArraySerializer, new ByteArraySerializer))

  def this(config: KafkaConfig) = this(KafkaUtil.buildProducerConfig(config))

  override def write(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    producer.send(record)
  }

  override def close(): Unit = {
    producer.close()
  }
}

