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

package org.apache.gearpump.streaming.kafka.lib.producer

import java.util.Properties

import org.apache.gearpump.streaming.kafka.lib.{KafkaUtil, PropertiesConfig}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer

object KafkaProducerConfig {

  def apply(): KafkaProducerConfig = {
    KafkaProducerConfig(new Properties())
  }

  def apply(filename: String): KafkaProducerConfig = {
    KafkaProducerConfig(KafkaUtil.loadProperties(filename))
  }

  def apply(properties: Properties): KafkaProducerConfig = {
    new KafkaProducerConfig(properties)
  }
}
class KafkaProducerConfig(properties: Properties) extends PropertiesConfig(properties) {

  // add default values for required keys if absent
  putIfAbsent("bootstrap.servers", "localhost:9092")
  putIfAbsent("client.id", "gearpump")

  def buildProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    new KafkaProducer[Array[Byte], Array[Byte]](properties, new ByteArraySerializer, new ByteArraySerializer)
  }
}
