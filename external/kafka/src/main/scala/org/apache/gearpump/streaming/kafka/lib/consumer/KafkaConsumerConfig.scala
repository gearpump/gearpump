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

package org.apache.gearpump.streaming.kafka.lib.consumer

import java.util.Properties

import kafka.consumer.ConsumerConfig
import org.apache.gearpump.streaming.kafka.lib.{KafkaUtil, PropertiesConfig}

object KafkaConsumerConfig {
  def apply(): KafkaConsumerConfig = {
    KafkaConsumerConfig(new Properties())
  }

  def apply(filename: String): KafkaConsumerConfig = {
    KafkaConsumerConfig(KafkaUtil.loadProperties(filename))
  }

  def apply(properties: Properties): KafkaConsumerConfig = {
    new KafkaConsumerConfig(properties)
  }
}

class KafkaConsumerConfig(properties: Properties) extends PropertiesConfig(properties) {

  // add default values for required keys if absent
  putIfAbsent("group.id", "")
  putIfAbsent("zookeeper.connect", "localhost:2181")

  def config: ConsumerConfig = {
    new ConsumerConfig(properties)
  }


}
