/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.kafka;

import org.apache.gearpump.streaming.kafka.lib.sink.AbstractKafkaSink;
import org.apache.gearpump.streaming.kafka.util.KafkaConfig;
import org.apache.gearpump.streaming.sink.DataSink;

import java.util.Properties;

/**
 * USER API for kafka sink connector.
 * Please refer to {@link AbstractKafkaSink} for detailed descriptions and implementations.
 */
public class KafkaSink extends AbstractKafkaSink implements DataSink {

  public KafkaSink(String topic, Properties props) {
    super(topic, props);
  }

  KafkaSink(String topic, Properties props,
      KafkaConfig.KafkaConfigFactory kafkaConfigFactory,
      KafkaProducerFactory factory) {
    super(topic, props, kafkaConfigFactory, factory);
  }
}
