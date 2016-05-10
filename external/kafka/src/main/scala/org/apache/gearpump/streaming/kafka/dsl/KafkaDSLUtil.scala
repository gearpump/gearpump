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
package org.apache.gearpump.streaming.kafka.dsl

import java.util.Properties

import org.apache.gearpump.streaming.dsl
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.{DefaultMessageDecoder, KafkaSourceConfig}
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, OffsetStorageFactory, TimeStampFilter}

object KafkaDSLUtil {
  def createStream[T](
      app: StreamApp,
      parallelism: Int,
      description: String,
      kafkaConfig: KafkaSourceConfig,
      offsetStorageFactory: OffsetStorageFactory,
      messageDecoder: MessageDecoder = new DefaultMessageDecoder): dsl.Stream[T] = {
    app.source[T](new KafkaSource(kafkaConfig, offsetStorageFactory, messageDecoder),
      parallelism, description)
  }

  def createStream[T](
      app: StreamApp,
      parallelism: Int,
      description: String,
      topics: String,
      zkConnect: String,
      offsetStorageFactory: OffsetStorageFactory): dsl.Stream[T] = {
    app.source[T](new KafkaSource(topics, zkConnect, offsetStorageFactory),
      parallelism, description)
  }

  def createStream[T](
      app: StreamApp,
      parallelism: Int,
      description: String,
      topics: String,
      zkConnect: String,
      offsetStorageFactory: OffsetStorageFactory,
      messageDecoder: MessageDecoder,
      timestampFilter: TimeStampFilter): dsl.Stream[T] = {
    app.source[T](new KafkaSource(topics, zkConnect, offsetStorageFactory,
    messageDecoder, timestampFilter), parallelism, description)
  }

  def createStream[T](
      app: StreamApp,
      parallelism: Int,
      description: String,
      topics: String,
      properties: Properties,
      offsetStorageFactory: OffsetStorageFactory): dsl.Stream[T] = {
    app.source[T](new KafkaSource(topics, properties, offsetStorageFactory),
    parallelism, description)
  }

  def createStream[T](
      app: StreamApp,
      topics: String,
      parallelism: Int,
      description: String,
      properties: Properties,
      offsetStorageFactory: OffsetStorageFactory,
      messageDecoder: MessageDecoder,
      timestampFilter: TimeStampFilter): dsl.Stream[T] = {
    app.source[T](new KafkaSource(topics, properties, offsetStorageFactory,
    messageDecoder, timestampFilter), parallelism, description)
  }
}

