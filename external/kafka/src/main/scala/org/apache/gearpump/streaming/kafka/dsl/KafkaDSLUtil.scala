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
package org.apache.gearpump.streaming.kafka.dsl

import java.util.Properties

import org.apache.gearpump.streaming.dsl.{TypedDataSource, StreamApp, Stream}
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.{DefaultMessageDecoder, KafkaConfig}
import org.apache.gearpump.streaming.source.DefaultTimeStampFilter
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}

import scala.reflect.ClassTag

object KafkaDSLUtil {
  def createStream[T: ClassTag](
      app: StreamApp,
      parallism: Int,
      description: String,
      kafkaConfig: KafkaConfig,
      messageDecoder: MessageDecoder = new DefaultMessageDecoder): Stream[T] = {
    app.source[T](new KafkaSource(kafkaConfig, messageDecoder) with TypedDataSource[T], parallism, description)
  }

  def createStream[T: ClassTag](
      app: StreamApp,
      parallism: Int,
      description: String,
      topics: String,
      zkConnect: String): Stream[T] = {
    app.source[T](new KafkaSource(topics, zkConnect) with TypedDataSource[T], parallism, description)
  }

  def createStream[T: ClassTag](
      app: StreamApp,
      parallism: Int,
      description: String,
      topics: String,
      zkConnect: String,
      messageDecoder: MessageDecoder,
      timestampFilter: TimeStampFilter): Stream[T] = {
    app.source[T](new KafkaSource(topics, zkConnect, messageDecoder, timestampFilter) with TypedDataSource[T], parallism, description)
  }

  def createStream[T: ClassTag](
      app: StreamApp,
      parallism: Int,
      description: String,
      topics: String,
      properties: Properties): Stream[T] = {
    app.source[T](new KafkaSource(topics, properties) with TypedDataSource[T], parallism, description)
  }

  def createStream[T: ClassTag](
      app: StreamApp,
      topics: String,
      parallism: Int,
      description: String,
      properties: Properties,
      messageDecoder: MessageDecoder,
      timestampFilter: TimeStampFilter): Stream[T] = {
    app.source[T](new KafkaSource(topics, properties, messageDecoder, timestampFilter) with TypedDataSource[T], parallism, description)
  }
}



