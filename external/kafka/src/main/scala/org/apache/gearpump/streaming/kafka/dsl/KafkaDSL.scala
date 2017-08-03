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

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.scalaapi.{Stream, StreamApp}
import org.apache.gearpump.streaming.kafka.{KafkaSink, KafkaSource}
import org.apache.gearpump.streaming.transaction.api.CheckpointStoreFactory

object KafkaDSL {

  /**
   * Creates stream from Kafka where Kafka offsets are not checkpointed
   * @param app stream application
   * @param topics Kafka source topics
   * @param properties Kafka configurations
   * @param parallelism number of source tasks
   * @param config task configurations
   * @param description descriptions to mark source on dashboard
   * @return a stream reading data from Kafka
   */
  def createAtMostOnceStream[T](
      app: StreamApp,
      topics: String,
      properties: Properties,
      parallelism: Int = 1,
      config: UserConfig = UserConfig.empty,
      description: String = "KafkaSource"
      ): Stream[T] = {
    app.source[T](new KafkaSource(topics, properties), parallelism, config, description)
  }

  /**
   * Creates stream from Kafka where Kafka offsets are checkpointed with timestamp
   * @param app stream application
   * @param topics Kafka source topics
   * @param checkpointStoreFactory factory to build checkpoint store
   * @param properties Kafka configurations
   * @param parallelism number of source tasks
   * @param config task configurations
   * @param description descriptions to mark source on dashboard
   * @return a stream reading data from Kafka
   */
  def createAtLeastOnceStream[T](
      app: StreamApp,
      topics: String,
      checkpointStoreFactory: CheckpointStoreFactory,
      properties: Properties,
      parallelism: Int = 1,
      config: UserConfig = UserConfig.empty,
      description: String = "KafkaSource"): Stream[T] = {
    val source = new KafkaSource(topics, properties)
    source.setCheckpointStore(checkpointStoreFactory)
    app.source[T](source, parallelism, config, description)
  }

  import scala.language.implicitConversions
  implicit def streamToKafkaDSL[T](stream: Stream[T]): KafkaDSL[T] = {
    new KafkaDSL[T](stream)
  }
}

class KafkaDSL[T](stream: Stream[T]) {

  /**
   * Sinks data to Kafka
   * @param topic Kafka sink topic
   * @param properties Kafka configurations
   * @param parallelism number of sink tasks
   * @param userConfig task configurations
   * @param description descriptions to mark sink on dashboard
   * @return a stream writing data to Kafka
   */
  def writeToKafka(
      topic: String,
      properties: Properties,
      parallelism: Int = 1,
      userConfig: UserConfig = UserConfig.empty,
      description: String = "KafkaSink"): Stream[T] = {
    stream.sink(new KafkaSink(topic, properties), parallelism, userConfig, description)
  }
}