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
package io.gearpump.streaming.kafka.dsl

import java.util.Properties

import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl
import io.gearpump.streaming.dsl.TypedDataSink
import io.gearpump.streaming.kafka.KafkaSink

import scala.reflect.ClassTag

class KafkaDSLSink[T: ClassTag](stream: dsl.Stream[T]) {
  def writeToKafka(
      topic: String,
      bootstrapServers: String,
      parallism: Int,
      description: String): dsl.Stream[T] = {
    stream.sink(new KafkaSink(topic, bootstrapServers), parallism, UserConfig.empty, description)
  }

  def writeToKafka(
      topic: String,
      properties: Properties,
      parallism: Int,
      description: String): dsl.Stream[T] = {
    stream.sink(new KafkaSink(topic, properties), parallism, UserConfig.empty, description)
  }
}

object KafkaDSLSink {
  implicit def streamToKafkaDSLSink[T: ClassTag](stream: dsl.Stream[T]): KafkaDSLSink[T] = {
    new KafkaDSLSink[T](stream)
  }
}