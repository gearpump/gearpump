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

import io.gearpump.streaming.dsl
import io.gearpump.streaming.dsl.{StreamApp, TimeExtractor, TypedDataSource}
import io.gearpump.streaming.kafka.KafkaSource
import io.gearpump.streaming.task.TaskContext
import io.gearpump.streaming.transaction.api.{MessageDecoder, OffsetStorageFactory, TimeStampFilter}
import io.gearpump.{Message, TimeStamp}

object KafkaDSLUtil {
  /**
   * creates a stream of raw bytes message from kafka
   * @param app stream application
   * @param parallelism number of run-time instances for source
   * @param description additional info for source
   * @param topics comma-separated string of topics
   * @param zkConnect kafka consumer config `zookeeper.connect`
   * @param offsetStorageFactory [[OffsetStorageFactory]] that creates [[io.gearpump.streaming.transaction.api.OffsetStorage]]
   * @param timeExtractor extracts timestamp from kafka message
   * @param timestampFilter filters out message based on timestamp
   * @return a stream of bytes from kafka
   */

  def createStream(
      app: StreamApp,
      parallelism: Int,
      description: String,
      topics: String,
      zkConnect: String,
      offsetStorageFactory: OffsetStorageFactory,
      timeExtractor: TimeExtractor[Array[Byte]],
      timestampFilter: TimeStampFilter): dsl.Stream[Array[Byte]] = {
    val source = new TypedKafkaSource(topics, zkConnect, offsetStorageFactory, timestampFilter)
    source.setTimeExtractor(timeExtractor)
    app.source[Array[Byte]](source, parallelism, description)
  }

  class TypedKafkaSource(
      topics: String,
      zkConnect: String,
      offsetStorageFactory: OffsetStorageFactory,
      timeStampFilter: TimeStampFilter)
    extends TypedDataSource[Array[Byte]] {

    var timeExtractor: TimeExtractor[Array[Byte]] = timeExtractor
    var kafkaSource: KafkaSource = null

    override def setTimeExtractor(extractor: TimeExtractor[Array[Byte]]): Unit = {
      this.timeExtractor = extractor
    }

    override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
      if (null == kafkaSource) {
        val messageDecoder = new MessageDecoder {
          override def fromBytes(bytes: Array[Byte]): Message = {
            Message(bytes, timeExtractor(bytes))
          }
        }
        kafkaSource = new KafkaSource(topics, zkConnect, offsetStorageFactory, messageDecoder, timeStampFilter)
      }
      kafkaSource.open(context, startTime)
    }

    override def close(): Unit = {
      if (kafkaSource != null) {
        kafkaSource.close()
        kafkaSource = null
      }
    }

    override def read(batchSize: Int): List[Message] = {
      if (kafkaSource != null) {
        kafkaSource.read(batchSize)
      } else {
        List.empty[Message]
      }
    }
  }

}



