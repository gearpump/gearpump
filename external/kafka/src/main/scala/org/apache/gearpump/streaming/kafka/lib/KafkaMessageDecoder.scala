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
package org.apache.gearpump.streaming.kafka.lib

import java.time.Instant

import org.apache.gearpump._

/**
 * Decodes Kafka raw message of (key, value) bytes
 */
trait KafkaMessageDecoder extends java.io.Serializable {
  /**
   * @param key key of a kafka message, can be NULL
   * @param value value of a kafka message
   * @return a gearpump Message and watermark (i.e. event time progress)
   */
  def fromBytes(key: Array[Byte], value: Array[Byte]): MessageAndWatermark
}

case class MessageAndWatermark(message: Message, watermark: Instant)
