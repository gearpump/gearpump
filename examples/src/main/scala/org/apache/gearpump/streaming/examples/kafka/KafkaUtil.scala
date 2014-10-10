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

package org.apache.gearpump.streaming.examples.kafka

import org.json4s._
import org.json4s.native.Serialization

object KafkaUtil {
  implicit val formats = Serialization.formats(NoTypeHints)
  val MESSAGE_ENCODING = "UTF8"

  def longToByteArray(long: Long): Array[Byte] = {
    java.nio.ByteBuffer.allocate(8).putLong(long).array()
  }

  def byteArrayToLong(bytes: Array[Byte]): Long = {
    java.nio.ByteBuffer.wrap(bytes).getLong
  }

  def deserialize(bytes: Array[Byte]): KafkaMessage = {
    Serialization.read(new String(bytes, MESSAGE_ENCODING))
  }

  def serialize(kafkaMsg: KafkaMessage): Array[Byte] = {
    Serialization.write(kafkaMsg).getBytes(MESSAGE_ENCODING)
  }
}
