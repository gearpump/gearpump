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

package org.apache.gearpump.streaming.kafka.lib.consumer

import kafka.common.TopicAndPartition

/**
 * wrapper over messages from kafka
 * @param topicAndPartition where message comes from
 * @param offset message offset on kafka queue
 * @param key message key, could be None
 * @param msg message payload
 */
case class KafkaMessage(topicAndPartition: TopicAndPartition, offset: Long,
    key: Option[Array[Byte]], msg: Array[Byte]) {

  def this(topic: String, partition: Int, offset: Long,
      key: Option[Array[Byte]], msg: Array[Byte]) = {
    this(TopicAndPartition(topic, partition), offset, key, msg)
  }
}

