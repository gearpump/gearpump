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

package org.apache.gearpump.streaming.transaction.checkpoint.kafka

import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig._
import org.apache.gearpump.util.Configs
import org.apache.gearpump.streaming.transaction.checkpoint.api.{CheckpointManagerFactory, CheckpointManager}

class KafkaCheckpointManagerFactory extends CheckpointManagerFactory {
  override def getCheckpointManager[K, V](conf: Configs): CheckpointManager[K, V] = {
    val config = conf.config
    val checkpointId = config.getCheckpointId
    val checkpointReplicas = config.getCheckpointReplicas
    val producer = config.getProducer[Array[Byte], Array[Byte]](
      producerConfig = config.getProducerConfig(
        serializerClass = "kafka.serializer.DefaultEncoder")
    )
    val clientId = config.getClientId
    val socketTimeout = config.getSocketTimeoutMS
    val receiveBufferSize = config.getSocketReceiveBufferBytes
    val fetchSize = config.getFetchMessageMaxBytes
    val zkClient = config.getZkClient()
    new KafkaCheckpointManager(
      checkpointId, checkpointReplicas, producer, clientId,
      socketTimeout, receiveBufferSize, fetchSize, zkClient)
  }
}
