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

package org.apache.gearpump.streaming.kafka.lib.store

import java.util.Properties

import com.twitter.bijection.Injection
import kafka.api.OffsetRequest
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.kafka.lib.source.consumer.KafkaConsumer
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
 * Factory class that constructs a KafkaStore
 *
 * @param props configuration for kafka store
 */

abstract class AbstractKafkaStoreFactory(
    props: Properties,
    configFactory: KafkaConfigFactory)
  extends CheckpointStoreFactory {

  def this(props: Properties) {
    this(props, new KafkaConfigFactory)
  }

  private lazy val config: KafkaConfig = configFactory.getKafkaConfig(props)

  override def getCheckpointStore(name: String): CheckpointStore = {
    val topic = config.getKafkaStoreTopic(name)
    val client = config.getKafkaClientFactory.getKafkaClient(config)
    val replicas = config.getInt(KafkaConfig.REPLICATION_FACTOR_CONFIG)
    val topicExists = client.createTopic(topic, 1, replicas)
    val consumer = if (topicExists) {
      Some(client.createConsumer(topic, 0, OffsetRequest.EarliestTime))
    } else {
      None
    }
    val producer = client.createProducer[Array[Byte], Array[Byte]](new ByteArraySerializer,
      new ByteArraySerializer)
    new KafkaStore(topic, producer, consumer)
  }
}

object KafkaStore {
  private val LOG = LogUtil.getLogger(classOf[KafkaStore])
}

/**
 * checkpoints (timestamp, state) to kafka
 *
 * @param topic kafka store topic
 * @param producer kafka producer
 * @param optConsumer kafka consumer
 */
class KafkaStore private[kafka](
    val topic: String,
    val producer: KafkaProducer[Array[Byte], Array[Byte]],
    val optConsumer: Option[KafkaConsumer])
  extends CheckpointStore {
  import org.apache.gearpump.streaming.kafka.lib.store.KafkaStore._

  private var maxTime: TimeStamp = 0L

  override def persist(time: TimeStamp, checkpoint: Array[Byte]): Unit = {
    // make sure checkpointed timestamp is monotonically increasing
    // hence (1, 1), (3, 2), (2, 3) is checkpointed as (1, 1), (3, 2), (3, 3)
    if (time > maxTime) {
      maxTime = time
    }
    val key = maxTime
    val value = checkpoint
    val message = new ProducerRecord[Array[Byte], Array[Byte]](
      topic, 0, Injection[Long, Array[Byte]](key), value)
    producer.send(message)
    LOG.debug("KafkaStore persisted state ({}, {})", key, value)
  }

  override def recover(time: TimeStamp): Option[Array[Byte]] = {
    var checkpoint: Option[Array[Byte]] = None
    optConsumer.foreach { consumer =>
      while (consumer.hasNext && checkpoint.isEmpty) {
        val kafkaMsg = consumer.next()
        checkpoint = for {
          k <- kafkaMsg.key
          t <- Injection.invert[TimeStamp, Array[Byte]](k).toOption
          c = kafkaMsg.msg if t >= time
        } yield c
      }
      consumer.close()
    }
    checkpoint match {
      case Some(c) =>
        LOG.info(s"KafkaStore recovered checkpoint ($time, $c)")
      case None =>
        LOG.info(s"no checkpoint existing for $time")
    }
    checkpoint
  }

  override def close(): Unit = {
    producer.close()
    LOG.info("KafkaStore closed")
  }
}