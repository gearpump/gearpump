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
package org.apache.gearpump.streaming.kafka

import java.util.Properties

import com.twitter.bijection.Injection
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.kafka.lib.source.consumer.{KafkaMessage, KafkaConsumer}
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import KafkaClient.KafkaClientFactory
import org.apache.gearpump.streaming.kafka.lib.store.KafkaStore
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.kafka.clients.producer.{Producer, ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.Serializer
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks


class KafkaStoreSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val timestampGen = Gen.chooseNum[Long](0L, 100L)

  property("KafkaStoreFactory should get KafkaStore given store name") {
    forAll(Gen.alphaStr, Gen.alphaStr, Gen.oneOf(true, false)) {
      (prefix: String, name: String, topicExists: Boolean) =>
        val props = mock[Properties]
        val config = mock[KafkaConfig]
        val configFactory = mock[KafkaConfigFactory]
        val clientFactory = mock[KafkaClientFactory]
        val client = mock[KafkaClient]
        val consumer = mock[KafkaConsumer]
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val topic = s"$prefix-$name"
        val replica = 1

        when(configFactory.getKafkaConfig(props)).thenReturn(config)
        when(config.getKafkaStoreTopic(name)).thenReturn(topic)
        when(config.getString(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG)).thenReturn(prefix)
        when(config.getInt(KafkaConfig.REPLICATION_FACTOR_CONFIG)).thenReturn(replica)
        when(config.getKafkaClientFactory).thenReturn(clientFactory)
        when(clientFactory.getKafkaClient(config)).thenReturn(client)
        when(client.createTopic(topic, 1, replica)).thenReturn(topicExists)
        if (topicExists) {
          when(client.createConsumer(topic, 0, OffsetRequest.EarliestTime)).thenReturn(consumer)
        }
        when(client.createProducer[Array[Byte], Array[Byte]](any[Serializer[Array[Byte]]],
          any[Serializer[Array[Byte]]])).thenReturn(producer)

        val storeFactory = new KafkaStoreFactory(props, configFactory)
        storeFactory.getCheckpointStore(name) shouldBe a [KafkaStore]

        if (topicExists) {
          verify(client).createConsumer(topic, 0, OffsetRequest.EarliestTime)
        }
    }
  }

  property("KafkaStore should close producer on close") {
    forAll(Gen.alphaStr) { (topic: String) =>
      val consumer = mock[KafkaConsumer]
      val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
      val kafkaStore = new KafkaStore(topic, producer, Some(consumer))
      kafkaStore.close()
      verify(producer).close()
    }
  }

  property("KafkaStore should read checkpoint from timestamp on recover") {
    forAll(Gen.alphaStr, timestampGen) {
      (topic: String, recoverTime: TimeStamp) =>
        val consumer = mock[KafkaConsumer]
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val kafkaStore = new KafkaStore(topic, producer, Some(consumer))

        // case 1: no checkpoint available
        when(consumer.hasNext).thenReturn(false)
        kafkaStore.recover(recoverTime) shouldBe None
        verify(consumer).close()
    }

    forAll(Gen.alphaStr, timestampGen) {
      (topic: String, recoverTime: TimeStamp) =>
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val kafkaStore = new KafkaStore(topic, producer, None)

        // case 2: no checkpoint store available
        kafkaStore.recover(recoverTime) shouldBe None
    }

    forAll(Gen.alphaStr, timestampGen, timestampGen) {
      (topic: String, recoverTime: TimeStamp, checkpointTime: TimeStamp) =>
        val consumer = mock[KafkaConsumer]
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val kafkaStore = new KafkaStore(topic, producer, Some(consumer))

        val key = Injection[TimeStamp, Array[Byte]](checkpointTime)
        val msg = key
        val kafkaMsg = KafkaMessage(TopicAndPartition(topic, 0), 0, Some(key), msg)

        when(consumer.hasNext).thenReturn(true, false)
        when(consumer.next()).thenReturn(kafkaMsg)

        if (checkpointTime < recoverTime) {
          // case 3: checkpointTime is less than recoverTime
          kafkaStore.recover(recoverTime) shouldBe None
        } else {
          // case 4: checkpoint time is equal to or larger than given timestamp
          kafkaStore.recover(recoverTime) shouldBe Some(msg)
        }

        verify(consumer).close()
    }
  }

  property("KafkaStore persist should write checkpoint with monotonically increasing timestamp") {
    forAll(Gen.alphaStr, timestampGen, Gen.alphaStr) {
      (topic: String, checkpointTime: TimeStamp, data: String) =>
        val consumer = mock[KafkaConsumer]
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val kafkaStore = new KafkaStore(topic, producer, Some(consumer))

        val value = Injection[String, Array[Byte]](data)
        kafkaStore.persist(checkpointTime, value)
        kafkaStore.persist(checkpointTime - 1, value)
        kafkaStore.persist(checkpointTime + 1, value)

        verifyProducer(producer, count = 2, topic, 0, checkpointTime, data)
        verifyProducer(producer, count = 1, topic, 0, checkpointTime + 1, data)

    }

    def verifyProducer(producer: Producer[Array[Byte], Array[Byte]], count: Int,
        topic: String, partition: Int, time: TimeStamp, data: String): Unit = {
      verify(producer, times(count)).send(
        MockUtil.argMatch[ProducerRecord[Array[Byte], Array[Byte]]](record =>
          record.topic() == topic
          && record.partition() == partition
          && Injection.invert[TimeStamp, Array[Byte]](record.key()).get == time
          && Injection.invert[String, Array[Byte]](record.value()).get == data
        ))
    }
  }

}
