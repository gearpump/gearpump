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

import java.time.Instant
import java.util.Properties

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.kafka.lib.{MessageAndWatermark, KafkaMessageDecoder}
import org.apache.gearpump.streaming.kafka.lib.source.consumer.FetchThread.FetchThreadFactory
import org.apache.gearpump.streaming.kafka.lib.source.grouper.PartitionGrouper
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient.KafkaClientFactory
import org.apache.gearpump.streaming.kafka.lib.source.consumer.{KafkaMessage, FetchThread}
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}
import org.apache.gearpump.Message
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val startTimeGen = Gen.choose[Long](0L, 100L).map(Instant.ofEpochMilli)
  val offsetGen = Gen.choose[Long](0L, 100L)
  val topicAndPartitionGen = for {
    topic <- Gen.alphaStr suchThat (_.nonEmpty)
    partition <- Gen.chooseNum[Int](1, 100)
  } yield TopicAndPartition(topic, partition)
  val tpsGen = Gen.listOf[TopicAndPartition](topicAndPartitionGen) suchThat (_.nonEmpty)

  property("KafkaSource open should not recover without checkpoint") {
    forAll(startTimeGen, tpsGen) { (startTime: Instant, tps: List[TopicAndPartition]) =>
      val taskContext = MockUtil.mockTaskContext
      val fetchThread = mock[FetchThread]
      val kafkaClient = mock[KafkaClient]
      val clientFactory = mock[KafkaClientFactory]
      val threadFactory = mock[FetchThreadFactory]
      val topics = tps.map(_.topic)
      val properties = mock[Properties]
      val kafkaConfig = mock[KafkaConfig]
      val configFactory = mock[KafkaConfigFactory]
      val partitionGrouper = mock[PartitionGrouper]

      when(configFactory.getKafkaConfig(properties)).thenReturn(kafkaConfig)
      when(clientFactory.getKafkaClient(kafkaConfig)).thenReturn(kafkaClient)
      val tpsArray = tps.toArray
      when(kafkaClient.getTopicAndPartitions(topics)).thenReturn(tpsArray)
      when(kafkaConfig.getConfiguredInstance(KafkaConfig.PARTITION_GROUPER_CLASS_CONFIG,
        classOf[PartitionGrouper])).thenReturn(partitionGrouper)
      when(partitionGrouper.group(taskContext.parallelism, taskContext.taskId.index, tpsArray))
        .thenReturn(tpsArray)
      when(threadFactory.getFetchThread(kafkaConfig, kafkaClient)).thenReturn(fetchThread)

      val source = new KafkaSource(topics.mkString(","), properties, configFactory,
        clientFactory, threadFactory)

      source.open(taskContext, startTime)

      verify(fetchThread, never()).setStartOffset(anyObject[TopicAndPartition](), anyLong())
    }
  }

  property("KafkaSource open should recover with checkpoint") {
    forAll(startTimeGen, offsetGen, tpsGen) {
      (startTime: Instant, offset: Long, tps: List[TopicAndPartition]) =>
        val taskContext = MockUtil.mockTaskContext
        val checkpointStoreFactory = mock[CheckpointStoreFactory]
        val checkpointStores = tps.map(_ -> mock[CheckpointStore]).toMap
        val topics = tps.map(_.topic)
        val properties = mock[Properties]
        val kafkaConfig = mock[KafkaConfig]
        val configFactory = mock[KafkaConfigFactory]
        val kafkaClient = mock[KafkaClient]
        val clientFactory = mock[KafkaClientFactory]
        val fetchThread = mock[FetchThread]
        val threadFactory = mock[FetchThreadFactory]
        val partitionGrouper = mock[PartitionGrouper]

        when(configFactory.getKafkaConfig(properties)).thenReturn(kafkaConfig)
        when(clientFactory.getKafkaClient(kafkaConfig)).thenReturn(kafkaClient)
        when(threadFactory.getFetchThread(kafkaConfig, kafkaClient)).thenReturn(fetchThread)
        val tpsArray = tps.toArray
        when(kafkaClient.getTopicAndPartitions(topics)).thenReturn(tps.toArray)
        when(kafkaConfig.getConfiguredInstance(KafkaConfig.PARTITION_GROUPER_CLASS_CONFIG,
          classOf[PartitionGrouper])).thenReturn(partitionGrouper)
        when(partitionGrouper.group(taskContext.parallelism, taskContext.taskId.index, tpsArray))
          .thenReturn(tpsArray)

        val source = new KafkaSource(topics.mkString(","), properties, configFactory,
          clientFactory, threadFactory)
        checkpointStores.foreach{ case (tp, store) => source.addPartitionAndStore(tp, store) }

        checkpointStores.foreach { case (tp, store) =>
          when(checkpointStoreFactory.getCheckpointStore(
            KafkaConfig.getCheckpointStoreNameSuffix(tp))).thenReturn(store)
          when(store.recover(startTime.toEpochMilli))
            .thenReturn(Some(Injection[Long, Array[Byte]](offset)))
        }

        source.setCheckpointStore(checkpointStoreFactory)
        source.open(taskContext, startTime)

        tps.foreach(tp => verify(fetchThread).setStartOffset(tp, offset))
    }
  }

  property("KafkaSource read checkpoints offset and returns a message or null") {
    val msgGen = for {
      tp <- topicAndPartitionGen
      offset <- Gen.choose[Long](0L, 1000L)
      key = Some(Injection[Long, Array[Byte]](offset))
      msg <- Gen.alphaStr.map(Injection[String, Array[Byte]])
    } yield KafkaMessage(tp, offset, key, msg)
    val msgQueueGen = Gen.listOf[KafkaMessage](msgGen)

    forAll(msgQueueGen) { (msgQueue: List[KafkaMessage]) =>
      val properties = mock[Properties]
      val config = mock[KafkaConfig]
      val configFactory = mock[KafkaConfigFactory]
      val messageDecoder = mock[KafkaMessageDecoder]
      val kafkaClient = mock[KafkaClient]
      val clientFactory = mock[KafkaClientFactory]
      val fetchThread = mock[FetchThread]
      val threadFactory = mock[FetchThreadFactory]
      val checkpointStoreFactory = mock[CheckpointStoreFactory]

      val checkpointStores = msgQueue.map(_.topicAndPartition -> mock[CheckpointStore]).toMap
      val topics = checkpointStores.map(_._1.topic).mkString(",")

      checkpointStores.foreach { case (tp, store) =>
        when(checkpointStoreFactory.getCheckpointStore(KafkaConfig
          .getCheckpointStoreNameSuffix(tp))).thenReturn(store)
      }
      when(configFactory.getKafkaConfig(properties)).thenReturn(config)
      when(config.getConfiguredInstance(KafkaConfig.MESSAGE_DECODER_CLASS_CONFIG,
        classOf[KafkaMessageDecoder])).thenReturn(messageDecoder)
      when(clientFactory.getKafkaClient(config)).thenReturn(kafkaClient)
      when(threadFactory.getFetchThread(config, kafkaClient)).thenReturn(fetchThread)

      val source = new KafkaSource(topics, properties, configFactory, clientFactory, threadFactory)
      checkpointStores.foreach{ case (tp, store) => source.addPartitionAndStore(tp, store) }
      source.setCheckpointStore(checkpointStoreFactory)

      if (msgQueue.isEmpty) {
        when(fetchThread.poll).thenReturn(None)
        source.read() shouldBe null
      } else {
        msgQueue.foreach { kafkaMsg =>
          when(fetchThread.poll).thenReturn(Option(kafkaMsg))
          val message = Message(kafkaMsg.msg, kafkaMsg.offset)
          val msgAndWmk = MessageAndWatermark(message, Instant.ofEpochMilli(kafkaMsg.offset))
          when(messageDecoder.fromBytes(kafkaMsg.key.get, kafkaMsg.msg)).thenReturn(msgAndWmk)
          source.read() shouldBe Message(kafkaMsg.msg, kafkaMsg.offset)
          verify(checkpointStores(kafkaMsg.topicAndPartition)).persist(
            kafkaMsg.offset, Injection[Long, Array[Byte]](kafkaMsg.offset))
        }
      }
    }
  }

  property("KafkaSource close should close all checkpoint stores") {
    forAll(Gen.chooseNum[Int](1, 100)) { (num: Int) =>
      val tps = 0.until(num).map(id => TopicAndPartition("topic", id))
      val checkpointStores = tps.map(_ -> mock[CheckpointStore]).toMap
      val props = mock[Properties]
      val kafkaConfig = mock[KafkaConfig]
      val configFactory = mock[KafkaConfigFactory]
      val threadFactory = mock[FetchThreadFactory]
      val kafkaClient = mock[KafkaClient]
      val clientFactory = mock[KafkaClientFactory]

      when(configFactory.getKafkaConfig(props)).thenReturn(kafkaConfig)
      when(clientFactory.getKafkaClient(kafkaConfig)).thenReturn(kafkaClient)

      val source = new KafkaSource("topic", props, configFactory, clientFactory, threadFactory)
      checkpointStores.foreach{ case (tp, store) => source.addPartitionAndStore(tp, store) }
      source.close()

      verify(kafkaClient).close()
      checkpointStores.foreach{ case (_, store) => verify(store).close() }
    }
  }
}
