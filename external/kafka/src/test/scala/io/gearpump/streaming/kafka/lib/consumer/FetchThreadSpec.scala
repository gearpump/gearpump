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

package io.gearpump.streaming.kafka.lib.consumer

import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class FetchThreadSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val nonNegativeGen = Gen.choose[Int](0, 1000)
  val positiveGen = Gen.choose[Int](1, 1000)
  val startOffsetGen = Gen.choose[Long](0L, 1000L)
  property("FetchThread should set startOffset to iterators") {
    forAll(nonNegativeGen, nonNegativeGen, startOffsetGen) {
      (fetchThreshold: Int, fetchSleepMS: Int, startOffset: Long) =>
      val topicAndPartition = mock[TopicAndPartition]
      val consumer = mock[KafkaConsumer]
      val createConsumer =  (tp: TopicAndPartition) => consumer
      val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
      val fetchThread = new FetchThread(Array(topicAndPartition), createConsumer,
        incomingQueue, fetchThreshold, fetchSleepMS)
      fetchThread.setStartOffset(topicAndPartition, startOffset)
      verify(consumer).setStartOffset(startOffset)
    }
  }

  val topicAndPartitionGen = for {
    topic <- Gen.alphaStr
    partition <- Gen.choose[Int](0, Int.MaxValue)
  } yield TopicAndPartition(topic, partition)
  property("FetchThread should only fetchMessage when the number of messages in queue is below the threshold") {
    forAll(positiveGen, nonNegativeGen, nonNegativeGen, startOffsetGen, topicAndPartitionGen) {
      (messageNum: Int, fetchThreshold: Int, fetchSleepMS: Int,
       startOffset: Long, topicAndPartition: TopicAndPartition) =>
        val message = mock[KafkaMessage]
        val consumer = mock[KafkaConsumer]
        val createConsumer = (tp: TopicAndPartition) => consumer
        when(consumer.hasNext).thenReturn(true)
        when(consumer.next).thenReturn(message)
        val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
        val fetchThread = new FetchThread(Array(topicAndPartition), createConsumer,
          incomingQueue, fetchThreshold, fetchSleepMS)

        0.until(messageNum) foreach { _ =>
          fetchThread.fetchMessage
        }

        incomingQueue.size() shouldBe Math.min(messageNum, fetchThreshold)
    }
  }

  property("FetchThread poll should try to retrieve and remove the head of incoming queue") {
    val topicAndPartition = mock[TopicAndPartition]
    val consumer = mock[KafkaConsumer]
    val createConsumer = (tp: TopicAndPartition) => consumer
    val kafkaMsg = mock[KafkaMessage]
    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    incomingQueue.put(kafkaMsg)
    val fetchThread = new FetchThread(Array(topicAndPartition), createConsumer, incomingQueue, 0, 0)
    fetchThread.poll shouldBe Some(kafkaMsg)
    fetchThread.poll shouldBe None
  }

  val tpAndHasNextGen = for {
    tp <- topicAndPartitionGen
    hasNext <- Gen.oneOf(true, false)
  } yield (tp, hasNext)
  val tpHasNextMapGen = Gen.listOf[(TopicAndPartition, Boolean)](tpAndHasNextGen).map(_.toMap) suchThat (_.nonEmpty)
  property("FetchThread fetchMessage should return false when there are no more messages from any TopicAndPartition") {
    forAll(tpHasNextMapGen, nonNegativeGen) {
      (tpHasNextMap: Map[TopicAndPartition, Boolean], fetchSleepMS: Int) =>
        val createConsumer = (tp: TopicAndPartition) => {
          val consumer = mock[KafkaConsumer]
          val kafkaMsg = mock[KafkaMessage]
          val hasNext = tpHasNextMap(tp)
          when(consumer.hasNext).thenReturn(hasNext)
          when(consumer.next).thenReturn(kafkaMsg)
          consumer
        }
        val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
        val fetchThread = new FetchThread(tpHasNextMap.keys.toArray,
          createConsumer, incomingQueue, tpHasNextMap.size + 1, fetchSleepMS)
        fetchThread.fetchMessage shouldBe tpHasNextMap.values.reduce(_ || _)
    }
  }
}
