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

package org.apache.gearpump.streaming.kafka

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.lib.{KafkaOffsetManager, KafkaMessage, KafkaConsumer}
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api.MessageDecoder
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.util.{Success, Failure}

class KafkaSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val startTimeGen = Gen.choose[Long](0L, 1000L)
  val offsetGen = Gen.choose[Long](0L, 1000L)

  property("KafkaSource setStartTime should not set consumer start offset if offset storage is empty") {
    forAll(startTimeGen) { (startTime: Long) =>
      val offsetManager = mock[KafkaOffsetManager]
      val topicAndPartition = mock[TopicAndPartition]
      when(offsetManager.resolveOffset(startTime)).thenReturn(Failure(StorageEmpty))
      val consumer = mock[KafkaConsumer]
      val messageDecoder = mock[MessageDecoder]
      val source = new KafkaSource(consumer, messageDecoder, Map(topicAndPartition -> offsetManager))
      source.setStartTime(startTime)
      verify(consumer, never()).setStartOffset(anyObject[TopicAndPartition](), anyLong())
      verify(consumer).start()
    }
  }

  property("KafkaSource setStartTime should set consumer start offset if offset storage is not empty") {
    forAll(startTimeGen, offsetGen) {
      (startTime: Long, offset: Long) =>
        val offsetManager = mock[KafkaOffsetManager]
        val topicAndPartition = mock[TopicAndPartition]
        when(offsetManager.resolveOffset(startTime)).thenReturn(Success(offset))
        val consumer = mock[KafkaConsumer]
        val messageDecoder = mock[MessageDecoder]
        val source = new KafkaSource(consumer, messageDecoder, Map(topicAndPartition -> offsetManager))
        source.setStartTime(startTime)
        verify(consumer).setStartOffset(topicAndPartition, offset)
        verify(consumer).start()
    }
  }

  val numberGen = Gen.choose[Int](1, 1000)
  val kafkaMsgGen = for {
    topic <- Gen.alphaStr
    partition <- Gen.choose[Int](0, 1000)
    offset <- Gen.choose[Long](0L, 1000L)
    key = None
    msg <- Gen.alphaStr.map(Injection[String, Array[Byte]])
  } yield KafkaMessage(TopicAndPartition(topic, partition), offset, key, msg)
  property("KafkaSource pull should return number of messages in best effort") {
    forAll(numberGen, kafkaMsgGen) {
      (number: Int, kafkaMsg: KafkaMessage) =>
        val topicAndPartition = mock[TopicAndPartition]
        val message = mock[Message]
        val offsetManager = mock[KafkaOffsetManager]
        val messageDecoder = mock[MessageDecoder]
        val consumer = mock[KafkaConsumer]
        when(consumer.pollNextMessage).thenReturn(None)
        when(messageDecoder.fromBytes(kafkaMsg.msg)).thenReturn(message)
        when(offsetManager.filter(message -> kafkaMsg.offset)).thenReturn(Some(message))
        val source = new KafkaSource(consumer, messageDecoder,
          Map(topicAndPartition -> offsetManager))
        source.pull(number).size shouldBe 0
        verify(consumer, times(number)).pollNextMessage
    }
  }

}
