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

package org.apache.gearpump.streaming.kafka.source

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.lib.{KafkaConfig, FetchThread, KafkaMessage, KafkaOffsetManager}
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.util.{Failure, Success}

class KafkaSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val startTimeGen = Gen.choose[Long](0L, 1000L)
  val offsetGen = Gen.choose[Long](0L, 1000L)

  property("KafkaSource sets consumer to earliest offset") {
    val topicAndPartition = mock[TopicAndPartition]
    val fetchThread = mock[FetchThread]
    val offsetManager = mock[KafkaOffsetManager]
    val kafkaConfig = mock[KafkaConfig]

    val kafkaSource = new KafkaSource(kafkaConfig, Some(fetchThread), Map(topicAndPartition -> offsetManager))

    kafkaSource.setStartTime(None)

    verify(fetchThread).start()
    verify(fetchThread, never()).setStartOffset(anyObject[TopicAndPartition](), anyLong())
  }

  property("KafkaSource setStartTime should not set consumer start offset if offset storage is empty") {
    forAll(startTimeGen) { (startTime: Long) =>
      val offsetManager = mock[KafkaOffsetManager]
      val topicAndPartition = mock[TopicAndPartition]
      val fetchThread = mock[FetchThread]
      val kafkaConfig = mock[KafkaConfig]
      val source = new KafkaSource(kafkaConfig, Some(fetchThread), Map(topicAndPartition -> offsetManager))

      when(offsetManager.resolveOffset(startTime)).thenReturn(Failure(StorageEmpty))

      source.setStartTime(Some(startTime))
      verify(fetchThread, never()).setStartOffset(anyObject[TopicAndPartition](), anyLong())
      verify(fetchThread).start()

      when(offsetManager.resolveOffset(startTime)).thenReturn(Failure(new RuntimeException))
      intercept[RuntimeException] {
        source.setStartTime(Some(startTime))
      }
      source.close()
    }
  }

  property("KafkaSource setStartTime should set consumer start offset if offset storage is not empty") {
    forAll(startTimeGen, offsetGen) {
      (startTime: Long, offset: Long) =>
        val offsetManager = mock[KafkaOffsetManager]
        val topicAndPartition = mock[TopicAndPartition]
        val fetchThread = mock[FetchThread]
        val kafkaConfig = mock[KafkaConfig]
        val source = new KafkaSource(kafkaConfig, Some(fetchThread), Map(topicAndPartition -> offsetManager))

        when(offsetManager.resolveOffset(startTime)).thenReturn(Success(offset))

        source.setStartTime(Some(startTime))
        verify(fetchThread).setStartOffset(topicAndPartition, offset)
        verify(fetchThread).start()

        when(offsetManager.resolveOffset(startTime)).thenReturn(Failure(new RuntimeException))
        intercept[RuntimeException] {
          source.setStartTime(Some(startTime))
        }
        source.close()
    }
  }

  property("KafkaSource read should return number of messages in best effort") {
    val numberGen = Gen.choose[Int](0, 1000)
    val kafkaMsgGen = for {
      topic <- Gen.alphaStr
      partition <- Gen.choose[Int](0, 1000)
      offset <- Gen.choose[Long](0L, 1000L)
      key = None
      msg <- Gen.alphaStr.map(Injection[String, Array[Byte]])
    } yield KafkaMessage(TopicAndPartition(topic, partition), offset, key, msg)
    val kafkaMsgListGen = Gen.listOf[KafkaMessage](kafkaMsgGen)
    forAll(numberGen, kafkaMsgListGen) {
      (number: Int, kafkaMsgList: List[KafkaMessage]) =>
        val offsetManager = mock[KafkaOffsetManager]
        val fetchThread = mock[FetchThread]
        val message = mock[Message]
        val kafkaConfig = mock[KafkaConfig]

        val messageDecoder = mock[MessageDecoder]
        val timestampFilter = mock[TimeStampFilter]

        when(kafkaConfig.getConsumerEmitBatchSize).thenReturn(number)
        when(kafkaConfig.getMessageDecoder).thenReturn(messageDecoder)
        when(kafkaConfig.getTimeStampFilter).thenReturn(timestampFilter)

        val source = new KafkaSource(kafkaConfig, Some(fetchThread),
          kafkaMsgList.map(_.topicAndPartition -> offsetManager).toMap)
        if (number == 0) {
          verify(fetchThread, never()).poll
          source.read().size shouldBe 0
        } else {
          kafkaMsgList match {
            case Nil =>
              if (number == 1) {
                when(fetchThread.poll).thenReturn(None)
              } else {
                val nones = List.fill(number)(None)
                when(fetchThread.poll).thenReturn(nones.head, nones.tail: _*)
              }
            case list =>
              val queue = list.map(Option(_)) ++ List.fill(number - list.size)(None)
              when(fetchThread.poll).thenReturn(queue.head, queue.tail: _*)
              when(offsetManager.filter(anyObject[(Message, Long)])).thenReturn(Some(message))
          }
          source.read().size shouldBe Math.min(number, kafkaMsgList.size)
          verify(fetchThread, times(number)).poll
        }
        source.close()
    }
  }

  property("KafkaSource close should close all offset managers") {
    val offsetManager = mock[KafkaOffsetManager]
    val topicAndPartition = mock[TopicAndPartition]
    val fetchThread = mock[FetchThread]
    val kafkaConfig = mock[KafkaConfig]

    val source = new KafkaSource(kafkaConfig, Some(fetchThread), Map(topicAndPartition -> offsetManager))

    source.close()
    verify(offsetManager).close()
  }

}
