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

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.lib.{FetchThread, KafkaOffsetManager, KafkaMessage, KafkaConsumer}
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
      val fetchThread = mock[FetchThread]
      val messageDecoder = mock[MessageDecoder]
      val source = new KafkaSource(fetchThread, messageDecoder, Map(topicAndPartition -> offsetManager))

      when(offsetManager.resolveOffset(startTime)).thenReturn(Failure(StorageEmpty))

      source.setStartTime(startTime)
      verify(fetchThread, never()).setStartOffset(anyObject[TopicAndPartition](), anyLong())
      verify(fetchThread).start()

      doThrow(new RuntimeException).when(offsetManager).resolveOffset(startTime)
      intercept[RuntimeException] {
        source.setStartTime(startTime)
      }
    }
  }

  property("KafkaSource setStartTime should set consumer start offset if offset storage is not empty") {
    forAll(startTimeGen, offsetGen) {
      (startTime: Long, offset: Long) =>
        val offsetManager = mock[KafkaOffsetManager]
        val topicAndPartition = mock[TopicAndPartition]
        val fetchThread = mock[FetchThread]
        val messageDecoder = mock[MessageDecoder]
        val source = new KafkaSource(fetchThread, messageDecoder, Map(topicAndPartition -> offsetManager))

        when(offsetManager.resolveOffset(startTime)).thenReturn(Success(offset))

        source.setStartTime(startTime)
        verify(fetchThread).setStartOffset(topicAndPartition, offset)
        verify(fetchThread).start()

        doThrow(new RuntimeException).when(offsetManager).resolveOffset(startTime)
        intercept[RuntimeException] {
          source.setStartTime(startTime)
        }
    }
  }

  property("KafkaSource pull should return number of messages in best effort") {
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
        val messageDecoder = mock[MessageDecoder]
        val fetchThread = mock[FetchThread]
        val message = mock[Message]
        val source = new KafkaSource(fetchThread, messageDecoder,
          kafkaMsgList.map(_.topicAndPartition -> offsetManager).toMap)
        if (number == 0) {
          verify(fetchThread, never()).poll
          source.pull(number).size shouldBe 0
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
          source.pull(number).size shouldBe Math.min(number, kafkaMsgList.size)
          verify(fetchThread, times(number)).poll
        }
    }
  }

}
