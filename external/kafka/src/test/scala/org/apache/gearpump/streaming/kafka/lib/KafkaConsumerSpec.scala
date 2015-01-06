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

package org.apache.gearpump.streaming.kafka.lib

import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaConsumerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("KafkaConsumer start and close should start and stop fetch thread") {
    val fetchThread = mock[FetchThread]
    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    val kafkaConsumer = new KafkaConsumer(fetchThread, incomingQueue)
    kafkaConsumer.start()
    verify(fetchThread).start()
    kafkaConsumer.close()
    verify(fetchThread).interrupt()
  }

  val startOffsetGen = Gen.choose[Long](0L, 1000L)
  property("KafkaConsumer should set startOffset to fetch thread")  {
    forAll(startOffsetGen) { (startOffset: Long) =>
      val fetchThread = mock[FetchThread]
      val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
      val kafkaConsumer = new KafkaConsumer(fetchThread, incomingQueue)
      val topicAndPartition = mock[TopicAndPartition]

      kafkaConsumer.setStartOffset(topicAndPartition, startOffset)
      verify(fetchThread).setStartOffset(topicAndPartition, startOffset)
    }
  }

  val pollNumGen = Gen.choose[Int](1, 1000)
  property("KafkaConsumer pollNextMessage should retrieve and remove head of queue in a non-blocking way") {
    forAll(pollNumGen) { (pollNum: Int) =>
      val kafkaMsg = mock[KafkaMessage]
      val queueSizeGen = Gen.choose[Int](0, pollNum)
      val fetchThread = mock[FetchThread]
      val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
      val kafkaConsumer = new KafkaConsumer(fetchThread, incomingQueue)

      forAll(queueSizeGen) { (queueSize: Int) =>
        0.until(queueSize).foreach { _ => incomingQueue.put(kafkaMsg) }
        0.until(queueSize).foreach { _ =>
          kafkaConsumer.pollNextMessage shouldBe Some(kafkaMsg)
        }
        queueSize.until(pollNum).foreach { _ =>
          kafkaConsumer.pollNextMessage shouldBe None
        }
      }

    }
  }


}
