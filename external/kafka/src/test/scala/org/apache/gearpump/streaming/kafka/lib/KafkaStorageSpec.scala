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

import java.nio.ByteBuffer

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import kafka.producer.{KeyedMessage, Producer}
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.{Overflow, StorageEmpty, Underflow}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.util.{Try, Failure, Success}

class KafkaStorageSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  val minTimeGen = Gen.choose[Long](1L, 500L)
  val maxTimeGen = Gen.choose[Long](500L, 999L)

  property("KafkaStorage lookup time should report StorageEmpty if storage is empty") {
    forAll { (time: Long, topic: String) =>
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val getConsumer = () => mock[KafkaConsumer]
      val storage = new KafkaStorage(topic, topicExists = false, producer, getConsumer())
      storage.lookUp(time) shouldBe Failure(StorageEmpty)
    }
  }

  property("KafkaStorage lookup time should return data or report failure if storage is not empty") {
    forAll(minTimeGen, maxTimeGen, Gen.alphaStr) { (minTime: Long, maxTime: Long, topic: String) =>
      val timeAndOffsets = minTime.to(maxTime).zipWithIndex.map { case (time, index) =>
        val offset = index.toLong
        time -> offset
      }
      val timeAndOffsetsMap = timeAndOffsets.toMap
      val data = timeAndOffsets.map {
        case (time, offset) =>
          new KafkaMessage(topic, 0, offset.toLong, Some(Injection[Long, Array[Byte]](time)),
            Injection[Long, Array[Byte]](offset))
      }.toList

      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val consumer = mock[KafkaConsumer]
      val getConsumer = () => consumer

      val hasNexts = List.fill(data.tail.size)(true) :+ false
      when(consumer.hasNext).thenReturn(true, hasNexts:_*)
      when(consumer.next).thenReturn(data.head, data.tail:_*)

      val storage = new KafkaStorage(topic, topicExists = true, producer, getConsumer())
      forAll(Gen.choose[Long](minTime, maxTime)) {
        time =>
          storage.lookUp(time) match {
            case Success(array) => array should equal (Injection[Long, Array[Byte]](timeAndOffsetsMap(time)))
            case Failure(e)     => fail("time in range should return Success with value")
          }
      }

      forAll(Gen.choose[Long](0L, minTime - 1)) {
        time =>
          storage.lookUp(time) match {
            case Failure(e) => e shouldBe a [Underflow]
              e.asInstanceOf[Underflow].min should equal (Injection[Long, Array[Byte]](timeAndOffsetsMap(minTime)))
            case Success(_) => fail("time less than min should return Underflow failure")
          }
      }

      forAll(Gen.choose[Long](maxTime + 1, 1000L)) {
        time =>
          storage.lookUp(time) match {
            case Failure(e) => e shouldBe a [Overflow]
              e.asInstanceOf[Overflow].max should equal (Injection[Long, Array[Byte]](timeAndOffsetsMap(maxTime)))
            case Success(_) => fail("time larger than max should return Overflow failure")
          }
      }
    }
  }

  property("KafkaStorage append should send data to Kafka") {
    forAll { (time: Long, offset: Long, topic: String, topicExists: Boolean) =>
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val getConsumer = () => mock[KafkaConsumer]
      val storage = new KafkaStorage(topic, topicExists, producer, getConsumer())
      val offsetBytes = Injection[Long, Array[Byte]](offset)
      storage.append(time, offsetBytes)
      verify(producer).send(anyObject[KeyedMessage[Array[Byte], Array[Byte]]]())
    }
  }

  val topicAndPartitionGen = for {
    topic <- Gen.alphaStr
    partition <- Gen.choose[Int](0, 100)
  } yield TopicAndPartition(topic, partition)
  property("KafkaStorage should load data from Kafka") {
    val kafkaMsgGen = for {
      timestamp <- Gen.choose[Long](1L, 1000L)
      offset    <- Gen.choose[Long](0L, 1000L)
    } yield (timestamp, Injection[Long, Array[Byte]](offset))
    val msgListGen = Gen.listOf[(Long, Array[Byte])](kafkaMsgGen)

    val topicExistsGen = Gen.oneOf(true, false)

    forAll(topicAndPartitionGen, msgListGen) {
      (topicAndPartition: TopicAndPartition, msgList: List[(Long, Array[Byte])]) =>
        val producer=  mock[Producer[Array[Byte], Array[Byte]]]
        val consumer = mock[KafkaConsumer]
        val getConsumer = () => consumer
        val kafkaStorage = new KafkaStorage(topicAndPartition.topic, topicExists = true, producer, getConsumer())
          msgList match {
            case Nil =>
              when(consumer.hasNext).thenReturn(false)
            case list =>
              val hasNexts = List.fill(list.tail.size)(true) :+ false
              val kafkaMsgList = list.zipWithIndex.map { case ((timestamp, bytes), index) =>
                KafkaMessage(topicAndPartition, index.toLong, Some(Injection[Long, Array[Byte]](timestamp)), bytes)
              }
              when(consumer.hasNext).thenReturn(true, hasNexts: _*)
              when(consumer.next).thenReturn(kafkaMsgList.head, kafkaMsgList.tail: _*)
          }
          kafkaStorage.load(consumer) shouldBe msgList
    }
  }

  property("KafkaStorage should get consumer when topic doesn't exist") {
    forAll(Gen.alphaStr) { (topic: String) =>
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val getConsumer = mock[() => KafkaConsumer]
      val kafkaStorage = new KafkaStorage(topic, topicExists = false, producer, getConsumer())
      verify(getConsumer, never()).apply()
    }
  }

  property("KafkaStorage should fail to load invalid KafkaMessage") {
    val invalidKafkaMsgGen = for {
      tp <- topicAndPartitionGen
      offset <- Gen.choose[Long](1L, 1000L)
      timestamp <- Gen.oneOf(Some(Injection[ByteBuffer, Array[Byte]](ByteBuffer.allocate(0))), None)
      msg <- Gen.alphaStr.map(Injection[String, Array[Byte]])
    } yield KafkaMessage(tp, offset, timestamp, msg)
    forAll(invalidKafkaMsgGen) { (invalidKafkaMsg: KafkaMessage) =>
      val consumer = mock[KafkaConsumer]
      val getConsumer = () => consumer
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val kafkaStorage = new KafkaStorage(invalidKafkaMsg.topicAndPartition.topic, topicExists = true,
        producer, getConsumer())
      when(consumer.hasNext).thenReturn(true, false)
      when(consumer.next).thenReturn(invalidKafkaMsg, invalidKafkaMsg)
      Try(kafkaStorage.load(consumer)).isFailure shouldBe true
    }
  }

  property("KafkaStorage close should close kafka producer") {
    val producer = mock[Producer[Array[Byte], Array[Byte]]]
    val getConsumer = mock[() => KafkaConsumer]
    val kafkaStorage = new KafkaStorage("topic", false, producer, getConsumer())
    kafkaStorage.close()
    verify(producer).close()
  }
}
