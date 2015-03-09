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

import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.transaction.api.OffsetStorage
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.{Overflow, StorageEmpty, Underflow}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.util.{Failure, Success}

class KafkaOffsetManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val timeStampGen = Gen.choose[Long](0L, 1000L)
  val messageGen = for {
    msg <- Gen.alphaStr
    time <- timeStampGen
  } yield Message(msg, time)

  val messageAndOffsetsGen = Gen.listOf[Message](messageGen).map(_.zipWithIndex)

  property("KafkaOffsetManager should append offset to storage in monotonically increasing time order") {
    forAll(messageAndOffsetsGen) { (messageAndOffsets: List[(Message, Int)]) =>
      val offsetStorage = mock[OffsetStorage]
      val offsetManager = new KafkaOffsetManager(offsetStorage)
      messageAndOffsets.foldLeft(0L){ (max, messageAndOffset) =>
        val (message, offset) = messageAndOffset
        offsetManager.filter((message, offset.toLong)) shouldBe Option(message)
        if (message.timestamp > max) {
          val newMax = message.timestamp
          verify(offsetStorage).append(newMax, Injection[Long, Array[Byte]](offset.toLong))
          newMax
        } else {
          verifyZeroInteractions(offsetStorage)
          max
        }
      }
      offsetManager.close()
    }
  }

  val minTimeStampGen = Gen.choose[Long](0L, 500L)
  val maxTimeStampGen = Gen.choose[Long](500L, 1000L)
  property("KafkaOffsetManager resolveOffset should report StorageEmpty failure when storage is empty") {
    forAll(timeStampGen) { (time: Long) =>
      val offsetStorage = mock[OffsetStorage]
      val offsetManager = new KafkaOffsetManager(offsetStorage)
      when(offsetStorage.lookUp(time)).thenReturn(Failure(StorageEmpty))
      offsetManager.resolveOffset(time) shouldBe Failure(StorageEmpty)

      doThrow(new RuntimeException).when(offsetStorage).lookUp(time)
      intercept[RuntimeException] {
        offsetManager.resolveOffset(time)
      }
      offsetManager.close()
    }
  }

  val offsetGen = Gen.choose[Long](0L, 1000L)
  property("KafkaOffsetManager resolveOffset should return a valid offset when storage is not empty") {
    forAll(timeStampGen, minTimeStampGen, maxTimeStampGen, offsetGen) {
      (time: Long, min: Long, max: Long, offset: Long) =>
        val offsetStorage = mock[OffsetStorage]
        val offsetManager = new KafkaOffsetManager(offsetStorage)
        if (time < min) {
          when(offsetStorage.lookUp(time)).thenReturn(Failure(Underflow(Injection[Long, Array[Byte]](min))))
          offsetManager.resolveOffset(time) shouldBe Success(min)
        } else if (time > max) {
          when(offsetStorage.lookUp(time)).thenReturn(Failure(Overflow(Injection[Long, Array[Byte]](max))))
          offsetManager.resolveOffset(time) shouldBe Success(max)
        } else {
          when(offsetStorage.lookUp(time)).thenReturn(Success(Injection[Long, Array[Byte]](offset)))
          offsetManager.resolveOffset(time) shouldBe Success(offset)
        }

        doThrow(new RuntimeException).when(offsetStorage).lookUp(time)
        intercept[RuntimeException] {
          offsetManager.resolveOffset(time)
        }
        offsetManager.close()
    }
  }

  property("KafkaOffsetManager close should close offset storage") {
    val offsetStorage = mock[OffsetStorage]
    val offsetManager = new KafkaOffsetManager(offsetStorage)
    offsetManager.close()
    verify(offsetStorage).close()
  }
}
