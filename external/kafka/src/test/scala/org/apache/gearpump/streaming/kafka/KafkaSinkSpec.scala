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
import kafka.producer.{KeyedMessage, Producer}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val batchSizeGen = Gen.choose[Int](2, 1000)
  val iterationGen = Gen.choose[Int](1, 1000)
  val dataGen = for {
    topic <- Gen.alphaStr
    key <- Gen.alphaStr
    msg <- Gen.alphaStr
  } yield (topic, Injection[String, Array[Byte]](key), Injection[String, Array[Byte]](msg))

  property("KafkaProducer should send data in batch") {
    forAll(batchSizeGen, iterationGen, dataGen) {
      (batchSize: Int, iteration: Int, data: (String, Array[Byte], Array[Byte])) =>
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val (topic, key, msg) = data
      val kafkaSink = new KafkaSink(producer, batchSize)
      0.until(batchSize * iteration) foreach { _ =>
        kafkaSink.write(topic, key, msg)
      }
      verify(producer, times(iteration)).send(anyObject[KeyedMessage[Array[Byte], Array[Byte]]]())
    }
  }

  property("KafkaProducer close should flush data") {
    forAll(batchSizeGen, dataGen) { (batchSize: Int, data: (String, Array[Byte], Array[Byte])) =>
      val (topic, key, msg) = data
      forAll(Gen.choose[Int](1, batchSize - 1)) { (sendTimes: Int) =>
        val producer = mock[Producer[Array[Byte], Array[Byte]]]
        val kafkaSink = new KafkaSink(producer, batchSize)
        0.until(sendTimes) foreach { _ =>
          kafkaSink.write(topic, key, msg)
        }
        kafkaSink.close()
        verify(producer).send(anyObject[KeyedMessage[Array[Byte], Array[Byte]]]())
      }
    }
  }

  property("KafkaProducer flush should send out data") {
    forAll(batchSizeGen, dataGen) { (batchSize: Int, data: (String, Array[Byte], Array[Byte])) =>
      val producer = mock[Producer[Array[Byte], Array[Byte]]]
      val (topic, key, msg) = data
      val kafkaSink = new KafkaSink(producer, batchSize)
      kafkaSink.flush()
      verify(producer, never()).send(anyObject[KeyedMessage[Array[Byte], Array[Byte]]])

      kafkaSink.write(topic, key, msg)
      kafkaSink.flush()
      verify(producer).send(anyObject[KeyedMessage[Array[Byte], Array[Byte]]])
    }
  }
}
