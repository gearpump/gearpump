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

package org.apache.gearpump.streaming.kafka.sink

import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.sink.KafkaSink
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val dataGen = for {
    topic <- Gen.alphaStr
    key <- Gen.alphaStr
    msg <- Gen.alphaStr
  } yield (topic, Injection[String, Array[Byte]](key), Injection[String, Array[Byte]](msg))

  property("KafkaSink write should send producer record") {
    forAll(dataGen) {
      (data: (String, Array[Byte], Array[Byte])) =>
      val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
      val (topic, key, msg) = data
      val kafkaSink = new KafkaSink(() => producer, topic)
      val message = Message((key, msg))
      kafkaSink.write(message)
      verify(producer).send(anyObject[ProducerRecord[Array[Byte], Array[Byte]]]())
      kafkaSink.close()
    }
  }

  property("KafkaSink close should close kafka producer") {
    val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
    val kafkaSink = new KafkaSink(() => producer, "topic")
    kafkaSink.close()
    verify(producer).close()
  }
}
