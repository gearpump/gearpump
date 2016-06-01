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

import java.util.Properties

import com.twitter.bijection.Injection
import org.apache.gearpump.streaming.kafka.lib.sink.AbstractKafkaSink.KafkaProducerFactory
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.MockUtil

class KafkaSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val dataGen = for {
    topic <- Gen.alphaStr
    key <- Gen.alphaStr
    msg <- Gen.alphaStr
  } yield (topic, Injection[String, Array[Byte]](key), Injection[String, Array[Byte]](msg))

  property("KafkaSink write should send producer record") {
    forAll(dataGen) {
      (data: (String, Array[Byte], Array[Byte])) =>
        val props = mock[Properties]
        val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
        val producerFactory = mock[KafkaProducerFactory]
        val configFactory = mock[KafkaConfigFactory]
        val config = mock[KafkaConfig]

        when(configFactory.getKafkaConfig(props)).thenReturn(config)
        when(producerFactory.getKafkaProducer(config)).thenReturn(producer)

        val (topic, key, msg) = data
        val kafkaSink = new KafkaSink(topic, props, configFactory, producerFactory)
        kafkaSink.write(Message((key, msg)))
        verify(producer).send(MockUtil.argMatch[ProducerRecord[Array[Byte], Array[Byte]]](
          r => r.topic == topic && (r.key sameElements key) && (r.value sameElements msg)))
        kafkaSink.write(Message(msg))
        verify(producer).send(MockUtil.argMatch[ProducerRecord[Array[Byte], Array[Byte]]](
          r => r.topic() == topic && (r.key == null) && (r.value() sameElements msg)
        ))
        kafkaSink.close()
    }
  }

  property("KafkaSink close should close kafka producer") {
    val props = mock[Properties]
    val producer = mock[KafkaProducer[Array[Byte], Array[Byte]]]
    val producerFactory = mock[KafkaProducerFactory]
    val configFactory = mock[KafkaConfigFactory]
    val config = mock[KafkaConfig]

    when(configFactory.getKafkaConfig(props)).thenReturn(config)
    when(producerFactory.getKafkaProducer(config)).thenReturn(producer)

    val kafkaSink = new KafkaSink("topic", props, configFactory, producerFactory)
    kafkaSink.close()
    verify(producer).close()
  }
}
