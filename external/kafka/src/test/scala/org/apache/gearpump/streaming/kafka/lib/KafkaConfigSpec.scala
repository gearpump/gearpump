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

import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

import java.util.{LinkedList => JLinkedList}

import scala.util.Try

class KafkaConfigSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._

  property("KafkaConfig getInt with optionally default value") {
    forAll { (key: String, value: Int, defaultValue: Int) =>
      val config = new ConfigToKafka(Map(key -> value))
      config.getInt(key) shouldBe value
      config.getInt(key, Some(defaultValue)) shouldBe value
      val newKey = key + ":"
      config.getInt(newKey, Some(defaultValue)) shouldBe defaultValue
      a [RuntimeException] should be thrownBy config.getInt(newKey)
    }
  }

  property("KafkaConfig getString with optionally default value") {
    forAll { (key: String, value: String, defaultValue: String) =>
      val config = new ConfigToKafka(Map(key -> value))
      config.getString(key) shouldBe value
      config.getString(key, Some(defaultValue)) shouldBe value
      val newKey = key + ":"
      config.getString(newKey, Some(defaultValue)) shouldBe defaultValue
      a [RuntimeException] should be thrownBy config.getString(newKey)
    }
  }

  val keyGen = Gen.alphaStr
  val listGen = Gen.listOf(Gen.alphaStr)
  property("KafkaConfig getStringList with optionally default value") {
    forAll(keyGen, listGen, listGen) { (key: String, value: List[String], defaultValue: List[String]) =>
      val jlist = new JLinkedList[String]()
      value.foreach(jlist.add)
      val defJList = new JLinkedList[String]()
      defaultValue.foreach(defJList.add)
      val config = new ConfigToKafka(Map(key -> jlist))
      config.getStringList(key) should equal (value)
      config.getStringList(key, Some(defJList)) should equal (value)
      val newKey = key + ":"
      config.getStringList(newKey, Some(defJList)) should equal (defaultValue)
      a [RuntimeException] should be thrownBy config.getStringList(newKey)
    }
  }

  property("kafka conf should be set correctly") {
    val config = KafkaConfig()
    assert(Try(config.getZookeeperConnect).isSuccess)
    assert(Try(config.getConsumerTopics).isSuccess)
    assert(Try(config.getSocketTimeoutMS).isSuccess)
    assert(Try(config.getSocketReceiveBufferBytes).isSuccess)
    assert(Try(config.getFetchMessageMaxBytes).isSuccess)
    assert(Try(config.getClientId).isSuccess)
    assert(Try(config.getConsumerEmitBatchSize).isSuccess)
    assert(Try(config.getFetchSleepMS).isSuccess)
    assert(Try(config.getFetchThreshold).isSuccess)
    assert(Try(config.getProducerTopic).isSuccess)
    assert(Try(config.getProducerEmitBatchSize).isSuccess)
    assert(Try(config.getProducerType).isSuccess)
    assert(Try(config.getSerializerClass).isSuccess)
    assert(Try(config.getRequestRequiredAcks).isSuccess)
    assert(Try(config.getMetadataBrokerList).isSuccess)
    assert(Try(config.getGrouperFactory).isSuccess)
    assert(Try(config.getStorageReplicas).isSuccess)
  }
}
