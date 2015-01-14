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

package org.apache.gearpump.streaming.examples.kafka

import akka.actor.{Actor, ActorSystem}
import com.twitter.bijection.Injection
import com.typesafe.config.ConfigFactory
import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.server.{KafkaConfig => KafkaServerConfig}
import kafka.utils.{TestUtils => TestKafkaUtils, Utils, TestZKUtils}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaServerHarness
import org.scalatest.{BeforeAndAfterEach, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class KafkaStreamProcessorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterEach with KafkaServerHarness {
  val numServers = 1
  override val configs: List[KafkaServerConfig] =
    for(props <- TestKafkaUtils.createBrokerConfigs(numServers, enableControlledShutdown = false))
    yield new KafkaServerConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 1
    }

  var system1: ActorSystem = null
  var system2: ActorSystem = null

  override def beforeEach: Unit = {
    super.setUp()
    system1 = ActorSystem("KafkaStreamProcessor", TestUtil.DEFAULT_CONFIG)
    system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
  }

  override def afterEach: Unit = {
    super.tearDown()
    system1.shutdown()
    system2.shutdown()
  }

  property("KafkaStreamProcessor should write data to kafka") {
    val topic = TestKafkaUtils.tempTopic()
    val brokerList = getBrokerList
    val brokerStr = brokerList.mkString(",")
    val messageNum = 1000
    val batchSize = 100

    createTopicUntilLeaderIsElected(topic, partitions = 1, replicas = 1)

    val kafkaConfig = getKafkaConfig(topic, producerEmitBatchSize = batchSize, brokerStr, zkConnect)
    val (kafkaStreamProcessor, _) = StreamingTestUtil.createEchoForTaskActor(
      classOf[KafkaStreamProcessor].getName, kafkaConfig, system1, system2)

    val messages = 0.until(messageNum).foldLeft(List.empty[String]) { (msgs, i) =>
      val msg = s"message-$i"
      kafkaStreamProcessor.tell(Message(i.toString -> msg), kafkaStreamProcessor)
      msgs :+ msg
    }
    system1.stop(kafkaStreamProcessor)

    brokerList.foreach { broker =>
      val hostPort = broker.split(":")
      val host = hostPort(0)
      val port = hostPort(1).toInt
      val consumer = new SimpleConsumer(host, port, 1000000, 64 * 1024, "")
      val iterator = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, Int.MaxValue).build())
        .messageSet(topic, 0).iterator
      var messageList = List.empty[String]
      while(iterator.hasNext) {
        Injection.invert[String, Array[Byte]](Utils.readBytes(iterator.next().message.payload)) match {
          case Success(msg) => messageList :+= msg
          case Failure(e) => throw e
        }
      }
      messageList shouldBe messages
    }
  }

  private def getKafkaConfig(producerTopic: String, producerEmitBatchSize: Int, brokerList: String, zookeeperConnect: String): UserConfig = {
    implicit val system = system1
    UserConfig.empty.withValue[KafkaConfig](KafkaConfig.NAME, KafkaConfig(ConfigFactory.parseMap(Map(
      KafkaConfig.METADATA_BROKER_LIST -> brokerList,
      KafkaConfig.PRODUCER_EMIT_BATCH_SIZE -> producerEmitBatchSize.toString,
      KafkaConfig.PRODUCER_TOPIC -> producerTopic,
      KafkaConfig.PRODUCER_TYPE -> "sync",
      KafkaConfig.REQUEST_REQUIRED_ACKS -> "1",
      KafkaConfig.ZOOKEEPER_CONNECT -> zookeeperConnect
    ).asJava)))
  }
}
