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

import akka.actor.ActorSystem
import com.twitter.bijection.Injection
import com.typesafe.config.ConfigFactory
import kafka.server.{KafkaConfig => KafkaServerConfig}
import kafka.utils.{TestUtils => TestKafkaUtils, TestZKUtils}
import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaDefaultGrouperFactory
import org.apache.gearpump.streaming.kafka.util.KafkaServerHarness
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, BeforeAndAfterEach, PropSpec}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class KafkaStreamProducerSpec extends PropSpec with Matchers with BeforeAndAfterEach with KafkaServerHarness {
  val numServers = 1
  override val configs: List[KafkaServerConfig] =
    for(props <- TestKafkaUtils.createBrokerConfigs(numServers, enableControlledShutdown = false))
    yield new KafkaServerConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 4
    }

  var system1: ActorSystem = null
  var system2: ActorSystem = null

  override def beforeEach: Unit = {
    super.setUp()
    system1 = ActorSystem("KafkaStreamProducer", TestUtil.DEFAULT_CONFIG)
    system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)

  }

  override def afterEach: Unit = {
    super.tearDown()
    system1.shutdown()
    system2.shutdown()
  }

  property("KafkaStreamProducer should pull messages from kafka") {
    val messageNum = 1000
    val batchSize = 100
    val topic = TestKafkaUtils.tempTopic()
    val brokerList = getBrokerList.mkString(",")
    val partitionsToBrokers = createTopicUntilLeaderIsElected(topic, partitions = 1, replicas = 1)
    val messages = partitionsToBrokers.foldLeft(List.empty[String]) { (msgs, partitionAndBroker) =>
      msgs ++ sendMessagesToPartition(configs, topic, partitionAndBroker._1, messageNum)
    }.map(Message(_, Message.noTimeStamp))

    val kafkaConfig = getKafkaConfig(topic, consumerEmitBatchSize = batchSize, brokerList, zkConnect)
    val (_, echo) = StreamingTestUtil.createEchoForTaskActor(
      classOf[KafkaStreamProducer].getName, kafkaConfig, system1, system2, usePinedDispatcherForTaskActor = true)

    messages.foreach { msg =>
      echo expectMsg(10 seconds, msg)
    }
  }

  private def getKafkaConfig(consumerTopic: String, consumerEmitBatchSize: Int, brokerList: String, zookeeperConnect: String): UserConfig = {
    implicit val system = system1
    UserConfig.empty.withValue[KafkaConfig](KafkaConfig.NAME, KafkaConfig(ConfigFactory.parseMap(Map(
      KafkaConfig.CLIENT_ID -> "",
      KafkaConfig.CONSUMER_EMIT_BATCH_SIZE -> consumerEmitBatchSize.toString,
      KafkaConfig.CONSUMER_TOPICS -> consumerTopic,
      KafkaConfig.FETCH_MESSAGE_MAX_BYTES -> "100",
      KafkaConfig.FETCH_SLEEP_MS -> "0",
      KafkaConfig.FETCH_THRESHOLD -> Int.MaxValue.toString,
      KafkaConfig.GROUPER_FACTORY_CLASS -> classOf[KafkaDefaultGrouperFactory].getName,
      KafkaConfig.METADATA_BROKER_LIST -> brokerList,
      KafkaConfig.PRODUCER_EMIT_BATCH_SIZE -> "1",
      KafkaConfig.PRODUCER_TOPIC -> "",
      KafkaConfig.PRODUCER_TYPE -> "sync",
      KafkaConfig.REQUEST_REQUIRED_ACKS -> "1",
      KafkaConfig.SOCKET_RECEIVE_BUFFER_BYTES -> "65536",
      KafkaConfig.SOCKET_TIMEOUT_MS -> "1000000",
      KafkaConfig.STORAGE_REPLICAS -> "1",
      KafkaConfig.ZOOKEEPER_CONNECT -> zookeeperConnect,
      KafkaConfig.TIMESTAMP_FILTER_CLASS -> classOf[KafkaStreamProducerSpec.DummyFilter].getName,
      KafkaConfig.MESSAGE_DECODER_CLASS -> classOf[KafkaStreamProducerSpec.NoTimeStampDecoder].getName
    ).asJava)))
  }
}


object KafkaStreamProducerSpec {
  class NoTimeStampDecoder extends MessageDecoder {
    override def fromBytes(bytes: Array[Byte]): Message = {
      Injection.invert[String, Array[Byte]](bytes) match {
        case Success(msg) => Message(msg, Message.noTimeStamp)
        case Failure(e) => throw e
      }
    }
  }

  class DummyFilter extends TimeStampFilter {
    override def filter(msg: Message, predicate: TimeStamp): Option[Message] = {
      Option(msg)
    }
  }
}