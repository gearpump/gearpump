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
package io.gearpump.integrationtest.checklist

import io.gearpump.integrationtest.{Util, TestSpecBase}
import io.gearpump.integrationtest.kafka._

/**
 * The test spec checks the Kafka datasource connector
 */
class ConnectorKafkaSpec extends TestSpecBase {

  private lazy val kafkaCluster = new KafkaCluster(cluster.getNetworkGateway)
  private lazy val kafkaJar = cluster.queryBuiltInExampleJars("kafka-").head
  private var producer: NumericalDataProducer = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
  }

  override def afterAll(): Unit = {
    kafkaCluster.shutDown()
    super.afterAll()
  }

  override def afterEach() = {
    super.afterEach()
    if (producer != null) {
      producer.stop()
      producer = null
    }
  }

  "KafkaSource and KafkaSink" should {
    "read from and write to kafka" in {
      // setup
      val sourceTopic = "topic1"
      val sinkTopic = "topic2"
      val messageNum = 10000
      kafkaCluster.produceDataToKafka(sourceTopic, messageNum)

      // exercise
      val args = Array("io.gearpump.streaming.examples.kafka.KafkaReadWrite",
        "-zookeeperConnect", kafkaCluster.getZookeeperConnectString,
        "-brokerList", kafkaCluster.getBrokerListConnectString,
        "-sourceTopic", sourceTopic,
        "-sinkTopic", sinkTopic).mkString(" ")
      val appId = restClient.submitApp(kafkaJar, args)

      // verify
      expectAppIsRunning(appId, "KafkaReadWrite")
      Util.retryUntil(kafkaCluster.getLatestOffset(sinkTopic) == messageNum)
    }
  }

  "Gearpump with Kafka" should {
    "support at-least-once message delivery" in {
      // setup
      val sourcePartitionNum = 2
      val sourceTopic = "topic3"
      val sinkTopic = "topic4"
      // Generate number sequence (1, 2, 3, ...) to the topic
      kafkaCluster.createTopic(sourceTopic, sourcePartitionNum)
      producer = new NumericalDataProducer(sourceTopic, kafkaCluster.getBrokerListConnectString)
      producer.start()

      // exercise
      val args = Array("io.gearpump.streaming.examples.kafka.KafkaReadWrite",
        "-zookeeperConnect", kafkaCluster.getZookeeperConnectString,
        "-brokerList", kafkaCluster.getBrokerListConnectString,
        "-sourceTopic", sourceTopic,
        "-sinkTopic", sinkTopic,
        "-source", sourcePartitionNum).mkString(" ")
      val appId = restClient.submitApp(kafkaJar, args)

      // verify #1
      expectAppIsRunning(appId, "KafkaReadWrite")
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)

      // verify #2
      val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
      restClient.killExecutor(appId, executorToKill) shouldBe true
      Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

      // verify #3
      val detector = new MessageLossDetector(producer.lastWriteNum)
      val kafkaReader = new SimpleKafkaReader(detector, sinkTopic,
        host = kafkaCluster.advertisedHost, port = kafkaCluster.advertisedPort)
      Util.retryUntil({
        kafkaReader.read()
        detector.allReceived
      })
    }
  }

}
