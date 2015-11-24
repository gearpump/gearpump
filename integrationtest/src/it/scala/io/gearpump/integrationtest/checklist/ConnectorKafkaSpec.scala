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

import scala.collection.mutable

/**
 * The test spec checks the Kafka datasource connector
 */
class ConnectorKafkaSpec extends TestSpecBase {

  private lazy val kafkaCluster = new KafkaCluster(cluster.getDockerMachineIp())
  private var producer: NumericalDataProducer = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
  }

  override def afterAll(): Unit = {
    kafkaCluster.shutDown()
    super.afterAll()
  }

  override def afterEach = {
    super.afterEach
    if(producer != null) {
      producer.stop()
      producer = null
    }
  }

  "KafkaSource and KafaSink" should {
    "read from and write to kafka" in {
      val kafkaHostname = s"${kafkaCluster.getHostname}"
      val zookeeperConnect = s"$kafkaHostname:${kafkaCluster.getZookeeperPort}"
      val brokerList = s"$kafkaHostname:${kafkaCluster.getBrokerPort}"
      val sourceTopic = "topic1"
      val sinkTopic = "topic2"

      val jar = cluster.queryBuiltInExampleJars("kafka-").head
      val appsCount = restClient.listApps().length
      val appId = appsCount + 1
      val messageNum = 10000

      kafkaCluster.produceDataToKafka(zookeeperConnect, brokerList, sourceTopic, messageNum)

      // exercise
      val args = Array("io.gearpump.streaming.examples.kafka.KafkaReadWrite",
        "-zookeeperConnect", zookeeperConnect,
        "-brokerList", brokerList,
        "-sourceTopic", sourceTopic,
        "-sinkTopic", sinkTopic).mkString(" ")
      restClient.submitApp(jar, args)
      val actual = restClient.queryApp(appId)

      actual.appId shouldEqual appId
      actual.status shouldEqual "active"
      actual.appName shouldEqual "KafkaReadWrite"

      Util.retryUntil(kafkaCluster.getLatestOffset(brokerList, sinkTopic) == messageNum)
    }
  }

  "Gearpump with Kafka" should {
    "support at least once" in {
      val zookeeperConnect = s"${kafkaCluster.getHostname}:${kafkaCluster.getZookeeperPort}"
      val brokerList = s"${kafkaCluster.advertisedHost}:${kafkaCluster.advertisedPort}"
      val sourcePartitionNum = 2
      val sourceTopic = "topic3"
      val sinkTopic = "topic4"
      val appsCount = restClient.listApps().length
      val appId = appsCount + 1

      //Generate number sequence to the topic, start from number 1
      kafkaCluster.createTopic(zookeeperConnect, sourceTopic, sourcePartitionNum)
      producer = new NumericalDataProducer(sourceTopic, brokerList)
      producer.start()

      // exercise
      val jar = cluster.queryBuiltInExampleJars("kafka-").head
      val args = Array("io.gearpump.streaming.examples.kafka.KafkaReadWrite",
        "-zookeeperConnect", zookeeperConnect,
        "-brokerList", brokerList,
        "-sourceTopic", sourceTopic,
        "-sinkTopic", sinkTopic,
        "-source", sourcePartitionNum).mkString(" ")
      restClient.submitApp(jar, args)

      expectAppIsRunning(appId, "KafkaReadWrite")

      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
      restClient.killExecutor(appId, executorToKill) shouldBe true
      producer.stop()

      Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

      val detector = new MessageLossDetector(producer.lastWriteNum)
      val kafkaReader = new SimpleKafkaReader(detector, sinkTopic, host = kafkaCluster.advertisedHost, port = kafkaCluster.advertisedPort)
      Util.retryUntil {
        kafkaReader.read()
        detector.allReceived
      }
    }
  }

  class MessageLossDetector(totalNum: Int) extends ResultVerifier {
    private val bitSets = new mutable.BitSet(totalNum)

    override def onNext(msg: String): Unit = {
      val num = msg.toInt
      bitSets.add(num)
    }

    def allReceived: Boolean = {
      bitSets.size == totalNum
    }
  }
}
