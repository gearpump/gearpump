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

import io.gearpump.integrationtest.kafka.{KafkaCluster, MessageLossDetector, NumericalDataProducer, SimpleKafkaReader}
import io.gearpump.integrationtest.storm.StormClient
import io.gearpump.integrationtest.{TestSpecBase, Util}

/**
  * The test spec checks the compatibility of running Storm applications
  */
class StormCompatibilitySpec extends TestSpecBase {

  private lazy val stormClient = new StormClient(cluster.getMastersAddresses)

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster.isAlive shouldBe true
    stormClient.start()
  }

  override def afterAll(): Unit = {
    stormClient.shutDown()
    super.afterAll()
  }

  def withStorm(testCode: String => Unit): Unit = {
    testCode("09")
    testCode("010")
  }

  def withKafkaCluster(testCode: KafkaCluster => Unit): Unit = {
    val kafkaCluster = new KafkaCluster(cluster.getNetworkGateway, "kafka")
    try {
      kafkaCluster.start()
      testCode(kafkaCluster)
    } finally {
      kafkaCluster.shutDown()
    }
  }

  def withDataProducer(topic: String, brokerList: String)
      (testCode: NumericalDataProducer => Unit): Unit = {
    val producer = new NumericalDataProducer(topic, brokerList)
    try {
      producer.start()
      testCode(producer)
    } finally {
      producer.stop()
    }
  }

  def getTopologyName(name: String, stormVersion: String): String = {
    s"${name}_$stormVersion"
  }

  def getStormJar(stormVersion: String): String = {
    cluster.queryBuiltInExampleJars(s"storm$stormVersion-").head
  }

  "Storm over Gearpump" should withStorm {
    stormVersion =>
      s"support basic topologies ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("exclamation", stormVersion)
        // exercise
        val appId = stormClient.submitStormApp(stormJar,
          mainClass = "storm.starter.ExclamationTopology", args = topologyName)

        // verify
        val actual = expectAppIsRunning(appId, topologyName)
        Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
      }

      s"support to run a python version of wordcount ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("wordcount", stormVersion)
        // exercise
        val appId = stormClient.submitStormApp(stormJar,
          mainClass = "storm.starter.WordCountTopology", args = topologyName)

        // verify
        expectAppIsRunning(appId, topologyName)
        Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      }

      s"support DRPC ($stormVersion)" in {
        // ReachTopology computes the Twitter url reached by users and their followers
        // using Storm Distributed RPC feature
        // input (user and follower) data are already prepared in memory
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("reach", stormVersion)
        stormClient.submitStormApp(stormJar,
          mainClass = "storm.starter.ReachTopology", args = topologyName)
        val drpcClient = stormClient.getDRPCClient(cluster.getNetworkGateway)

        // verify
        Util.retryUntil {
          drpcClient.execute("reach", "notaurl.com") == "0"
        }
        drpcClient.execute("reach", "foo.com/blog/1") shouldEqual "16"
        drpcClient.execute("reach", "engineering.twitter.com/blog/5") shouldEqual "14"
      }

      s"support tick tuple ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("slidingWindowCounts", stormVersion)
        // exercise
        val appId = stormClient.submitStormApp(stormJar,
          mainClass = "storm.starter.RollingTopWords", args = s"$topologyName remote")

        // verify
        val actual = expectAppIsRunning(appId, topologyName)
        Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
      }

      s"support at-least-once semantics with Storm's Kafka connector ($stormVersion)" in {

        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("storm_kafka", stormVersion)
        val stormKafkaTopology = s"io.gearpump.streaming.examples.storm.Storm${stormVersion}KafkaTopology"

        withKafkaCluster {
          kafkaCluster =>
            val sourcePartitionNum = 2
            val sinkPartitionNum = 1
            val zookeeper = kafkaCluster.getZookeeperConnectString
            val brokerList = kafkaCluster.getBrokerListConnectString
            val sourceTopic = "topic1"
            val sinkTopic = "topic2"
            val appsCount = restClient.listApps().length
            val appId = appsCount + 1

            val args = Array("-topologyName", topologyName, "-sourceTopic", sourceTopic,
              "-sinkTopic", sinkTopic, "-zookeeperConnect", zookeeper, "-brokerList", brokerList,
              "-spoutNum", s"$sourcePartitionNum",  "-boltNum", s"$sinkPartitionNum"
            )

            kafkaCluster.createTopic(sourceTopic, sourcePartitionNum)

            // generate number sequence (1, 2, 3, ...) to the topic
            withDataProducer(sourceTopic, brokerList) { producer =>

              stormClient.submitStormApp(stormJar,
                mainClass = stormKafkaTopology,
                args = args.mkString(" "))

              expectAppIsRunning(appId, topologyName)
              Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)

              // kill executor and verify at-least-once is guaranteed on application restart
              val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
              restClient.killExecutor(appId, executorToKill) shouldBe true
              Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

              // verify no message loss
              val detector = new
                      MessageLossDetector(producer.lastWriteNum)
              val kafkaReader = new
                      SimpleKafkaReader(detector, sinkTopic, host = kafkaCluster.advertisedHost,
                        port = kafkaCluster.advertisedPort)

              Util.retryUntil {
                kafkaReader.read()
                detector.allReceived
              }
            }
        }
      }
  }
}
