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
package org.apache.gearpump.integrationtest.checklist

import org.apache.gearpump.integrationtest.kafka.{KafkaCluster, MessageLossDetector, SimpleKafkaReader}
import org.apache.gearpump.integrationtest.storm.StormClient
import org.apache.gearpump.integrationtest.{TestSpecBase, Util}

/**
 * The test spec checks the compatibility of running Storm applications
 */
class StormCompatibilitySpec extends TestSpecBase {

  private lazy val stormClient = {
    new StormClient(cluster, restClient)
  }

  val `version0.9` = "09"
  val `version0.10` = "010"

  override def beforeAll(): Unit = {
    super.beforeAll()
    stormClient.start()
  }

  override def afterAll(): Unit = {
    stormClient.shutDown()
    super.afterAll()
  }

  def withStorm(testCode: String => Unit): Unit = {
    testCode(`version0.9`)
    testCode(`version0.10`)
  }

  def getTopologyName(name: String, stormVersion: String): String = {
    s"${name}_$stormVersion"
  }

  def getStormJar(stormVersion: String): String = {
    cluster.queryBuiltInITJars(s"storm$stormVersion-").head
  }

  "Storm over Gearpump" should withStorm {
    stormVersion =>
      s"support basic topologies ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("exclamation", stormVersion)

        // exercise
        val appId = stormClient.submitStormApp(
          jar = stormJar,
          mainClass = "storm.starter.ExclamationTopology",
          args = topologyName,
          appName = topologyName)

        // verify
        Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
      }

      s"support to run a python version of wordcount ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("wordcount", stormVersion)

        // exercise
        val appId = stormClient.submitStormApp(
          jar = stormJar,
          mainClass = "storm.starter.WordCountTopology",
          args = topologyName,
          appName = topologyName)

        // verify
        Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
      }

      s"support DRPC ($stormVersion)" in {
        // ReachTopology computes the Twitter url reached by users and their followers
        // using Storm Distributed RPC feature
        // input (user and follower) data are already prepared in memory
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("reach", stormVersion)
        stormClient.submitStormApp(
          jar = stormJar,
          mainClass = "storm.starter.ReachTopology",
          args = topologyName,
          appName = topologyName)
        val drpcClient = stormClient.getDRPCClient(cluster.getNetworkGateway)

        // verify
        Util.retryUntil(() => {
          drpcClient.execute("reach", "notaurl.com") == "0"
        }, "drpc reach == 0")
        drpcClient.execute("reach", "foo.com/blog/1") shouldEqual "16"
        drpcClient.execute("reach", "engineering.twitter.com/blog/5") shouldEqual "14"
      }

      s"support tick tuple ($stormVersion)" in {
        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("slidingWindowCounts", stormVersion)

        // exercise
        val appId = stormClient.submitStormApp(
          jar = stormJar,
          mainClass = "storm.starter.RollingTopWords",
          args = s"$topologyName remote",
          appName = topologyName)

        // verify
        Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
      }

      s"support at-least-once semantics with Storm's Kafka connector ($stormVersion)" in {

        val stormJar = getStormJar(stormVersion)
        val topologyName = getTopologyName("storm_kafka", stormVersion)
        val stormKafkaTopology =
          s"org.apache.gearpump.integrationtest.storm.Storm${stormVersion}KafkaTopology"

        import org.apache.gearpump.integrationtest.kafka.KafkaCluster._
        withKafkaCluster(cluster) {
          kafkaCluster =>
            val sourcePartitionNum = 2
            val sinkPartitionNum = 1
            val zookeeper = kafkaCluster.getZookeeperConnectString
            val brokerList = kafkaCluster.getBrokerListConnectString
            val sourceTopic = "topic1"
            val sinkTopic = "topic2"

            val args = Array("-topologyName", topologyName, "-sourceTopic", sourceTopic,
              "-sinkTopic", sinkTopic, "-zookeeperConnect", zookeeper, "-brokerList", brokerList,
              "-spoutNum", s"$sourcePartitionNum", "-boltNum", s"$sinkPartitionNum"
            )

            kafkaCluster.createTopic(sourceTopic, sourcePartitionNum)

            // generate number sequence (1, 2, 3, ...) to the topic
            withDataProducer(sourceTopic, brokerList) { producer =>

              val appId = stormClient.submitStormApp(
                jar = stormJar,
                mainClass = stormKafkaTopology,
                args = args.mkString(" "),
                appName = topologyName)

              Util.retryUntil(() =>
                restClient.queryStreamingAppDetail(appId).clock > 0, "app running")

              // kill executor and verify at-least-once is guaranteed on application restart
              val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
              restClient.killExecutor(appId, executorToKill) shouldBe true
              Util.retryUntil(() =>
                restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill,
                s"executor $executorToKill killed")

              // verify no message loss
              val detector = new
                  MessageLossDetector(producer.lastWriteNum)
              val kafkaReader = new
                  SimpleKafkaReader(detector, sinkTopic, host = kafkaCluster.advertisedHost,
                    port = kafkaCluster.advertisedPort)

              Util.retryUntil(() => {
                kafkaReader.read()
                detector.allReceived
              }, "all kafka message read")
            }
        }
      }
  }
}
