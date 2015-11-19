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

import io.gearpump.integrationtest.MiniClusterProvider
import io.gearpump.integrationtest.kafka.KafkaCluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * The test spec checks the Kafka datasource connector
 */
class ConnectorKafkaSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val cluster = MiniClusterProvider.get
  val client = cluster.restClient
  val kafkaCluster = new KafkaCluster

  override def beforeAll(): Unit = {
    cluster.start()
    kafkaCluster.start()
  }

  override def afterAll(): Unit = {
    cluster.shutDown()
    kafkaCluster.shutDown()
  }

  "KafkaSource and KafaSink" should "read from and write to kafka" in {
    val kafkaHostname = s"${kafkaCluster.getHostname}"
    val zookeeperConnect = s"$kafkaHostname:${kafkaCluster.getZookeeperPort}"
    val brokerList = s"$kafkaHostname:${kafkaCluster.getBrokerPort}"
    val sourceTopic = "topic1"
    val sinkTopic = "topic2"

    val jar = cluster.queryBuiltInExampleJars("kafka-").head
    val appsCount = client.listApps().size
    val appId = appsCount + 1

    // exercise
    val args = Array("io.gearpump.streaming.examples.kafka.KafkaReadWrite",
      "-zookeeperConnect", zookeeperConnect,
      "-brokerList", brokerList,
      "-sourceTopic", sourceTopic,
      "-sinkTopic", sinkTopic).mkString(" ")
    val success = client.submitApp(jar, args)
    Thread.sleep(5000)
    success shouldEqual true
    val actual = client.queryApp(appId)

    actual.appId shouldEqual appId
    actual.status shouldEqual "active"
    actual.appName shouldEqual "KafkaReadWrite"

    val messageNum = 10000
    kafkaCluster.produceDataToKafka(zookeeperConnect, brokerList, sourceTopic, messageNum)
    kafkaCluster.getLatestOffset(brokerList, sinkTopic) shouldBe messageNum
  }

}
