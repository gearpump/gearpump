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

package org.apache.gearpump.streaming.kafka.util

import java.util.Properties

import kafka.common.KafkaException
import kafka.message.{NoCompressionCodec, CompressionCodec}
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig => KafkaServerConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{FixedValuePartitioner, IntEncoder, Utils, TestUtils}
import org.I0Itec.zkclient.ZkClient

trait KafkaServerHarness extends ZookeeperHarness {
  val configs: List[KafkaServerConfig]
  private var servers: List[KafkaServer] = null
  private var brokerList: String = null

  def getServers: List[KafkaServer] = servers
  def getBrokerList: Array[String] = brokerList.split(",")

  override def setUp() {
    super.setUp
    if (configs.size <= 0)
      throw new KafkaException("Must supply at least one server config.")
    brokerList = TestUtils.getBrokerListStrFromConfigs(configs)
    servers = configs.map(TestUtils.createServer(_))
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => server.config.logDirs.map(Utils.rm(_)))
    super.tearDown
  }


  /**
   * forked from TestUtils in kafka trunk
   * TODO: use this method in TestUtils directly when Kafka releases 0.8.2
   */
  def sendMessagesToPartition(configs: Seq[KafkaServerConfig],
                              topic: String,
                              partition: Int,
                              numMessages: Int,
                              compression: CompressionCodec = NoCompressionCodec): List[String] = {
    val header = "test-%d".format(partition)
    val props = new Properties()
    props.put("compression.codec", compression.codec.toString)
    val producer: Producer[Int, String] =
      createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[IntEncoder].getName,
        partitioner = classOf[FixedValuePartitioner].getName,
        producerProps = props)

    val ms = 0.until(numMessages).map(x => header + "-" + x)
    producer.send(ms.map(m => new KeyedMessage[Int, String](topic, partition, m)):_*)
    producer.close()
    ms.toList
  }

  def createTopicUntilLeaderIsElected(topic: String, partitions: Int, replicas: Int) = {
    val zkClient = newZkClient
    try {
      TestUtils.createTopic(zkClient, topic, partitions, replicas, servers)
    } catch {
      case e: Exception => throw e
    } finally {
      zkClient.close()
    }
  }
}
