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

package org.apache.gearpump.streaming.transaction.lib.kafka

import java.net.InetAddress

import akka.actor.{ActorContext, ExtendedActorSystem}
import kafka.admin.AdminUtils
import kafka.common.TopicExistsException
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConsumer.Broker

object KafkaUtil {
  def getBroker(zkClient: ZkClient, topic: String, partition: Int): Broker = {
    val leader =  ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      .getOrElse(throw new Exception(s"leader not available for TopicAndPartition($topic, $partition)"))
    val broker = ZkUtils.getBrokerInfo(zkClient, leader)
      .getOrElse(throw new Exception(s"broker info not found for leader $leader"))
    Broker(broker.host, broker.port)
  }

  def getHost(context: ActorContext): String = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    InetAddress.getByName(address.host.get).getHostName
  }

  def createTopic(zkClient: ZkClient, topic: String, replicas: Int): Boolean = {
    try {
      AdminUtils.createTopic(zkClient, topic, 1, replicas)
      true
    } catch {
      case tee: TopicExistsException =>
        false
      case e: Exception => throw e
    }
  }
}
