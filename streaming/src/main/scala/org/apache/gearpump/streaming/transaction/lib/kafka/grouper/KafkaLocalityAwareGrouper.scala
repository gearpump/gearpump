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

package org.apache.gearpump.streaming.transaction.lib.kafka.grouper

import akka.actor.ActorContext
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaUtil
import org.apache.gearpump.util.Configs
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig._

class KafkaLocalityAwareGrouperFactory extends KafkaGrouperFactory {
  override def getKafkaGrouper(conf: Configs, context: ActorContext): KafkaGrouper = {
    val zkClient = conf.config.getZkClient()
    val host = KafkaUtil.getHost(context)
    new KafkaLocalityAwareGrouper(zkClient, host)
  }
}

/**
 *  get TopicAndPartitions whose Kafka brokers are collocated with task
 *  TODO: we need to provide a TopicAndPartition for task
 *        which is not collocated with any Kafka brokers
 */
class KafkaLocalityAwareGrouper(zkClient: ZkClient, host: String) extends KafkaGrouper {
  def group(topicAndPartitions: Array[TopicAndPartition]): Array[TopicAndPartition] = {
    topicAndPartitions.filter(tp =>
      KafkaUtil.getBroker(zkClient, tp.topic, tp.partition).host.equals(host))
  }
}