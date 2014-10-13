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

import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import org.apache.gearpump.streaming.examples.kafka.KafkaConfig._
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

object KafkaCheckpointManager {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaCheckpointManager])
}

class KafkaCheckpointManager(topicAndPartitions: Array[TopicAndPartition],
                             conf: Configs) extends CheckpointManager {

  private val config = conf.config
  private val producer = config.getProducer[Array[Byte], Array[Byte]](
    producerConfig = config.getProducerConfig(serializerClass = "kafka.serializer.DefaultEncoder")
  )
  private val checkpointTopicAndPartitions =
    topicAndPartitions.map(getCheckpointTopicAndPartition(_))
  private var consumer: KafkaConsumer = null

  override def start(): Unit = {
    createTopics()
  }

  override def writeCheckpoint(topicAndPartition: TopicAndPartition,
                               checkpoint: Checkpoint): Unit = {
    producer.send(topicAndPartition.topic, KafkaUtil.longToByteArray(checkpoint.timestamp),
      topicAndPartition.partition, checkpoint.data)
  }

  override def readCheckpoints(topicAndPartition: TopicAndPartition): List[Checkpoint] = {
    // can only get consumer after checkpoint topics having been created
    if (null == consumer) {
      consumer = config.getConsumer(topicAndPartitions = checkpointTopicAndPartitions)
    }
    val checkpointTopicAndPartition = getCheckpointTopicAndPartition(topicAndPartition)
    @annotation.tailrec
    def fetch(checkpoints: List[Checkpoint]): List[Checkpoint] = {
      val kafkaMsg = consumer.nextMessage(checkpointTopicAndPartition)
      if (kafkaMsg != null) {
        fetch(checkpoints :+ Checkpoint(KafkaUtil.byteArrayToLong(kafkaMsg.key), kafkaMsg.msg))
      } else {
        checkpoints
      }
    }
    fetch(List.empty[Checkpoint])
  }

  override def close(): Unit = {
    producer.close()
    if (consumer != null) {
      consumer.close()
    }
  }

  private def createTopics(): Unit = {
    val partitionsByTopic = checkpointTopicAndPartitions.groupBy(tp => tp.topic)
    partitionsByTopic.foreach(entry => {
      val topic = entry._1
      val partitions = entry._2
      val zkClient = config.getZkClient()
      if (!AdminUtils.topicExists(zkClient, topic)) {
        AdminUtils.createTopic(
          zkClient, topic,
          partitions.size, config.getCheckpointReplicas)
      }
    })
  }

  private def getCheckpointTopic(appId: Int, topic: String): String  = {
    s"checkpoint_application${appId}_${topic}"
  }

  private def getCheckpointTopicAndPartition(topicAndPartition: TopicAndPartition): TopicAndPartition = {
    TopicAndPartition(getCheckpointTopic(conf.appId, topicAndPartition.topic), topicAndPartition.partition)
  }

}
