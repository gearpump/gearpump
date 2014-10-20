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

package org.apache.gearpump.streaming.transaction.kafka

import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.api.{Checkpoint, CheckpointManager}
import org.apache.gearpump.streaming.transaction.api.Source
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig._
import org.apache.gearpump.streaming.transaction.kafka.KafkaUtil._
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}


object KafkaCheckpointManager {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaCheckpointManager])
}

class KafkaCheckpointManager(conf: Configs) extends CheckpointManager {
  import org.apache.gearpump.streaming.transaction.kafka.KafkaCheckpointManager._

  private val config = conf.config
  private val producer = config.getProducer[Array[Byte], Array[Byte]](
    producerConfig = config.getProducerConfig(serializerClass = "kafka.serializer.DefaultEncoder")
  )

  private var checkpointTopicAndPartitions: Array[TopicAndPartition] = null
  private var consumer: KafkaConsumer = null

  override def start(): Unit = {
    createTopics()

  }

  override def register(topicAndPartitions: Array[Source]): Unit = {
    this.checkpointTopicAndPartitions =
      topicAndPartitions.map(getCheckpointTopicAndPartition(_))
  }

  override def writeCheckpoint(source: Source,
                               checkpoint: Checkpoint): Unit = {
    val checkpointTopicAndPartition = getCheckpointTopicAndPartition(source)
    checkpoint.timeAndOffsets.foreach(timeAndOffset => {
      producer.send(checkpointTopicAndPartition.topic, longToByteArray(timeAndOffset._1),
        0, longToByteArray(timeAndOffset._2))
    })

  }

  override def readCheckpoint(source: Source): Checkpoint = {
    // get consumers only after topics having been created
    LOG.info("creating consumer...")
    if (null == consumer ) {
      consumer = config.getConsumer(topicAndPartitions = checkpointTopicAndPartitions)
    }
    val checkpointTopicAndPartition = getCheckpointTopicAndPartition(source)

    @annotation.tailrec
    def fetch(timeAndOffsets: Map[TimeStamp, Long]): Map[TimeStamp, Long] = {
      val kafkaMsg = consumer.nextMessage(checkpointTopicAndPartition)
      if (kafkaMsg != null) {
        if (kafkaMsg.key != null) {
          fetch(timeAndOffsets +
            (byteArrayToLong(kafkaMsg.key) -> byteArrayToLong(kafkaMsg.msg)))
        } else {
          LOG.error(s"timestamp is null at offset ${kafkaMsg.offset} for ${checkpointTopicAndPartition}")
          fetch(timeAndOffsets)
        }
      } else {
        timeAndOffsets
      }
    }
    Checkpoint(fetch(Map.empty[TimeStamp, Long]))
  }

  override def close(): Unit = {
    producer.close()
    if (consumer != null) {
      consumer.close()
    }
  }

  private def createTopics(): Unit = {
    val partitionsByTopic = checkpointTopicAndPartitions.groupBy(_.topic)
    partitionsByTopic.foreach(entry => {
      val topic = entry._1
      val zkClient = config.getZkClient()
      if (!AdminUtils.topicExists(zkClient, topic)) {
        AdminUtils.createTopic(zkClient, topic, 1, config.getCheckpointReplicas)
      }
    })
  }

  private def getCheckpointTopic(appId: Int, topic: String, partition: Int): String  = {
    s"checkpoint_application${appId}_${topic}_${partition}"
  }

  private def getCheckpointTopicAndPartition(source: Source): TopicAndPartition = {
    TopicAndPartition(getCheckpointTopic(conf.appId, source.name, source.partition), 0)
  }

}
