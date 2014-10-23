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
import kafka.common.{TopicExistsException, TopicAndPartition}
import org.apache.gearpump.streaming.transaction.api.CheckpointManager
import org.apache.gearpump.streaming.transaction.api.CheckpointManager._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{Map => MutableMap}
import org.I0Itec.zkclient.ZkClient


object KafkaCheckpointManager {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaCheckpointManager[_, _]])
}

class KafkaCheckpointManager[K, V](checkpointId: Int,
                             checkpointReplicas: Int,
                             producer: KafkaProducer[Array[Byte], Array[Byte]],
                             clientId: String,
                             socketTimeout: Int,
                             receiveBufferSize: Int,
                             fetchSize: Int,
                             zkClient: ZkClient
                             ) extends CheckpointManager[K, V] {
  import org.apache.gearpump.streaming.transaction.kafka.KafkaCheckpointManager._

  private var sources: Array[Source] = null
  private var checkpointTopicAndPartitions: Array[TopicAndPartition] = null
  private val topicExists: MutableMap[TopicAndPartition, Boolean] = MutableMap.empty[TopicAndPartition, Boolean]

  override def start(): Unit = {
    createTopics()
  }

  override def register(sources: Array[Source]): Unit = {
    this.sources = sources
    this.checkpointTopicAndPartitions =
      sources.map(getCheckpointTopicAndPartition(_))
  }

  override def writeCheckpoint(source: Source,
                               checkpoint: Checkpoint[K, V],
                               checkpointSerDe: CheckpointSerDe[K, V]): Unit = {
    val checkpointTopicAndPartition = getCheckpointTopicAndPartition(source)

    checkpoint.records.foreach(record => {
      producer.send(
        checkpointTopicAndPartition.topic,
        checkpointSerDe.toKeyBytes(record._1),
        0,
        checkpointSerDe.toValueBytes(record._2))
    })
  }

  override def sourceAndCheckpoints(checkpointSerDe: CheckpointSerDe[K, V]): Map[Source, Checkpoint[K, V]] = {
    sources.map(source => source -> readCheckpoint(source, checkpointSerDe)).toMap
  }

  override def readCheckpoint(source: Source, checkpointSerDe: CheckpointSerDe[K, V]): Checkpoint[K, V] = {
    val topicAndPartition = TopicAndPartition(source.name, source.partition)
    // no checkpoint to read for the first time
    if (!topicExists.getOrElse(topicAndPartition, false)) {
      Checkpoint.empty
    } else {
      // get consumers only after topics having been created
      LOG.info("creating consumer...")
      val msgIter = consume(topicAndPartition)
      val checkpointTopicAndPartition = getCheckpointTopicAndPartition(source)

      @annotation.tailrec
      def fetch(records: List[(K, V)]): List[(K, V)] = {
        if (msgIter.hasNext) {
          val key = msgIter.getKey
          if (key != null) {
            val r: (K, V) = (checkpointSerDe.fromKeyBytes(key), checkpointSerDe.fromValueBytes(msgIter.next))
            fetch(records :+ r)
          } else {
            // TODO: this should not happen; need further investigation
            LOG.error(s"timestamp is null at offset ${msgIter.getOffset} for ${checkpointTopicAndPartition}")
            fetch(records)
          }
        } else {
          msgIter.close()
          records
        }
      }
      Checkpoint(fetch(List.empty[(K, V)]))
    }
  }

  override def close(): Unit = {
    producer.close()
    zkClient.close()
  }


  private def consume(topicAndPartition: TopicAndPartition): MessageIterator = {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    val broker = KafkaUtil.getBroker(zkClient, topic, partition)
    new MessageIterator(broker.host, broker.port, topic, partition, socketTimeout,
      receiveBufferSize, fetchSize, clientId)
  }

  private def createTopics(): Unit = {
    checkpointTopicAndPartitions.foreach {
      tp => {
        try {
          val topic = tp.topic
          AdminUtils.createTopic(zkClient, topic, 1, checkpointReplicas)
          topicExists.put(tp, false)
        } catch {
          case tee: TopicExistsException => {
            LOG.info(s"${tp} already exists")
            topicExists.put(tp, true)
          }
          case e: Exception => throw e
        }
      }
    }
  }

  private def getCheckpointTopic(id: Int, topic: String, partition: Int): String  = {
    s"checkpoint_${id}_${topic}_${partition}"
  }

  private def getCheckpointTopicAndPartition(source: Source): TopicAndPartition = {
    TopicAndPartition(getCheckpointTopic(checkpointId, source.name, source.partition), 0)
  }

}
