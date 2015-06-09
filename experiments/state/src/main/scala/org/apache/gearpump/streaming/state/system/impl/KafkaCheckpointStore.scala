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

package org.apache.gearpump.streaming.state.system.impl

import com.twitter.bijection.Injection
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.kafka.lib.{KafkaConfig, KafkaConsumer, KafkaUtil}
import org.apache.gearpump.streaming.state.system.api.{CheckpointStore, CheckpointStoreFactory}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.Logger

import scala.util.Try

object KafkaCheckpointStore {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaCheckpointStore])

  def apply(config: KafkaConfig, topic: String, topicExists: Boolean) = {
    val getConsumer = () => KafkaConsumer(topic, 0, config)
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](
      KafkaUtil.buildProducerConfig(config), new ByteArraySerializer, new ByteArraySerializer)
    new KafkaCheckpointStore(topic, topicExists, producer, getConsumer())
  }
}

/**
 * checkpoint store that writes to kafka
 */
class KafkaCheckpointStore(topic: String,
                           topicExists: Boolean,
                           producer: KafkaProducer[Array[Byte], Array[Byte]],
                           getConsumer: => KafkaConsumer) extends CheckpointStore {

  override def read(timestamp: TimeStamp): Option[Array[Byte]] = {
    val consumer = Try(getConsumer).toOption
    consumer.flatMap { con =>

      @annotation.tailrec
      def readInternal(checkpoint: Option[Array[Byte]]): Option[Array[Byte]] = {
        if (con.hasNext) {
          val kafkaMsg = con.next()
          val time = Injection.invert[TimeStamp, Array[Byte]](kafkaMsg.key.get).get
          if (time == timestamp) {
            readInternal(Option(kafkaMsg.msg))
          } else {
            readInternal(checkpoint)
          }
        } else {
          checkpoint
        }
      }

      try {
        readInternal(None)
      } finally {
        con.close()
      }
    }
  }

  override def write(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    val message = new ProducerRecord[Array[Byte], Array[Byte]](
      topic, 0, Injection[Long, Array[Byte]](timestamp), checkpoint)
    producer.send(message)
  }

  override def close(): Unit = {
    producer.close()
  }
}

class KafkaCheckpointStoreFactory extends CheckpointStoreFactory {
  override def getCheckpointStore(conf: UserConfig, taskContext: TaskContext): CheckpointStore = {
    import taskContext.{appId, taskId}
    implicit val system = taskContext.system
    val topic = s"app${appId}_task_${taskId.processorId}_${taskId.index}"
    val kafkaConfig = conf.getValue[KafkaConfig](KafkaConfig.NAME)
      .getOrElse(throw new RuntimeException("kafka config not provided"))
    val replicas = kafkaConfig.getStorageReplicas
    val connectZk = KafkaUtil.connectZookeeper(kafkaConfig)
    val topicExists = KafkaUtil.createTopic(connectZk(), topic, partitions = 1, replicas)
    KafkaCheckpointStore(kafkaConfig, topic, topicExists)
  }
}
