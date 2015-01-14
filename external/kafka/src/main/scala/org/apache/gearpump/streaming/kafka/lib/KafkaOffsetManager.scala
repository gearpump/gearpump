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

package org.apache.gearpump.streaming.kafka.lib

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.{Overflow, StorageEmpty, Underflow}
import org.apache.gearpump.streaming.transaction.api.{OffsetManager, OffsetStorage}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

object KafkaOffsetManager {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaOffsetManager])
  def apply(appId: Int, config: KafkaConfig, topicAndPartition: TopicAndPartition): KafkaOffsetManager = {
    val storageTopic = s"app${appId}_${topicAndPartition.topic}_${topicAndPartition.partition}"
    val replicas = config.getStorageReplicas
    val zkClient = KafkaUtil.connectZookeeper(config)
    val topicExists = KafkaUtil.createTopic(zkClient, storageTopic, partitions = 1, replicas)
    new KafkaOffsetManager(KafkaStorage(config, storageTopic, topicExists, topicAndPartition))
  }
}

private[kafka] class KafkaOffsetManager(storage: OffsetStorage) extends OffsetManager {
  import org.apache.gearpump.streaming.kafka.lib.KafkaOffsetManager._

  var maxTime: TimeStamp  = 0L

  override def filter(messageAndOffset: (Message, Long)): Option[Message] = {
    val (message, offset) = messageAndOffset
    maxTime = Math.max(maxTime, message.timestamp)
    storage.append(maxTime, Injection[Long, Array[Byte]](offset))
    Some(message)
  }

  override def resolveOffset(time: TimeStamp): Try[Long] = {
    storage.lookUp(time) match {
      case Success(offset) => Injection.invert[Long, Array[Byte]](offset)
      case Failure(Overflow(max)) =>
        LOG.warn(s"start time larger than the max stored TimeStamp; set to max offset")
        Injection.invert[Long, Array[Byte]](max)
      case Failure(Underflow(min)) =>
        LOG.warn(s"start time less than the min stored TimeStamp; set to min offset")
        Injection.invert[Long, Array[Byte]](min)
      case Failure(StorageEmpty) => Failure(StorageEmpty)
      case Failure(e) => throw e
    }
  }
}
