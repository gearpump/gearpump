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

import com.twitter.bijection.Injection
import kafka.admin.AdminUtils
import kafka.common.{TopicAndPartition, TopicExistsException}
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.api.Storage
import org.apache.gearpump.streaming.transaction.api.Storage.{Overflow, Underflow, StorageEmpty}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig._
import org.apache.gearpump.util.{Configs, LogUtil}
import org.slf4j.Logger

import scala.util.{Success, Failure, Try}

object KafkaStorage {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaStorage])

  def apply(conf: Configs, topicAndPartition: TopicAndPartition): KafkaStorage =  {
    val config = conf.config
    val id = conf.appId
    val topic = s"app${id}_${topicAndPartition.topic}_${topicAndPartition.partition}"
    val partition = 0
    val replicas = config.getStorageReplicas
    val producer = config.getProducer[Array[Byte], Array[Byte]]()
    val clientId = config.getClientId
    val socketTimeout = config.getSocketTimeoutMS
    val receiveBufferSize = config.getSocketReceiveBufferBytes
    val fetchSize = config.getFetchMessageMaxBytes
    val zkClient = config.getZkClient()
    val topicExists = KafkaUtil.createTopic(zkClient, topic, replicas)
    val getMessageIterator = () => {
      val broker = KafkaUtil.getBroker(zkClient, topic, partition)
      new MessageIterator(broker.host, broker.port,
        topic, partition, socketTimeout, receiveBufferSize, fetchSize, clientId)
    }
    new KafkaStorage(topic, topicExists, producer, getMessageIterator)
  }
}

class KafkaStorage(topic: String,
                   topicExists: Boolean,
                   producer: KafkaProducer[Array[Byte], Array[Byte]],
                   getMessageIterator: () => MessageIterator) extends Storage {
  import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaStorage._

  val iterator = getMessageIterator()
  // already sorted by TimeStamp
  var dataByTime: List[(TimeStamp, Array[Byte])] = load(iterator)

  /**
   * find data of max TimeStamp <= @param time
   * return Success(Array[Byte]) if the offset exists
   * return Failure(StorageEmpty) if no (TimeStamp, Array[Byte]) stored
   * return Failure(Overflow(max Array[Byte])) if @param time > max TimeStamp
   * return Failure(Underflow(min Array[Byte])) if @param time < min TimeStamp
   */
  override def lookUp(time: TimeStamp): Try[Array[Byte]] = {
    val min = dataByTime.head
    val max = dataByTime.last
    if (dataByTime.isEmpty) {
      Failure(StorageEmpty)
    } else if (time < min._1) {
      Failure(Underflow(min._2))
    } else if (time > max._1) {
      Failure(Overflow(max._2))
    } else {
      Success(dataByTime.reverse.find(_._1 <= time).get._2)
    }
  }

  override def append(time: TimeStamp, offset: Array[Byte]): Unit = {
    producer.send(topic, Injection[Long, Array[Byte]](time), 0,
      offset)
  }

  private def load(iterator: MessageIterator): List[(TimeStamp, Array[Byte])] = {
    @annotation.tailrec
    def fetch(offsets: List[(TimeStamp, Array[Byte])]): List[(TimeStamp, Array[Byte])] = {
      if (iterator.hasNext) {
        val (_, key, payload) = iterator.next
        val offset = key.map { k =>
          (Injection.invert[TimeStamp, Array[Byte]](k).get,
            payload)
        } orElse (throw new RuntimeException("offset key should not be null"))
        fetch(offsets :+ offset.get)
      } else {
        iterator.close()
        offsets
      }
    }
    if (topicExists) {
      fetch(List.empty[(TimeStamp, Array[Byte])])
    } else {
      List.empty[(TimeStamp, Array[Byte])]
    }
  }
}

