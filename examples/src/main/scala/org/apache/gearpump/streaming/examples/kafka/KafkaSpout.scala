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

import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.serializer.StringDecoder
import kafka.utils.{Utils, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.streaming.examples.kafka.KafkaConstants._
import org.apache.gearpump.streaming.task.{TaskActor, Message}
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{Map => MutableMap}
import scala.util._


object KafkaSpout {

  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaSpout])
}


/**
 * connect gearpump with Kafka
 */
class KafkaSpout(conf: Configs) extends TaskActor(conf) {

  import org.apache.gearpump.streaming.examples.kafka.KafkaSpout._

  private val config = conf.config
  private val zookeeper = config.get(ZOOKEEPER).get.asInstanceOf[String]
  private val kafkaRoot = config.get(KAFKA_ROOT).get.asInstanceOf[String]
  private val connectString = zookeeper + kafkaRoot
  private val topic = config.get(CONSUMER_TOPIC).get.asInstanceOf[String]
  private val clientId = config.get(CLIENT_ID).get.asInstanceOf[String]
  private val soTimeout = config.get(SO_TIMEOUT).get.asInstanceOf[Int]
  private val bufferSize = config.get(SO_BUFFERSIZE).get.asInstanceOf[Int]
  private val fetchSize = config.get(FETCH_SIZE).get.asInstanceOf[Int]
  private val zkClient = new ZkClient(connectString, soTimeout, soTimeout, ZKStringSerializer)
  private val batchSize = config.get(BATCH_SIZE).get.asInstanceOf[Int]

  val brokers = {
    ZkUtils.getAllBrokersInCluster(zkClient).map(b => Broker(b.host, b.port)).toList
  }

  val partitions = {
    val partitionPath = kafkaRoot + ZkUtils.getTopicPartitionsPath(topic)
    val numPartitions = ZkUtils.getPartitionsForTopics(zkClient, List(topic))(topic).size
    val numTasks = conf.dag.tasks(taskId.groupId).parallism
    val id = taskId.index
    val partitions = 0.until(numPartitions).filter(_ % numTasks == id).toList
    LOG.info(s"spout $id assigned partitions $partitions")
    partitions
  }

  private val leaders: Map[Int, Broker] = partitions.map(
    p => (p, findLeader(brokers, topic, p).get)).toMap

  private val iterators: Map[Int, MessageIterator] = partitions.map(
    p => {
      val broker = leaders(p)
      val host = broker.host
      val port = broker.port
      (p, new MessageIterator(host, port, topic, p, 0L,
        soTimeout, bufferSize, fetchSize, clientId))
    }).toMap


  override def onStart(): Unit = {
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    fetchMessagesAndOutput(partitions.size, batchSize)
    self ! Message("continue", Message.noTimeStamp)
  }

  override def onStop(): Unit = {
    iterators.foreach(_._2.close())
  }

  private def fetchMessagesAndOutput(partitionNum: Int, batchSize: Int): Unit = {
    @annotation.tailrec
    def emit(partition: Int, num: Int): Unit = {
      val iter = iterators(partition)
      if (num < batchSize && iter.hasNext()) {
        val msg = iter.next()
        output(new Message(msg, System.currentTimeMillis()))
        emit((partition + 1) % partitionNum, num + 1)
      }
    }
    emit(0, 0)
  }

  private def findLeader(brokers: List[Broker], topic: String, partition: Int): Option[Broker] = brokers match {
    case Nil => throw new IllegalArgumentException("empty broker list")
    case Broker(host, port) :: Nil =>
      findLeader(host, port, topic, partition)
    case Broker(host, port) :: brokers =>
      Try(findLeader(host, port, topic, partition)) match {
        case Success(leader) => leader
        case Failure(e) => findLeader(brokers, topic, partition)
      }
  }

  private def findLeader(host: String, port: Int, topic: String, partition: Int): Option[Broker] = {
    val consumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId)
    val request = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, clientId, List(topic))
    try {
      val response = consumer.send(request)
      val metaData = response.topicsMetadata(0)

      metaData.errorCode match {
        case NoError => metaData.partitionsMetadata
          .filter(_.partitionId == partition)(0).leader
          .map(l => Broker(l.host, l.port))
        case LeaderNotAvailableCode => None
        case error => throw exceptionFor(error)
      }
    } finally {
      consumer.close()
    }
  }
}

class MessageIterator(host: String,
                      port: Int,
                      topic: String,
                      partition: Int,
                      startOffset: Long,
                      soTimeout: Int,
                      bufferSize: Int,
                      fetchSize: Int,
                      clientId: String) {


  private val consumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId)
  private val decoder = new StringDecoder()

  private var iter = iterator(startOffset)
  private var readMessages = 0L
  private var offset = startOffset
  private var nextOffset = offset

  def getOffset(): Long = {
    offset
  }

  def next(): String = {
    val mo = iter.next()
    readMessages += 1
    offset = mo.offset
    nextOffset = mo.nextOffset
    decoder.fromBytes(Utils.readBytes(mo.message.payload))
  }

  @annotation.tailrec
  final def hasNext(): Boolean = {
    if (iter.hasNext) {
      true
    } else if (0 == readMessages) {
      close()
      false
    } else {
      iter = iterator(nextOffset)
      readMessages = 0
      hasNext()
    }
  }

  def close(): Unit = {
    consumer.close()
  }

  private def iterator(offset: Long): Iterator[MessageAndOffset] = {
    val request = new FetchRequestBuilder()
      .clientId(clientId)
      .addFetch(topic, partition, offset, fetchSize)
      .build()

    val response = consumer.fetch(request)
    response.errorCode(topic, partition) match {
      case NoError => response.messageSet(topic, partition).iterator
      case error => throw exceptionFor(error)
    }
  }
}
