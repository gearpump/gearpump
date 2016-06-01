/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.kafka.lib.sink

import java.util.Properties

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.kafka.lib.sink.AbstractKafkaSink.KafkaProducerFactory
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

object AbstractKafkaSink {
  private val LOG = LogUtil.getLogger(classOf[AbstractKafkaSink])

  val producerFactory = new KafkaProducerFactory {
    override def getKafkaProducer(config: KafkaConfig): KafkaProducer[Array[Byte], Array[Byte]] = {
      new KafkaProducer[Array[Byte], Array[Byte]](config.getProducerConfig,
        new ByteArraySerializer, new ByteArraySerializer)
    }
  }

  trait KafkaProducerFactory extends java.io.Serializable {
    def getKafkaProducer(config: KafkaConfig): KafkaProducer[Array[Byte], Array[Byte]]
  }
}
/**
 * kafka sink connectors that invokes {{org.apache.kafka.clients.producer.KafkaProducer}} to send
 * messages to kafka queue
 */
abstract class AbstractKafkaSink private[kafka](
    topic: String,
    props: Properties,
    kafkaConfigFactory: KafkaConfigFactory,
    factory: KafkaProducerFactory) extends DataSink {
  import org.apache.gearpump.streaming.kafka.lib.sink.AbstractKafkaSink._

  def this(topic: String, props: Properties) = {
    this(topic, props, new KafkaConfigFactory, AbstractKafkaSink.producerFactory)
  }

  private lazy val config = kafkaConfigFactory.getKafkaConfig(props)
  // Lazily construct producer since KafkaProducer is not serializable
  private lazy val producer = factory.getKafkaProducer(config)

  override def open(context: TaskContext): Unit = {
    LOG.info("KafkaSink opened")
  }

  override def write(message: Message): Unit = {
    message.msg match {
      case (k: Array[Byte], v: Array[Byte]) =>
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v)
        producer.send(record)
        LOG.debug("KafkaSink sent record {} to Kafka", record)
      case v: Array[Byte] =>
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, v)
        producer.send(record)
        LOG.debug("KafkaSink sent record {} to Kafka", record)
      case m =>
        val errorMsg = s"unexpected message type ${m.getClass}; " +
          s"Array[Byte] or (Array[Byte], Array[Byte]) required"
        LOG.error(errorMsg)
    }
  }

  override def close(): Unit = {
    producer.close()
    LOG.info("KafkaSink closed")
  }
}

