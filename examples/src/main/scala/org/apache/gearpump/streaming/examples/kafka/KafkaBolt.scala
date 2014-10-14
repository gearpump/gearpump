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

import java.util.Properties

import kafka.producer.ProducerConfig
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.examples.kafka.KafkaBolt.KafkaBoltHandler
import org.apache.gearpump.streaming.task.{Handler, MessageHandler, TaskContext, TaskActor}
import org.apache.gearpump.util.Configs

object KafkaBolt {
  type Tuple = (String,String)
  implicit object KafkaBoltHandler extends Handler[(String,String)](
  {
    case a: Tuple =>
      a
  }
  )
}

class KafkaBolt(conf: Configs) extends TaskActor(conf) with MessageHandler[(String,String)]{

  private val kafkaConfig = new KafkaConfig()
  private val topic = kafkaConfig.getProducerTopic
  private val batchSize = kafkaConfig.getProducerEmitBatchSize
  private val kafkaProducer =
    new KafkaProducer[String, String](getProducerConfig(kafkaConfig), topic, batchSize)


  override def onStart(taskContext : TaskContext): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    KafkaBoltHandler
    doNext(msg)
  }

  def next(kvMessage:(String,String)): Unit = {
    val key = kvMessage._1
    val value = kvMessage._2
    kafkaProducer.send(key, value)
  }

  override def onStop(): Unit = {
    kafkaProducer.close()
  }

  private def getProducerConfig(config: KafkaConfig): ProducerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", config.getMetadataBrokerList)
    props.put("serializer.class", config.getSerializerClass)
    props.put("producer.type", config.getProducerType)
    props.put("request.required.acks", config.getRequestRequiredAcks)
    new ProducerConfig(props)
  }
}

