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
import KafkaConstants._
import org.apache.gearpump.streaming.task.{Message, TaskActor}
import org.apache.gearpump.util.Configs

class KafkaBolt(conf: Configs) extends TaskActor(conf) {

  private val config = conf.config
  private val topic = config.get(PRODUCER_TOPIC).get.asInstanceOf[String]
  private val batchSize = config.get(BATCH_SIZE).get.asInstanceOf[Int]
  private val kafkaProducer =
    new KafkaProducer[String, String](getProducerConfig(config), topic, batchSize)


  override def onStart(): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    val kvMessage = msg.msg.asInstanceOf[(String, String)]
    val key = kvMessage._1
    val value = kvMessage._2
    kafkaProducer.send(key, value)
  }

  override def onStop(): Unit = {
    kafkaProducer.close()
  }

  private def getProducerConfig(config: Map[String, _]): ProducerConfig = {
    val props = new Properties()
    props.put(BROKER_LIST, config.get(BROKER_LIST).get.asInstanceOf[String])
    props.put(SERIALIZER_CLASS, config.get(SERIALIZER_CLASS).get.asInstanceOf[String])
    props.put(PRODUCER_TYPE, config.get(PRODUCER_TYPE).get.asInstanceOf[String])
    props.put(REQUIRED_ACKS, config.get(REQUIRED_ACKS).get.asInstanceOf[String])
    new ProducerConfig(props)
  }
}

