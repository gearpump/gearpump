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

package org.apache.gearpump.streaming.kafka.sink

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.kafka.lib.{KafkaConfig, KafkaUtil}
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration.FiniteDuration

class KafkaSinkTask(context: TaskContext, conf: UserConfig) extends Task(context, conf) {

  private val kafkaConfig = conf.getValue[KafkaConfig](KafkaConfig.NAME).get
  private val topic = kafkaConfig.getProducerTopic
  private val producerConfig = KafkaUtil.buildProducerConfig(kafkaConfig)
  private val kafkaSink = new KafkaSink(producerConfig)

  private var count = 0L
  private var lastCount = 0L
  private var lastTime = System.currentTimeMillis()

  private var scheduler: Cancellable = null

  override def onStart(startTime : StartTime): Unit = {
    scheduler = context.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportThroughput())
  }

  override def onNext(msg: Message): Unit = {
    val (key, value) = msg.msg.asInstanceOf[(Array[Byte], Array[Byte])]
    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
    kafkaSink.write(producerRecord)
    count += 1
  }

  override def onStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
    kafkaSink.close()
    LOG.info("KafkaStreamProcessor stopped")
  }

  private def reportThroughput() : Unit = {
    val current = System.currentTimeMillis()
    LOG.info(s"Task ${context.taskId}; Throughput: ${(count - lastCount, (current - lastTime) / 1000)} (messages, second)")
    lastCount = count
    lastTime = current
  }
}
