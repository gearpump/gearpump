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

import akka.actor.Cancellable
import kafka.producer.ProducerConfig
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig._
import org.apache.gearpump.streaming.task.{TaskContext, TaskActor}
import org.apache.gearpump.util.Configs
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}

object KafkaBolt {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaBolt])
}

class KafkaBolt(conf: Configs) extends TaskActor(conf) {

  import org.apache.gearpump.streaming.examples.kafka.KafkaBolt._

  private val config = conf.config
  private val topic = config.getProducerTopic
  private val kafkaProducer = config.getProducer[String, String]()

  private var count = 0L
  private var lastCount = 0L
  private var lastTime = System.currentTimeMillis()
  private var scheduler: Cancellable = null

  override def onStart(taskContext : TaskContext): Unit = {
    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportThroughput)
  }

  override def onNext(msg: Message): Unit = {
    val kvMessage = msg.msg.asInstanceOf[(String, String)]
    val key = kvMessage._1
    val value = kvMessage._2
    kafkaProducer.send(topic, key, value)
    count += 1
  }

  override def onStop(): Unit = {
    kafkaProducer.close()
    scheduler.cancel()
  }

  private def reportThroughput : Unit = {
    val current = System.currentTimeMillis()
    LOG.info(s"Task $taskId; Actor ${self}; Throughput: ${((count - lastCount), ((current - lastTime) / 1000))} (messages, second)")
    lastCount = count
    lastTime = current
  }
}

