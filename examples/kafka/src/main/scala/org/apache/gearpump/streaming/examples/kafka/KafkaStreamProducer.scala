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

import akka.actor.actorRef2Scala
import com.twitter.bijection.Injection
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.StartTime
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, TimeReplayableSource, MessageDecoder}
import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.streaming.task.{TaskActor, TaskContext}
import scala.util.{Failure, Success}

class KafkaStreamProducer(taskContext : TaskContext, conf: UserConfig)
  extends TaskActor(taskContext, conf) {

  private val kafkaConfig = conf.getValue[KafkaConfig](KafkaConfig.NAME).get
  private val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder
  private val filter: TimeStampFilter = kafkaConfig.getTimeStampFilter

  private val source: TimeReplayableSource = new KafkaSource(taskContext.appId, taskContext,
    kafkaConfig, msgDecoder)
  private var startTime: TimeStamp = 0L

  override def onStart(newStartTime: StartTime): Unit = {
    startTime = newStartTime.startTime
    LOG.info(s"start time $startTime")
    source.setStartTime(startTime)
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    source.pull(batchSize).foreach{msg => filter.filter(msg, startTime).map(output)}
    self ! Message("continue", System.currentTimeMillis())
  }
}
