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
import org.apache.gearpump.streaming.task.{NewStartTime, TaskActor, TaskContext}
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeReplayableSource, TimeStampFilter}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig.ConfigToKafka
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaSource
import org.apache.gearpump.{Message, TimeStamp}

import scala.util.{Failure, Success}

/**
 * connect gearpump with kafka
 */
class KafkaStreamProducer(taskContext : TaskContext, conf: UserConfig) extends TaskActor(taskContext, conf) {

  private val config = conf.config
  private val batchSize = config.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = new MessageDecoder {
    override def fromBytes(bytes: Array[Byte]): Message = {
      Injection.invert[String, Array[Byte]](bytes) match {
        case Success(s) => Message(s, System.currentTimeMillis())
        case Failure(e) => throw e
      }
    }
  }

  private val filter: TimeStampFilter = new TimeStampFilter {
    override def filter(msg: Message, predicate: TimeStamp): Option[Message] = {
      Option(msg).find(_.timestamp >= predicate)
    }
  }

  private val source: TimeReplayableSource = KafkaSource(taskContext.appId, taskContext, conf, msgDecoder)
  private var startTime: TimeStamp = 0L

  override def onStart(taskContext: NewStartTime): Unit = {
    startTime = taskContext.startTime
    LOG.info(s"start time $startTime")
    source.setStartTime(startTime)
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    source.pull(batchSize).foreach(msg => filter.filter(msg, startTime).map(output))
    self ! Message("continue", System.currentTimeMillis())
  }
}
