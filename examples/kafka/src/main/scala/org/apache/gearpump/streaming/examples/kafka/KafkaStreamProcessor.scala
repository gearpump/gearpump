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

import akka.actor.Cancellable
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig._
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaUtil._
import org.apache.gearpump.streaming.task.{TaskContext, TaskActor}
import org.apache.gearpump.util.Configs
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}
import org.apache.gearpump.streaming.transaction.storage.api.{KeyValueSerDe, StorageManager}

object KafkaStreamProcessor {

  class String2SerDe(encoding: String) extends KeyValueSerDe[String, String] {
    override def toBytes(kv: (String, String)): Array[Byte] = {
      val (key, value) = kv
      val keyBytes = intToByteArray(key.length) ++ key.getBytes(encoding)
      val valueBytes = intToByteArray(value.length) ++ value.getBytes(encoding)
      keyBytes ++ valueBytes
    }

    override def fromBytes(bytes: Array[Byte]): (String, String) = {
      val keyLen = byteArrayToInt(bytes.take(4))
      val key = new String(bytes.drop(4).take(keyLen), encoding)
      val valLen = byteArrayToInt(bytes.drop(4 + keyLen).take(4))
      val value = new String(bytes.drop(4 + keyLen + 4).take(valLen))
      (key, value)
    }
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaStreamProcessor])
}

class KafkaStreamProcessor(conf: Configs) extends TaskActor(conf) {

  import org.apache.gearpump.streaming.examples.kafka.KafkaStreamProcessor._

  private val config = conf.config
  private val topic = config.getProducerTopic
  private val kafkaProducer = config.getProducer[String, String]()
  private val storageManager = new StorageManager[String, String](
    s"taskId_${conf.appId}_${taskId.groupId}_${taskId.index}",
    config.getKeyValueStoreFactory.getKeyValueStore[String, String](conf),
    new String2SerDe("UTF8"),
    config.getCheckpointManagerFactory.getCheckpointManager[TimeStamp, (String, String)](conf)
  )

  private var count = 0L
  private var lastCount = 0L
  private var lastTime = System.currentTimeMillis()
  private var scheduler: Cancellable = null

  private var lastCheckpointTime = System.currentTimeMillis()
  private val checkpointIntervalMS = config.getStorageCheckpointIntervalMS

  override def onStart(taskContext : TaskContext): Unit = {
    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportThroughput())
    storageManager.start()
    storageManager.restore(taskContext.startTime)
  }

  override def onNext(msg: Message): Unit = {
    val kvMessage = msg.msg.asInstanceOf[(String, String)]
    val key = kvMessage._1
    val value = kvMessage._2
    storageManager.put(key, value)
    kafkaProducer.send(topic, key, value)
    count += 1

    val timestamp = System.currentTimeMillis()
    if (shouldCheckpoint) {
      storageManager.checkpoint(timestamp)
    }
  }

  override def onStop(): Unit = {
    kafkaProducer.close()
    scheduler.cancel()
  }

  private def reportThroughput() : Unit = {
    val current = System.currentTimeMillis()
    LOG.info(s"Task $taskId; Throughput: ${(count - lastCount, (current - lastTime) / 1000)} (messages, second)")
    lastCount = count
    lastTime = current
  }

  private def shouldCheckpoint: Boolean = {
    val current = System.currentTimeMillis()
    (current - lastCheckpointTime) > checkpointIntervalMS
  }
}

